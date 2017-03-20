const fs = require('fs');
const moment = require('moment');
const parse = require('csv-parse');
const glob = require("glob");
const adapter = {};

const MAX_FILES = 10;

adapter.sensorName = 'torque-log';

const torqueIndex = 'torque-log';

adapter.types = [
  {
    name: torqueIndex,
    fields: {
      "timestamp": "date",
      "session": "string",
      "GPS Time": "date",
      "Device Time": "date",
      "Longitude": "float",
      "Latitude": "float",
      "coords": "geo_point"
    }
  }
];

adapter.promptProps = {
  properties: {
    inputDir: {
      description: 'Directory where log files reside'.magenta
    },
    outputDir: {
      description: 'Directory where indexed files are moved'.magenta,
    }
  }
};

adapter.storeConfig = function(c8, result) {
  let conf = result;
  return c8.config(conf).then(function(){
    console.log('Configuration stored.');
  }).catch(function(error) {
    console.trace(error);
  });
};

adapter.importData = function(c8, conf, opts) {
  return new Promise(function (fulfill, reject){
    glob(conf.inputDir + "/*.csv", function (er, files) {
      if (er) {
        reject(er);
        return;
      }
      let messages = [];
      let fileNames = files.slice(0, MAX_FILES);
      let results = [];
      if(fileNames.length <= 0) {
        fulfill('No logfiles found in ' + conf.inputDir);
        return;
      }
      fileNames.forEach(function(fileName) {
        results.push(indexFile(fileName, c8));
        newFile = fileName.replace(conf.inputDir, conf.outputDir);
        fs.rename(fileName, newFile, function(error) {
          if (error) {
            reject(error);
          }
          // console.log('Moved ' + fileName + ' to ' + newFile);
        });
      });
      // console.log(JSON.stringify(results));
      Promise.all(results).then(function(res) {
        let totalRows = 0;
        let totalTime = 0;
        // console.log(JSON.stringify(res));
        for (let i=0; i<res.length; i++) {
          totalRows += res[i].items.length;
          totalTime += res[i].took;
        }
        fulfill('Indexed ' + totalRows + ' log rows in ' + res.length + ' files. Took ' + totalTime + ' ms.');
      }).catch(function(error) {
        reject(error);
      });
    });
  });
};

function indexFile(fileName, c8) {
  return new Promise(function (fulfill, reject){
    let bulk = [];
    let sessionId = 1;
    fs.createReadStream(fileName).pipe(
      parse({columns: true, trim: true, auto_parse: true, skip_empty_lines: true, relax_column_count: true})
    ).on('data', function(data) {
      for (let prop in data) {
        if (prop === '' || data[prop] == '-') {
          // delete empty cells
          delete data[prop];
        }
        else if (prop && data[prop] == prop) {
          // extra headers indicate new session
          sessionId++;
          return;
        }
        else if (prop == 'GPS Time') {
          var gpsTime = moment(data['GPS Time'].replace(/ GMT/, ''), 'ddd MMM dd HH:mm:ss ZZ YYYY');
          if (gpsTime.isValid()) {
            data[prop] = gpsTime.format();
          }
          else {
            console.warn(data['GPS Time'] + ' is not valid dateTime in ' + fileName + '!');
            delete data['GPS Time'];
          }
        }
        else if (prop == 'Device Time') {
          var deviceTime = moment(data['Device Time'], 'DD-MMM-YYYY HH:mm:ss.SSS');
          if (deviceTime.isValid()) {
            data['Device Time'] = deviceTime.valueOf();
          }
          else {
            reject(new Error(data['Device Time'] + ' is not valid dateTime in ' + fileName + '!'));
            return;
          }
        }
        else {
          if (isNaN(parseFloat(data[prop]))) {
            console.warn(prop + ' ' + data[prop] + ' is not valid float!');
          }
          else {
            data[prop] = parseFloat(data[prop]);
          }
        }
      }
      data.timestamp = data['Device Time'];
      data.session = fileName.replace(/^.*\//, '') + '-' + sessionId;
      if (data['Latitude'] || data['Longitude']) {
        data['coords'] = data['Latitude'] + ',' + data['Longitude'];
      }
      bulk.push({index: {_index: c8._index, _type: c8._type, _id: data.timestamp}});
      bulk.push(data);
    }).on('error', function(error) {
      reject(new Error('Error parsing file ' + fileName + ': ' + error));
      return;
    }).on('end', function() {
      if (bulk.length > 0) {
        // console.log(bulk);
        // return;
        c8.bulk(bulk).then(function(result) {
          if (result.errors) {
            let errors = [];
            for (let x=0; x<result.items.length; x++) {
              if (result.items[x].index.error) {
                errors.push(x + ': ' + result.items[x].index.error.reason);
              }
            }
            reject(new Error(fileName + ': ' + errors.length + ' errors in bulk insert:\n ' + errors.join('\n ')));
            return;
          }
          console.log(fileName + ': ' + result.items.length + ' rows, ' + sessionId + ' session' + ((sessionId != 1) ? 's' : ''));
          fulfill(result);
        }).catch(function(error) {
          reject(error);
          return;
        });
      }
      else {
        fulfill('No data to import');
      }
    });
  });
}

module.exports = adapter;
