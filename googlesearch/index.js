const fs = require('fs');
const glob = require("glob");
const MAX_FILES = 100;

var adapter = {};

adapter.sensorName = 'googlesearch';

adapter.types = [
  {
    name: 'googlesearch',
    fields: {
      timestamp: 'date',
      id: 'keyword',
      query_text: 'text',
      query_keyword: 'keyword',
    }
  }
];

adapter.promptProps = {
  properties: {
    inputDir: {
      description: 'Local directory where your Takeout search history files are'
    },
    outputDir: {
      description: 'Local directory where indexed files are moved'
    }
  }
};

adapter.storeConfig = function(c8, result) {
  return c8.config(result).then(function(){
    console.log('Configuration stored.');
    c8.release();
  });
}

adapter.importData = function(c8, conf, opts) {
  return new Promise(function (fulfill, reject){
    glob(conf.inputDir + "/*.json", function (er, files) {
      // console.log(conf.inputDir + "/*.json");
      if (er) {
        reject(er);
        return;
      }
      let messages = [];
      let fileNames = files.slice(0, MAX_FILES);
      if(fileNames.length <= 0) {
        fulfill('No JSON files found in ' + conf.inputDir);
        return;
      }
      let results = [];
      fileNames.forEach(function(fileName) {
        results.push(indexFile(fileName, conf, c8));
      });
      // console.log(JSON.stringify(results));
      Promise.all(results).then(function(res) {
        let totalRows = 0;
        let totalTime = 0;
        // console.log(JSON.stringify(res));
        for (let i=0; i<res.length; i++) {
        }
      }).catch(reject);
    });
  });
};

function indexFile(fileName, conf, c8) {
  return new Promise(function (fulfill, reject){
    let bulk = [];
    let sessionId = 1;
    let json = JSON.parse(fs.readFileSync(fileName, 'utf8'));
      if (json.event && json.event.length) {
      for (var i=0; i<json.event.length; i++) {
        let query = json.event[i].query;
        query.id.forEach(id => {
          let data = query;
          data.timestamp = Math.floor(id.timestamp_usec/1000);
          data.id = id.timestamp_usec;
          data.query_keyword = data.query_text;
          // console.log(JSON.stringify(data));
          bulk.push({index: {_index: c8._index, _type: c8._type, _id: data.id}});
          bulk.push(data);
        })
      }
    }
    if (bulk.length > 0) {
      // console.log(JSON.stringify(bulk, null, 1));
      // return;
      c8.bulk(bulk).then(function(result) {
        if (result.errors) {
          let errors = [];
          for (let x=0; x<result.items.length; x++) {
            if (result.items[x].index.error) {
              errors.push(x + ': ' + result.items[x].index.error.reason);
            }
          }
          throw(new Error(fileName + ': ' + errors.length + ' errors in bulk insert:\n ' + errors.join('\n ')));
        }
        console.log(fileName + ': ' + result.items.length + ' search' + ((result.items.length > 1) ? 'es' : ''));
        fulfill(result);
      }).then(function (result) {
        newFile = fileName.replace(conf.inputDir, conf.outputDir);
        fs.rename(fileName, newFile, function(error) {
          if (error) {
            reject(error);
          }
          // console.log('Moved ' + fileName + ' to ' + newFile);
        });
      }).catch(function(error) {
        // console.log(JSON.stringify(bulk));
        reject(error);
        return;
      });
    }
    else {
      fulfill('No data to import');
    }
  });
}

module.exports = adapter;
