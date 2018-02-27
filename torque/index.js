const fs = require('fs');
const glob = require("glob");
const google = require('googleapis');
const googleAuth = require('google-auth-library');
const moment = require('moment');
const parse = require('csv-parse');
const prompt = require('prompt');
const request = require('request');

const adapter = {};

// default limit 1000 queries per user per 100 seconds
// one file uses at least 1 get and 1 update
const MAX_FILES = 10;

const SCOPES = ['https://www.googleapis.com/auth/drive'];

const csvParserOpts = {
  columns: true,
  trim: true,
  auto_parse: true,
  skip_empty_lines: true,
  skip_lines_with_empty_values: true,
  relax_column_count: true
};

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
    authconfig: {
      description: 'OAUth credentials file for Google Drive (type "none" to use local file system)',
      default: 'client_secret.json'
    },
    inputDir: {
      description: 'Local directory or Google Drive folder ID where log files reside'
    },
    outputDir: {
      description: 'Local directory or Google Drive folder ID where indexed files are moved'
    }
  }
};

adapter.storeConfig = function(c8, result) {
  let conf = result;
  c8.config(conf).then(function(){
    if (conf.authconfig && conf.authconfig != 'none') {
      fs.readFile(conf.authconfig, function (err, content) {
        if (err) {
          console.log('Error loading client secret file: ' + err);
          return;
        }
        Object.assign(conf, JSON.parse(content));
        // console.log(conf);
        c8.config(conf).then(function(){
          var auth = new googleAuth();
          var clientSecret = conf.installed.client_secret;
          var clientId = conf.installed.client_id;
          var redirectUrl = conf.installed.redirect_uris[0];
          var oauth2Client = new auth.OAuth2(clientId, clientSecret, redirectUrl);
          var authUrl = oauth2Client.generateAuthUrl({
            access_type: 'offline',
            scope: SCOPES
          });
          console.log('Authorize this app by visiting this url\n', authUrl, '\n\n');
          prompt.start();
          prompt.message = '';
          var promptProps = {
            properties: {
              code: {
                description: 'Enter the code shown on page'
              },
            }
          }
          prompt.get(promptProps, function (err, result) {
            if (err) {
              console.trace(err);
            }
            else {
              oauth2Client.getToken(result.code, function(err, token) {
                if (err) {
                  console.log('Error while trying to retrieve access token', err);
                  return;
                }
                conf.credentials = token;
                // console.log(conf);
                c8.config(conf).then(function(){
                  console.log('Access credentials saved.');
                  c8.release();
                  process.exit;
                });
              });
            }
          });
        });
      });
    }
  });
};

adapter.importData = function(c8, conf, opts) {
  return new Promise(function (fulfill, reject){
    let results = [];
    if (conf.credentials) {
      // use Google Drive
      var drive = google.drive('v3');
      var auth = new googleAuth();
      var clientSecret = conf.installed.client_secret;
      var clientId = conf.installed.client_id;
      var redirectUrl = conf.installed.redirect_uris[0];
      var oauth2Client = new auth.OAuth2(clientId, clientSecret, redirectUrl);
      oauth2Client.credentials = conf.credentials;
      drive.files.list({
        auth: oauth2Client,
        spaces: 'drive',
        q: "'" + conf.inputDir + "' in parents and trashed != true and (mimeType='text/csv' or mimeType='text/comma-separated-values')",
        pageSize: MAX_FILES,
        orderBy: 'modifiedTime desc',
        fields: "files(id, name, webContentLink)"
      }, function(err, response) {
        if (err) {
          reject(err);
          return;
        }
        var files = response.files;
        if (files.length <= 0) {
          fulfill('No logs found in Drive folder ' + conf.inputDir);
        }
        else {
          let results = [];
          for (let i = 0; i < files.length; i++) {
            let file = files[i];
            let fileName = file.name;
            drive.files.get({
              auth: oauth2Client,
              fileId: file.id,
              alt: 'media'
            },
            function(error, content) {
              if (error) {
                reject(error);
                return;
              }
              parse(content, csvParserOpts, function(err, parsed) {
                let bulk = [];
                if (err) {
                  reject(err);
                  return;
                }
                let sessionId = 1;
                // console.log(JSON.stringify(parsed));
                for (let i=0; i<parsed.length; i++) {
                  var returned = prepareRow(parsed[i], fileName, sessionId);
                  // console.log(returned);
                  data = returned[0];
                  sessionId = returned[1];
                  if (data) {
                    bulk.push({index: {_index: c8._index, _type: c8._type, _id: data.timestamp}});
                    bulk.push(data);
                  }
                }
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
                    // fulfill(result);
                    // fulfill('Indexed ' + totalRows + ' log rows in ' + res.length + ' files. Took ' + totalTime + ' ms.');
                    var updateParams = {
                      auth: oauth2Client,
                      fileId: file.id,
                      addParents: conf.outputDir,
                      removeParents: conf.inputDir,
                      fields: 'id, parents'
                    };
                    drive.files.update(updateParams, function(err, updated) {
                      if(err) {
                        reject(err);
                        return;
                      }
                      else {
                        console.log('Moved ' + file.name + ' from ' + conf.inputDir + ' to ' + conf.outputDir);
                      }
                    });
                  }).catch(function(error) {
                    reject(error);
                    return;
                  });
                }
                else {
                  fulfill('No data to import');
                }
                // console.log(data);
              });
            });
          }
        }
      });
    }
    else {
      // use local file system
      glob(conf.inputDir + "/*.csv", function (er, files) {
        if (er) {
          reject(er);
          return;
        }
        let messages = [];
        let fileNames = files.slice(0, MAX_FILES);
        if(fileNames.length <= 0) {
          fulfill('No logfiles found in ' + conf.inputDir);
          return;
        }
        fileNames.forEach(function(fileName) {
          results.push(indexCSV(fileName, fs.createReadStream, c8));
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
    }
  });
};

function indexCSV(fileName, reader, c8) {
  return new Promise(function (fulfill, reject){
    let bulk = [];
    let sessionId = 1;
    reader(fileName).pipe(
      parse(csvParserOpts)
    ).on('data', function(parsed) {
      let returned = prepareRow(parsed, fileName, sessionId);
      let data = returned[0];
      sessionId = returned[1];
      if (data) {
        bulk.push({index: {_index: c8._index, _type: c8._type, _id: data.timestamp}});
        bulk.push(data);
      }
    }).on('error', function(error) {
      // console.log(JSON.stringify(bulk));
      reject(new Error('Error parsing file ' + fileName + ': ' + error));
      return;
    }).on('end', function() {
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
            reject(new Error(fileName + ': ' + errors.length + ' errors in bulk insert:\n ' + errors.join('\n ')));
            return;
          }
          console.log(fileName + ': ' + result.items.length + ' rows, ' + sessionId + ' session' + ((sessionId != 1) ? 's' : ''));
          fulfill(result);
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
  });
}

function prepareRow(data, fileName, sessionId) {
  if (!data) {
    throw(new Error('Empty data'));
    return false;
  }
  for (let prop in data) {
   if (prop === '' || data[prop] == '-') {
     // delete empty cells
     delete data[prop];
    }
    else if (prop && data[prop] == prop) {
      // extra headers indicate new session
      sessionId++;
      return [null, sessionId];
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
        throw(new Error(data['Device Time'] + ' is not valid dateTime in ' + fileName + '!'));
        return [null, sessionId];
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
  return [data, sessionId];
}

module.exports = adapter;
