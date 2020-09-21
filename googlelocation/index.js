const JSONStream = require('JSONStream');
const {google} = require('googleapis');
const tar = require('tar');
const eos = require('end-of-stream');
const prompt = require('prompt');
const fs = require('fs');
const request = require('request');
const moment = require('moment');

const SCOPES = ['https://www.googleapis.com/auth/drive'];

const MAX_FILES = 5;
const MAX_ZIP_ENTRIES = 1;
const MAX_BULK_BATCH = 10000;
// const BULK_BATCH_MS = 2500;

var adapter = {};
let finishedBatches = 0;
let driveStream;

adapter.sensorName = 'googlelocation';

adapter.types = [
  {
    name: adapter.sensorName,
    fields: {
      "@timestamp": "date",
      "ecs": {
        "version": 'keyword'
      },
      "event": {
        "created": "date",
        "dataset": "keyword",
        "module": "keyword",
        "original": "keyword",
        "start": "date",
        "timezone": "keyword"
      },
      "date_details": {
        "year": 'long',
        "month": {
          "number": 'long',
          "name": 'keyword',
        },
        "week_number": 'long',
        "day_of_year": 'long',
        "day_of_month": 'long',
        "day_of_week": {
          "number": 'long',
          "name": 'keyword',
        }
      },
      "time_slice": {
        "start_hour": 'long',
        "id": 'long',
        "name": 'keyword',
      },
      "geo": {
        "location": "geo_point"
      },
      "location": {
        "latitudeE7": 'long',
        "longitudeE7": 'long',
        "accuracy": 'integer',
        "altitude": 'integer',
      },
      "activity": {
        "timestampMs": 'keyword',
        "activity": {
          "type": 'keyword',
          "confidence": 'integer',
        }
      }
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
      description: 'Google Drive folder ID where Takeout files reside'
    },
    outputDir: {
      description: 'Google Drive folder ID where indexed files are moved to'
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
          var auth = google.auth;
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
    if (!conf.credentials) {
      reject(new Error('Authentication credentials not found. Configure first!'));
      return;
    }
    var drive = google.drive('v3');
    var auth = google.auth;
    var clientSecret = conf.installed.client_secret;
    var clientId = conf.installed.client_id;
    var redirectUrl = conf.installed.redirect_uris[0];
    var oauth2Client = new auth.OAuth2(clientId, clientSecret, redirectUrl);
    oauth2Client.credentials = conf.credentials;
    // console.log(JSON.stringify(conf.credentials));
    drive.files.list({
      auth: oauth2Client,
      spaces: "drive",
      q: "trashed != true and '" + conf.inputDir + "' in parents and mimeType='application/x-gtar'",
      pageSize: MAX_FILES,
      fields: "files(id, name)"
    }, function(err, response) {
      if (err) {
        reject(new Error(err));
        return;
      }
      // console.log(response.data.files);
      var files = response.data.files;
      if (files.length <= 0) {
        fulfill('No Takeout archives found in Drive folder ' + conf.inputDir);
      }
      else {
        let results = [];
        for (let i = 0; i < files.length; i++) {
          let file = files[i];
          let fileName = file.name;
          let bulk = [];
          console.log('Processing file ' + i + ': ' + fileName);
          
/*
          driveStream = drive.files.get({
            auth: oauth2Client,
            fileId: file.id,
            alt: 'media'
          });
*/
          // temporary workaround
          let oauth = {
            consumer_key: clientId,
            consumer_secret: clientSecret,
            token: conf.credentials.access_token
          }
          let url = 'https://www.googleapis.com/drive/v3/files/' + file.id + '?alt=media';
          driveStream = request.get({url: url, headers: {'Authorization': 'Bearer ' + oauth2Client.credentials.access_token}});
          if (!driveStream) {
            console.error('stream failed for file ' + file.id + '!');
            continue;
          }
/*
          eos(driveStream, function(err) {
            console.log('End of main driveStream!');
            if (err) {
              return console.log('stream had an error or closed early');
            }
          });
*/
          driveStream
          // .setMaxListeners(MAX_ZIP_ENTRIES)
          .on('error', function (error) {
            console.error(new Error('driveStream error: ' + error));
            return;
          })
          .pipe(tar.x({filter: (path, entry) => {
            if (path.indexOf('.json') > 0) {
              // console.log('Processing ' + path);
              return true;
            }
            console.log('Skipping ' + path);
            return false;
          }}))
          .on('error', function (error) {
            console.error(new Error('tar.x stream error: ' + error));
            return;
          })
          .on('entry', function(substream) {
            // console.log(substream);
            console.log(substream.mtime + ': ' + substream.path + ' (' + Math.round(substream.size/1024) + ' kB)');
            eos(substream, function(err) {
              if (err) {
                return console.log('tar entry stream had an error or closed early');
              }
            });
            let parse = JSONStream.parse('locations.*');
            substream.pipe(parse)
            .on('data', function(data) {
              // console.log(JSON.stringify(data));
              let start = moment(Number(data.timestampMs));
              let values = {
                "@timestamp": start.format(),
                "ecs": {
                  "version": "1.0.1"
                },
                "event": {
                  "created": new Date(),
                  "dataset": "google.location",
                  "module": "Takeout",
                  "original": JSON.stringify(data),
                  "start":  start.format(),
                },
                "time_slice": time2slice(start),
                "date_details": {
                  "year": start.format('YYYY'),
                  "month": {
                    "number": start.format('M'),
                    "name": start.format('MMMM'),
                  },
                  "week_number": start.format('W'),
                  "day_of_year": start.format('DDD'),
                  "day_of_month": start.format('D'),
                  "day_of_week": {
                    "number": start.format('d'),
                    "name": start.format('dddd'),
                  }
                },
                "geo": {},
                "activity": data
              };
              values.geo.location = data.latitudeE7/10E6 + ',' + data.longitudeE7/10E6;
              // console.log(JSON.stringify(data));
              let meta = {
                index: {
                  _index: c8._index, _type: c8._type, _id: data.timestamp
                }
              };
              bulk.push(meta);
              bulk.push(data);
              if (bulk.length >= (MAX_BULK_BATCH * 2)) {
                driveStream.pause();
                // console.log(JSON.stringify(bulk, null, 1));
                // return;
                let clone = bulk.slice(0);
                bulk = [];
                results.push(indexBulk(clone, conf, c8).catch(reject));
                console.log('Started ' + results.length + ' bulk batches (' + clone[1].timestamp + ')');
                // setTimeout(driveStream.resume, BULK_BATCH_MS);
              }
            })
            .on('end', function() {
              console.log('Last batch of ' + substream.path + '!');
              if (bulk.length > 0) {
                results.push(indexBulk(bulk, conf, c8).catch(reject));
              }
              // there is no next entry
              // next();
            })
            .on('error', (error) => {
              console.log('Error processing ' + substream.path);
              console.log(new Error(error));
              // reject(error);
            });
          })
          .on('finished', function() {
            console.log('Happy ending!');
              if (finishedBatches > 0) {
                var updateParams = {
                  auth: oauth2Client,
                  fileId: file.id,
                  addParents: conf.outputDir,
                  removeParents: conf.inputDir,
                  fields: 'id, parents'
                };
 /*
               drive.files.update(updateParams, function(err, updated) {
                  if(err) {
                    reject(new Error(err));
                    return;
                  }
                  else {
                    fulfill('Moved ' + file.name + ' from ' + conf.inputDir + ' to ' + conf.outputDir);
                  }
                });
 */
                fulfill('(fake) Moved ' + file.name + ' from ' + conf.inputDir + ' to ' + conf.outputDir);
              }
              else {
                fulfill('No location history in ' + file.name);
              }
          })
          .on('error', err => {reject(new Error(err))});
        }
        console.log('Found ' + files.length + ' file ' + (files.length == 1 ? '' : 's') + ' in ' + conf.inputDir);
      }
    });
  });
};

function indexBulk(bulkData, oonf, c8) {
  return new Promise(function (fulfill, reject){
    c8.bulk(bulkData).then(function(response) {
      let result = c8.trimBulkResults(response);
      if (result.errors) {
        if (result.items) {
          let errors = [];
          for (let x=0; x<result.items.length; x++) {
            if (result.items[x].index.error) {
              errors.push(x + ': ' + result.items[x].index.error.reason);
            }
          }
          reject(new Error(fileName + ': ' + errors.length + ' errors in bulk insert:\n ' + errors.join('\n ')));
        }
        else {
          reject(new Error(JSON.stringify(result.errors))); 
        }
      }
      console.log('Finished ' + (++finishedBatches) + ' bulk batches.');
      driveStream.resume();
      // process.stdout.write('>');
      fulfill(result);
    }).catch(reject);
  });
}

function time2slice(t) {
  // creates a time_slice from a moment object
  let time_slice = {};
  let hour = t.format('H');
  let minute = (5 * Math.floor(t.format('m') / 5 )) % 60;
  time_slice.name = [hour, minute].join(':');
  if (minute == 5) {
    time_slice.name = [hour, '0' + minute].join(':');
  }
  else if (minute == 0) {
    time_slice.name += '0';
  }
  let idTime = parseInt(hour) + parseInt(minute)/60;
  time_slice.id = Math.round((idTime + (idTime >= 4 ? -4 : 20)) * 12);
  return time_slice;
}

module.exports = adapter;
