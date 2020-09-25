const {google} = require('googleapis');
const {chain}  = require('stream-chain');
const {parser} = require('stream-json');
const StreamValues = require('stream-json/streamers/StreamValues.js');
const tar = require('tar');
const eos = require('end-of-stream');
const prompt = require('prompt');
const fs = require('fs');
const http = require('http');
const request = require('request');
const moment = require('moment');
const path = require('path');

const SCOPES = ['https://www.googleapis.com/auth/drive'];

const MAX_FILES = 5;
const MAX_ZIP_ENTRIES = 500000;
const MAX_BULK_BATCH = 1000;

var adapter = {};
let finishedBatches = 0;
let totalActions = 0;
let fileActions = 0;
let driveStream;

adapter.sensorName = 'googleactivity';

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
        "duration": "long",
        "end": "date",
        "ingested": "date",
        "kind": "keyword",
        "module": "keyword",
        "original": "keyword",
        "start": "date",
        "timezone": "keyword"
      },
      "message": 'text',
      "activity": {
        "dateString": 'keyword',
        "time": 'keyword',
        "header": 'keyword',
        "title": 'keyword',
        "products": 'keyword',
        "actionType": 'keyword',
        "target": 'keyword',
        "actionString": 'text',
        "actionUrl": 'keyword',
        "locations": 'keyword',
        "coords": 'geo_point',
        "details": {
          "name": 'keyword',
        },
        "service": 'keyword'
      }
    }
  }
];

adapter.promptProps = {
  properties: {
    authconfig: {
      description: 'OAUth credentials file for Google Drive',
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

adapter.storeConfig = (c8, result) => {
  let conf = result;
  c8.config(conf).then(() => {
    if (conf.authconfig && conf.authconfig != 'none') {
      fs.readFile(conf.authconfig, (err, content) => {
        if (err) {
          console.log('Error loading client secret file: ' + err);
          return;
        }
        Object.assign(conf, JSON.parse(content));
        // console.log(conf);
        c8.config(conf).then(() => {
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
          prompt.get(promptProps, (err, result) => {
            if (err) {
              console.trace(err);
            }
            else {
              oauth2Client.getToken(result.code, (err, token) => {
                if (err) {
                  console.log('Error while trying to retrieve access token', err);
                  return;
                }
                conf.credentials = token;
                // console.log(conf);
                c8.config(conf).then(() => {
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

adapter.importData = (c8, conf, opts) => {
  return new Promise((fulfill, reject) => {
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
    drive.files.list({
      auth: oauth2Client,
      spaces: "drive",
      q: "trashed != true and '" + conf.inputDir + "' in parents and mimeType='application/x-gtar'",
      pageSize: MAX_FILES,
      fields: "files(id, name)"
    }, (err, response) => {
      if (err) {
        reject(new Error(err));
        return;
      }
      var files = response.data.files;
      if (files.length <= 0) {
        fulfill('No Takeout archives found in Drive folder ' + conf.inputDir);
      }
      else {
        let results = [];
        for (let i = 0; i < files.length; i++) {
          let file = files[i];
          let fileName = file.name;
          console.log('Processing file ' + i + ': ' + fileName);
/*
          let driveStream = drive.files.get({
            auth: oauth2Client,
            fileId: file.id,
            alt: 'media'
          }).then((res) => {
            console.log(res);
          });
*/
          // temporary workaround
          let url = 'https://www.googleapis.com/drive/v3/files/' + file.id + '?alt=media&mimeType=application/x-gtar';
          driveStream = request.get({url: url, headers: {'Authorization': 'Bearer ' + oauth2Client.credentials.access_token}});
/*
          const pipeline = chain([
            driveStream,
            tar.x({filter: (path, entry) => {
              if (path.indexOf('.json') > 0) {
                // console.log('Processing ' + path);
                return true;
              }
              console.log('Skipping ' + path);
              return false;
            }})
          ]);
          pipeline.on('entry', (data) => {
            console.log(JSON.stgingify(data, null, 1));
          });
	  pipeline.on('end', () => {
            console.log('End of pipeline!');
          });
*/
          if (!driveStream) {
            console.error('stream failed for file ' + file.id + '!');
            continue;
          }
          eos(driveStream, (err) => {
            if (err) {
              return console.log('Drive stream had an error or closed early');
            }
            // console.log('Drive stream has ended', this === driveStream);
          });
          driveStream.on('error', (err) => {
            console.log(err);
          });
          driveStream.on('response', (response) => {
            // console.log(response);
          });
          driveStream.pipe(
            tar.x({filter: (path, entry) => {
              if (path.indexOf('.json') > 0) {
                // console.log('Processing ' + path);
                return true;
              }
              // console.log('Skipping ' + path);
              return false;
            }})
          )
          .on('error', (error) => {
            console.error(new Error('Stream error: ' + error));
            return;
          })
          .on('entry', (substream) => {
            fileActions = 0;
            let jsonstream = StreamValues.withParser();
            let bulk = [];
            eos(substream, (err) => {
              if (err) {
                return console.log('stream had an error or closed early');
              }
              // console.log('substream has ended', this === substream);
            });
            substream
            .on('error', (error) => {
              console.log('Error processing tar entry ' + substream.path);
              console.log(new Error(error));
              // reject(error);
            })
            .pipe(jsonstream)
            .on('error', (error) => {
              console.log('Error processing JSON stream in ' + substream.path);
              console.log(new Error(error));
              // reject(error);
            })
            .on('data', async (data) => {
              if (data.value && data.value.length) {
                bulk = [];
                for (var i=0; i<data.value.length; i++) {
                  let item = data.value[i];
                  if (item.time) {
                    let values = {
                      "@timestamp": item.time,
                      "ecs": {
                        "version": "1.6.0"
                      },
                      "event": {
                        "created": substream.mtime,
                        "dataset": "google.activity",
                        "ingested": new Date(),
                        "kind": "event",
                        "module": "Takeout",
                        "original": JSON.stringify(item),
                        "provider": item.header,
                        "start":  item.time,
                      },
                      "message": item.title,
                      "activity": item
                    };
                    let meta = {
                      index: {
                        _index: c8._index, _id: item.time + '-' + item.header
                      }
                    };
                    bulk.push(meta);
                    bulk.push(values);
                    fileActions++;
                    totalActions++;
                    if (bulk.length >= (MAX_BULK_BATCH * 2)) {
                      jsonstream.pause();
                      let clone = bulk.slice(0);
                      bulk = [];
                      results.push(await indexBulk(clone, conf, c8).catch((error) => {reject(new Error(error));}));
                      // console.log('Started ' + results.length + ' bulk batches (' + clone[1]['@timestamp'] + ')');
                      jsonstream.resume();
                    }
                  }
                }
                console.log(substream.path + ': ' + fileActions + ' activities.');
              }
            })
            .on('end', async () => {
              if (bulk.length > 0) {
                // console.log(bulk);
                results.push(await indexBulk(bulk, conf, c8).catch((error) => {reject(new Error(error));}));
                // console.log('Handled ' + substream.path + '. Started ' + results.length + ' bulk batches (' + bulk[1]['@timestamp'] + ', last of ' + substream.path + ')');
              }
            })
          })
          .on('finish', () => {
            console.log('finish');
          })
          .on('end', () => {
            console.log('end');
            if (totalActions > 0) {
              var updateParams = {
                auth: oauth2Client,
                fileId: file.id,
                addParents: conf.outputDir,
                removeParents: conf.inputDir,
                fields: 'id, parents'
              };
              drive.files.update(updateParams, (err, updated) => {
                if(err) {
                  reject(new Error(err));
                  return;
                }
                else {
                  fulfill('Indexed ' + totalActions + ' activities. Moved ' + file.name + ' from ' + conf.inputDir + ' to ' + conf.outputDir);
                }
              });
            }
            else {
              fulfill('No activity history in ' + file.name);
            }
          });
        }
        console.log('Found ' + files.length + ' file' + (files.length == 1 ? '' : 's') + ' in ' + conf.inputDir);
      }
    });
  });
};

function indexBulk(bulkData, oonf, c8) {
  return new Promise(async (fulfill, reject) => {
    try {
      let response = await c8.bulk(bulkData);
      let result = c8.trimBulkResults(response);
      if (result.errors) {
        if (result.items) {
          let errors = [];
          for (let x=0; x<result.items.length; x++) {
            if (result.items[x].index.error) {
              errors.push(x + ': ' + result.items[x].index.error.reason);
            }
          }
          reject(new Error(errors.length + ' errors in bulk insert:\n ' + errors.join('\n ')));
        }
        else {
          reject(new Error(JSON.stringify(result.errors))); 
        }
      }
      // process.stdout.write('>');
      // console.log('Finished ' + (++finishedBatches) + ' bulk batches. (' + result.items.length + ' items)');
      fulfill(result);
    }
    catch (e) {
      reject(e);
    }
  });
}

module.exports = adapter;
