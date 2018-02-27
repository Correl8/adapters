const {google} = require('googleapis');
const compressing = require('compressing');
// const unzip = require('unzip-stream');
const eos = require('end-of-stream');
const prompt = require('prompt');
const fs = require('fs');
const request = require('request');


const SCOPES = ['https://www.googleapis.com/auth/drive'];

const MAX_FILES = 1;
const MAX_ZIP_ENTRIES = 100;
const MAX_BULK_BATCH = 10000;
// const BULK_BATCH_MS = 2500;

var adapter = {};
let finishedBatches = 0;
let stream;

adapter.sensorName = 'googleactivity';

adapter.types = [
  {
    name: adapter.sensorName,
    fields: {
      timestamp: 'date',
      dateString: 'keyword',
      products: 'keyword',
      action: 'keyword',
      actionString: 'text',
      actionUrl: 'keyword',
      locations: 'keyword',
      details: 'keyword',
      service: 'keyword',
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
      spaces: drive,
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
          stream = drive.files.get({
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
          stream = request.get({url: url, headers: {'Authorization': 'Bearer ' + oauth2Client.credentials.access_token}});
          if (!stream) {
            console.error('stream failed for file ' + file.id + '!');
            continue;
          }
/*
          eos(stream, function(err) {
            console.log('End of main stream!');
            if (err) {
              return console.log('stream had an error or closed early');
            }
            // The main stream (read files from Google Drive) never ends!
            if (finishedBatches > 0) {
              var updateParams = {
                auth: oauth2Client,
                fileId: file.id,
                addParents: conf.outputDir,
                removeParents: conf.inputDir,
                fields: 'id, parents'
              };
              drive.files.update(updateParams, function(err, updated) {
                if(err) {
                  reject(new Error(err));
                  return;
                }
                else {
                  fulfill('Moved ' + file.name + ' from ' + conf.inputDir + ' to ' + conf.outputDir);
                }
              });
            }
            else {
              console.log('No location history in ' + file.name);
            }
          });
*/
          stream
          .setMaxListeners(MAX_ZIP_ENTRIES)
          .on('error', function (error) {
            console.error(new Error('Stream error: ' + error));
            return;
          })
          .pipe(new compressing.tgz.UncompressStream())
          .on('error', function (error) {
            console.error(new Error('UncompressStream error: ' + error));
            return;
          })
          .on('entry', function(header, substream, next) {
            if (header.type != 'file' || header.name == 'index.html' < 0 || header.name.indexOf('.html') < 0) {
              console.log('Skipping ' + header.type + ' ' + header.name);
              return;
            }
            console.log(header.mtime + ': ' + header.name + ' (' + Math.round(header.size/1024) + ' kB)');
            eos(substream, function(err) {
              if (err) {
                return console.log('stream had an error or closed early');
              }
              // console.log('stream has ended', this === substream);
            });
            substream.on('data', function(html) {
              console.log(html);
              return;
              // console.log(JSON.stringify(data));
              let meta = {
                index: {
                  _index: c8._index, _type: c8._type, _id: data.timestamp
                }
              };
              bulk.push(meta);
              bulk.push(data);
              if (bulk.length >= (MAX_BULK_BATCH * 2)) {
                stream.pause();
                // console.log(JSON.stringify(bulk, null, 1));
                // return;
                let clone = bulk.slice(0);
                bulk = [];
                results.push(indexBulk(clone, conf, c8).catch(reject));
                console.log('Started ' + results.length + ' bulk batches (' + clone[1].timestamp + ')');
                // setTimeout(stream.resume, BULK_BATCH_MS);
              }
            })
            .on('end', function() {
              console.log('Last batch of ' + header.name + '!');
              if (bulk.length > 0) {
                results.push(indexBulk(bulk, conf, c8).catch(reject));
              }
              // there is no next entry
              // next();
            })
            .on('error', (error) => {
              console.log('Error processing ' + header.name);
              console.log(new Error(error));
              // reject(error);
            });
          })
          .on('end', function() {
            console.log('Happy ending!');
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
    c8.bulk(bulkData).then(function(result) {
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
      stream.resume();
      // process.stdout.write('>');
      fulfill(result);
    }).catch(reject);
  });
}

module.exports = adapter;
