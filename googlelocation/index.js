const JSONStream = require('JSONStream');
const google = require('googleapis');
const googleAuth = require('google-auth-library');
const unzip = require('unzip-stream');
const prompt = require('prompt');

const SCOPES = ['https://www.googleapis.com/auth/drive'];

const MAX_FILES = 10;
const MAX_BULK_BATCH = 10000;
// const BULK_BATCH_MS = 2500;

var adapter = {};
let finishedBatches = 0;
let stream;

adapter.sensorName = 'googlelocation';

adapter.types = [
  {
    name: adapter.sensorName,
    fields: {
      timestamp: 'date',
      timestampMs: 'keyword',
      latitudeE7: 'long',
      longitudeE7: 'long',
      location: 'geo_point',
      accuracy: 'integer',
      altitude: 'integer',
      activity: {
        timestampMs: 'keyword',
        activity: {
          type: 'keyword',
          confidence: 'integer',
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
    if (!conf.credentials) {
      reject(new Error('Authentication credentials not found. Configure first!'));
      return;
    }
    var drive = google.drive('v3');
    var auth = new googleAuth();
    var clientSecret = conf.installed.client_secret;
    var clientId = conf.installed.client_id;
    var redirectUrl = conf.installed.redirect_uris[0];
    var oauth2Client = new auth.OAuth2(clientId, clientSecret, redirectUrl);
    let parse = JSONStream.parse('locations.*');
    oauth2Client.credentials = conf.credentials;
    drive.files.list({
      auth: oauth2Client,
      spaces: drive,
      q: "'" + conf.inputDir + "' in parents and mimeType='application/x-zip'",
      pageSize: MAX_FILES,
      fields: "files(id, name, webContentLink)"
    }, function(err, response) {
      if (err) {
        reject(err);
        return;
      }
      var files = response.files;
      if (files.length <= 0) {
        fulfill('No Takeout archives found in Drive folder ' + conf.inputDir);
      }
      else {
        let results = [];
        for (let i = 0; i < files.length; i++) {
          let file = files[i];
          let fileName = file.name;
          let bulk = [];
          console.log('Processing ' + fileName);
          drive.files.get({
            auth: oauth2Client,
            fileId: file.id,
            alt: 'media'
          })
          .setMaxListeners(MAX_FILES)
          .pipe(unzip.Parse())
          .on('entry', function (entry) {
            var zipEntry = entry.path;
            if (zipEntry.indexOf('index.html') >= 0) {
              // console.log('Skipping ' + zipEntry);
              return;
            }
            console.log('Processing ' + zipEntry);
            stream = entry;
            stream.pipe(parse)
            .setMaxListeners(0)
            .on('data', function(data) {
              // console.log(JSON.stringify(data));
              data.timestamp = new Date(Number(data.timestampMs));
              data.location = data.latitudeE7/10E6 + ',' + data.longitudeE7/10E6;
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
              console.log('Last batch of ' + zipEntry + '!');
              if (bulk.length > 0) {
                results.push(indexBulk(bulk, conf, c8).catch(reject));
              }
              // fulfill(results);
            })
            .on('error', reject);
          })
          .on('end', function() {
            if (finishedBatches > 0) {
              // wait a while before moving the file!
              setTimeout(function() {
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
              }, 2000);
            }
            else {
              console.log('No location history in ' + file.name);
            }
          })
          .on('error', reject);
        }
        console.log('Found ' + files.length + ' files in ' + conf.inputDir);
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
