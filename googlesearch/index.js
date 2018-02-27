const fs = require('fs');
const JSONStream = require('JSONStream');
const google = require('googleapis');
const drive = google.drive('v3');
const googleAuth = require('google-auth-library');
const unzip = require('unzipper');
const prompt = require('prompt');

const SCOPES = ['https://www.googleapis.com/auth/drive'];

const MAX_FILES = 100;
const MAX_BULK_BATCH = 10000;

var adapter = {};
let finishedBatches = 0;

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

adapter.importData = function(c8, conf, opts) {
  return new Promise((fulfill, reject) => {
    let results = [];
    if (!conf.credentials) {
      reject(new Error('Authentication credentials not found. Configure first!'));
      return;
    }
    var auth = new googleAuth();
    var clientSecret = conf.installed.client_secret;
    var clientId = conf.installed.client_id;
    var redirectUrl = conf.installed.redirect_uris[0];
    var oauth2Client = new auth.OAuth2(clientId, clientSecret, redirectUrl);
    oauth2Client.credentials = conf.credentials;
    drive.files.list({
      auth: oauth2Client,
      spaces: drive,
      q: "'" + conf.inputDir + "' in parents and mimeType='application/x-zip'",
      pageSize: MAX_FILES,
      fields: "files(id, name)"
    }, (err, response) => {
      if (err) {
        reject(err);
        return;
      }
      let files = response.files;
      let fileResults = [];
      if (files.length <= 0) {
        fulfill('No Takeout archives found in Drive folder ' + conf.inputDir);
      }
      else {
        for (let i = 0; i < files.length; i++) {
          let file = files[i];
          fileResults.push(processTakeoutFile(oauth2Client, file, c8, conf));
        }
        console.log('Found ' + files.length + ' files in ' + conf.inputDir);
      }
      Promise.all(fileResults).then(results => {
        let resultMessage = 'Indexed ' + results.length + ' archives';
        totalFiles = 0;
        totalEntries = 0;
        totalQueries = 0;
        totalIndexed = 0;
        totalTook = 0;
        results.forEach(res => {
          totalEntries += res.zipEntries;
          totalQueries += res.fileQueries;
          totalIndexed += res.indexed;
          totalTook += res.took;
          if (res.indexed > 0) {
            var updateParams = {
              auth: oauth2Client,
              fileId: res.file.id,
              addParents: conf.outputDir,
              removeParents: conf.inputDir,
              fields: 'id, parents'
            };
            drive.files.update(updateParams, (err, updated) => {
              if(err) {
                console.error('Error moving file: ' + JSON.stringify(err));
                return;
              }
              else {
                console.log('Moved ' + res.file.name + ' from ' + conf.inputDir + ' to ' + conf.outputDir);
              }
            });
          }
          else {
            console.log('No search history in ' + res.file.name);
          }
        });
        fulfill(resultMessage + ', ' + totalEntries + ' files, ' + totalQueries + ' search queries in ' + totalTook + ' ms. (' + totalIndexed + ' entries due to repeated queries.)');
      });
    });
  });
};

function processTakeoutFile(oauth2Client, file, c8, conf) {
  return new Promise((fulfill, reject) => {
    let fileName = file.name;
    console.log('Processing ' + fileName);
    let bulk = [];
    let zipEntries = 0;
    let fileQueries = 0;
    drive.files.get({
      auth: oauth2Client,
      fileId: file.id,
      alt: 'media'
    })
    .setMaxListeners(MAX_FILES)
    .on('end', () => {
      if (bulk.length > 0) {
        c8.bulk(bulk).then(result => {
          finishedBatches++;
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
          fulfill({file: file, zipEntries: zipEntries, fileQueries: fileQueries, indexed: result.items.length, took: result.took});
        }).catch(reject);
      }
      else {
        fulfill({file: file, zipEntries: zipEntries, fileQueries: fileQueries, indexed: 0, took: 0});
      }
    })
    .pipe(unzip.Parse())
    .on('entry', entry =>  {
      var zipEntry = entry.path;
      if (zipEntry.indexOf('index.html') >= 0) {
        // console.log('Skipping ' + zipEntry);
        return;
      }
      // console.log('Processing ' + zipEntry);
      entry.buffer().then(json => {
        let queries = JSON.parse(json);
        // console.log('All queries are ' + JSON.stringify(queries, null, 1));
        queries.event.forEach(event => {
          let query = event.query;
          // console.log('query is ' + JSON.stringify(query));
          if (!query.id.forEach) {
            console.log('Failed query is ' + JSON.stringify(query));
            process.exit();
          }
          query.id.forEach(id => {
            let data = query;
            data.timestamp = Math.floor(id.timestamp_usec/1000);
            data.id = id.timestamp_usec;
            data.query_keyword = data.query_text;
            // console.log(JSON.stringify(data));
            let meta = {
              index: {
                _index: c8._index, _type: c8._type, _id: data.id
              }
            };
            bulk.push(meta);
            bulk.push(data);
          });
        });
        console.log(fileName + '/' + zipEntry + ': ' + queries.event.length + ' queries');
        zipEntries++;
        fileQueries += queries.event.length;
      })
      .catch(reject);
    })
    .on('end', () => {
      console.log('\n\nEnd never reached!\n\n');
    })
    .on('error', reject)
    .promise().then(res => {
      console.log('\n\nNever promised!\n\n');
    });
  });
}

module.exports = adapter;
