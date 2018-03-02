const {google} = require('googleapis');
const compressing = require('compressing');
// const unzip = require('unzip-stream');
const eos = require('end-of-stream');
const prompt = require('prompt');
const fs = require('fs');
const request = require('request');
const cheerio = require('cheerio');
const moment = require('moment');
const path = require('path');


const SCOPES = ['https://www.googleapis.com/auth/drive'];

const MAX_FILES = 1;
const MAX_ZIP_ENTRIES = 100;
const MAX_BULK_BATCH = 5000;
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
      coords: 'geo_point',
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
            let bulk = [];
            let data = '';
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
            substream.on('data', function(buff) {
              data += buff;
              // console.log(buff);
            })
            .on('end', function() {
              console.log('Read HTML contents of ' + header.name + '!');
              let $ = cheerio.load(data, {
                normalizeWhitespace: true,
                decodeEntities: true
              });
              $('div.content-cell').each((i, elem) => {
                let hit = $(elem)
                let html = hit.html();
                let text = hit.text();
                // console.log(text);
                // return;
                let found = '';
                let prods = [];
                if (found = text.match(/(.*)([A-Z][a-z][a-z]\s\d{2},\s\d{4},\s\d{1,2}:\d{2}:\d{2} (A|P)M)/i)) {
                  let d = moment(found[2], 'MMM D, YYYY, H:mm:ss A');
                  let values = {
                    timestamp: d,
                    dateString: found[2],
                    actionString: found[1]
                  };
                  if (found = html.match(/\&nbsp;(.*)/gi)) {
                    values.action = found;
                  }
                  if (caption = hit.siblings('.mdl-typography--caption')) {
                    let details = [];
                    let products = [];
                    let locations = [];
                    caption.each((j, obj) => {
                      let item = $(obj);
                      let captHTML = item.html();
                      let captText = item.text();
                      let captLinks = [];
                      if (captLinks = item.find('a')) {
                        captLinks.each((k, a) => {
                          let locLink = $(a).attr('href');
                          if (found = locLink.match(/maps\?q=([\d.]+,[\d.]+)/)) {
                            values.coords = found[1];
                          }
                        });
                      }
                      if (found = captText.match(/Details:\s(.*)/i)) {
                        if (found[1]) {
                          details.push(found[1]);
                        }
                      }
                      if (found = captText.match(/Products:\s(.*)/i)) {
                        if (found[1]) {
                          products.push(found[1]);
                        }
                      }
                      if (found = captText.match(/Locations:\s(.*)/i)) {
                        if (found[1]) {
                          locations.push(found[1]);
                        }
                      }
                      else {
                        // console.log(captText);
                      }
                    });
                    if (products.length > 0) {
                      values.products = products;
                    }
                    if (locations.length > 0) {
                      values.locations = locations;
                    }
                  }
                  if (hdr = hit.siblings('.header-cell')) {
                    hdr.each((l, header) => {
                      values.service = $(header).text();
                    });                    
                  }
                  let links = [];
                  if (links = hit.find('a')) {
                    links.each((n, a) => {
                      // only keep the last
                      values.actionUrl = $(a).attr('href');
                    });
                  }
                  console.log(values.timestamp.format() + ': ' + values.actionString + ' on ' + values.service);
                  // console.log(JSON.stringify(values, null, 1));
                  let meta = {
                    index: {
                      _index: c8._index, _type: c8._type, _id: data.timestamp
                    }
                  };
                  bulk.push(meta);
                  bulk.push(values);
                  if (bulk.length >= (MAX_BULK_BATCH * 2)) {
                    stream.pause();
                    // console.log(JSON.stringify(bulk, null, 1));
                    // return;
                    let clone = bulk.slice(0);
                    bulk = [];
                    results.push(indexBulk(clone, conf, c8).catch(reject));
                    console.log('Started ' + results.length + ' bulk batches (' + clone[1].timestamp + ')');
                  }
                }
                else {
                  // console.log('No date string found in ' + html);
                }
              });
              if (bulk.length > 0) {
                results.push(indexBulk(bulk, conf, c8).catch(reject));
                console.log('Started ' + results.length + ' bulk batches (' + bulk[1].timestamp + ', last of ' + header.name + ')');
              }
              next();
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
