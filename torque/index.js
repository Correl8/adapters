const {google} = require('googleapis');
const fs = require('fs');
const glob = require("glob");
const moment = require('moment');
const parse = require('csv-parse');
const prompt = require('prompt');
const request = require('request');

const adapter = {};

// default limit 1000 queries per user per 100 seconds
// one file uses at least 1 get and 1 update
const MAX_FILES = 1;

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
      "@timestamp": "date",
      "ecs": {
        "version": 'keyword'
      },
      "event": {
        "created": "date",
        "dataset": "keyword",
        "ingested": "date",
        "kind": "keyword",
        "module": "keyword",
        "original": "keyword",
        "provider": "keyword",
        "start": "date",
        "sequence": "long",
      },
      "geo": {
        "location": "geo_point"
      },
      "odb2": {
        "timestamp": "date",
        "session": "string",
        "GPS Time": "date",
        "Device Time": "date",
        "Longitude": "float",
        "Latitude": "float",
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
      description: 'Local directory or Google Drive folder ID where log files reside'
    },
    outputDir: {
      description: 'Local directory or Google Drive folder ID where indexed files are moved'
    }
  }
};

adapter.storeConfig = async (c8, result) => {
  let conf = result;
  await c8.config(conf);
  if (conf.authconfig && conf.authconfig != 'none') {
    fs.readFile(conf.authconfig, async (err, content) => {
      if (err) {
        console.log('Error loading client secret file: ' + err);
        return;
      }
      Object.assign(conf, JSON.parse(content));
      // console.log(conf);
      await c8.config(conf);
      var auth = google.auth;
      const clientSecret = conf.installed.client_secret;
      const clientId = conf.installed.client_id;
      const redirectUrl = conf.installed.redirect_uris[0];
      const oauth2Client = new auth.OAuth2(clientId, clientSecret, redirectUrl);
      const authUrl = oauth2Client.generateAuthUrl({
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
  }
};

adapter.importData = (c8, conf, opts) => {
  return new Promise(async (fulfill, reject) => {
    let results = [];
    if (conf.credentials) {
      // use Google Drive
      var drive = google.drive('v3');
      var auth = google.auth;
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
        fields: "files(id, name, createdTime, webContentLink)"
      }, (err, response) => {
        // console.log(response.data.files);
        if (err) {
          reject(err);
          return;
        }
        var files = response.data.files;
        if (files.length <= 0) {
          fulfill('No logs found in Drive folder ' + conf.inputDir);
        }
        else {
          let results = [];
          for (let i = 0; i < files.length; i++) {
            const file = files[i];
            const fileName = file.name;
            const cTime = file.createdTime;
            const opts = {
              auth: oauth2Client,
              fileId: file.id,
              alt: 'media'
            };
            drive.files.get(opts, (error, content) => {
              if (error) {
                reject(error);
                return;
              }
              // console.log(content.data);
              // process.exit();
              parse(content.data, csvParserOpts, async (err, parsed) => {
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
                  data = returned;
                  if (data && data.odb2 && data.odb2.session) {
                    data.event.created = moment(cTime).format();
                    data.event.sequence = i;
                    sessionId = data.odb2.session.replace(/^.*-(\d+)$/, '$1');
                    if (data.ecs) {
                      bulk.push({index: {_index: c8._index, _id: data["@timestamp"]}});
                      bulk.push(data);
                    }
                  }
                }
                if (bulk.length > 0) {
                  // console.log(bulk);
                  // return;
                  const response = await c8.bulk(bulk);
                  const result = c8.trimBulkResults(response);
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
                  drive.files.update(updateParams, (err, updated) => {
                    if(err) {
                      reject(err);
                      return;
                    }
                    else {
                      fulfill('Moved ' + file.name + ' from ' + conf.inputDir + ' to ' + conf.outputDir);
                    }
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
  });
};

function prepareRow(data, fileName, sessionId) {
  if (!data) {
    throw(new Error('Empty data'));
    return false;
  }
  const original = Object.assign({}, data);
  for (let prop in data) {
    if (prop === '' || data[prop] == '-') {
      // delete empty cells
      delete data[prop];
    }
    else if (prop && data[prop] == prop) {
      // extra headers indicate new session
      sessionId++;
      return [{odb2: {session: sessionId}}];
    }
    else if (prop == 'GPS Time') {
      var gpsTime = moment(data['GPS Time'].replace(/ GMT/, ''), 'ddd MMM DD HH:mm:ss ZZ YYYY');
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
        return [{odb2: {session: sessionId}}];
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
  data.session = fileName.replace(/^.*\//, '') + '-' + sessionId;
  let ecsData = {
    "@timestamp": data['Device Time'],
    "ecs": {
      "version": '1.6.0'
    },
    "event": {
      "dataset": "torque",
      "ingested": new Date(),
      "kind": "metric",
      "module": "drivesync",
      "original": JSON.stringify(original),
      "provider": "ODB2",
      "start": data['Device Time']
    },
    "odb2": data
  };
  if (data['Latitude'] || data['Longitude']) {
    ecsData.geo = {location: data['Latitude'] + ',' + data['Longitude']};
  }
  return ecsData;
}

module.exports = adapter;
