const {google} = require('googleapis');
const fs = require('fs');
const glob = require("glob");
const moment = require('moment');
const {parse} = require('csv-parse');
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
      "position": {
        "geo": {
          "location": "geo_point"
        },
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

adapter.importData = async (c8, conf, opts) => {
  let results = [];
  if (!conf.credentials) {
    throw new Error('Missing credentials!');
  }
  // use Google Drive
  var drive = google.drive('v3');
  var auth = google.auth;
  var clientSecret = conf.installed.client_secret;
  var clientId = conf.installed.client_id;
  var redirectUrl = conf.installed.redirect_uris[0];
  var oauth2Client = new auth.OAuth2(clientId, clientSecret, redirectUrl);
  oauth2Client.credentials = conf.credentials;
  const response = await drive.files.list({
        auth: oauth2Client,
        spaces: 'drive',
        q: "'" + conf.inputDir + "' in parents and trashed != true and (mimeType='text/csv' or mimeType='text/comma-separated-values')",
        pageSize: MAX_FILES,
        orderBy: 'modifiedTime desc',
        fields: "files(id, name, createdTime, webContentLink)"
  });
  // console.log(response.data.files);
  const files = response.data.files;
  if (files.length <= 0) {
    return 'No logs found in Drive folder ' + conf.inputDir;
  }
  const promises = [];
  for (const file of files) {
    const fileName = file.name;
    const cTime = file.createdTime;
    const opts = {
      auth: oauth2Client,
      fileId: file.id,
      alt: 'media'
    };
    const res = await drive.files.get(opts, {responseType: 'stream'});
    res.data.on('error', e => {
      throw new Error('Failed to get file from Drive! ' + e.name + ': ' + e.message);
    })
    const bulk = [];
    let sessionId = 1;
    let rowNr = 0;
    let bulkPromise = new Promise((resolve, reject) => {
      res.data.pipe(parse(csvParserOpts)).on('data', row => {
        const ts = moment(row['GPS Time'].replace(/ GMT/, ''), 'ddd MMM DD HH:mm:ss ZZ YYYY');
        const data = prepareRow(row, fileName, sessionId)
        if (data && data.odb2 && data.odb2.session) {
          sessionId = data.odb2.session.replace(/^.*-(\d+)$/, '$1');
          if (data.ecs) {
            data.event.created = moment(cTime).format();
            data.event.sequence = rowNr;
            bulk.push({index: {_index: c8._index, _id: data["@timestamp"]}});
            bulk.push(data);
            rowNr++;
          }
          else {
            rowNr = 0;
          }
        }
        else {
          // console.log(data)
        }
      }).on('end', async e => {
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
            reject(fileName + ': ' + errors.length + ' errors in bulk insert:\n ' + errors.join('\n '));
          }
          resolve(fileName + ': ' + result.items.length + ' rows, ' + sessionId + ' session' + ((sessionId != 1) ? 's' : ''));
        }
        else {
          resolve(fileName + ': no data to import');
        }
      });
    });
    bulkPromise.then((message) => {
      var updateParams = {
        auth: oauth2Client,
        fileId: file.id,
        addParents: conf.outputDir,
        removeParents: conf.inputDir,
        fields: 'id, parents'
      };
      drive.files.update(updateParams, (err, updated) => {
        if(err) {
          throw new Error(err);
        }
        else {
          console.log('Moved ' + fileName + ' from ' + conf.inputDir + ' to ' + conf.outputDir);
        }
      });
    });
    promises.push(bulkPromise);
  }
  const messages = await Promise.all(promises);
  return messages.join('\n')
  console.log('End of importdata');
};

function prepareRow(data, fileName, sessionId) {
  if (!data) {
    throw new Error('Empty data');
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
      if (! deviceTime.isValid()) {
        throw new Error(data['Device Time'] + ' is not valid dateTime in ' + fileName + '!');
        return [{odb2: {session: sessionId}}];
      }
    }
    else {
      if (isNaN(parseFloat(data[prop]))) {
        console.warn(prop + ' ' + data[prop] + ' is not valid float!');
        delete(data[prop]);
      }
      else {
        data[prop] = parseFloat(data[prop]);
      }
    }
  }
  data.session = fileName.replace(/^.*\//, '') + '-' + sessionId;
  data['Device Time'] = deviceTime.valueOf();
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
    "time_slice": time2slice(deviceTime),
    "date_details": {
      "year": deviceTime.format('YYYY'),
      "month": {
        "number": deviceTime.format('M'),
        "name": deviceTime.format('MMMM'),
      },
      "week_number": deviceTime.format('W'),
      "day_of_year": deviceTime.format('DDD'),
      "day_of_month": deviceTime.format('D'),
      "day_of_week": {
        "number": deviceTime.format('d'),
        "name": deviceTime.format('dddd'),
      }
    },
    "odb2": data
  };
  if (data['Latitude'] || data['Longitude']) {
    ecsData.geo = {location: data['Latitude'] + ',' + data['Longitude']};
  }
  return ecsData;
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
