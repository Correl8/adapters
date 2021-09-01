const fs = require('fs');
const {google} = require('googleapis');
const prompt = require('prompt');
const moment = require('moment');

const adapter = {};

const SCOPES = ['https://www.googleapis.com/auth/spreadsheets.readonly'];

adapter.sensorName = 'googlesheets-car';

adapter.types = [
  {
    name: 'googlesheets-car',
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
        "start": "date",
        "timezone": "keyword",
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
      "sheet": {
        "carModel": "keyword",
        "odometer": "integer",
        "odometerSinceLast": "integer",
        "fuelCost": "float",
        "fuelLitres": "float",
        "station": "string",
        "fuelCostPerLitre": "float",
        "fuelConsumption": "float",
        "insuranceCost": "float",
        "maintenanceCost": "float",
        "otherCost": "float",
        "otherCostDescription": "string",
      }
    }
  },
  {
    name: 'googlesheets-car-cost',
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
        "start": "date",
        "timezone": "keyword",
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
      "sheet": {
        carModel: "keyword",
        cost: "float",
        type: "keyword",
        desc: "keyword"
      }
    }
  }
];

adapter.promptProps = {
  properties: {
    authconfig: {
      description: 'Configuration file'.magenta,
      default: 'client_secret.json'
    },
    sheetID: {
      description: 'ID of the sheet'.magenta,
      default: '12CpPbR1rXnzT3XYbfgROuXcf3voevjRtm7EQijWIx0E'
    },
    range: {
      description: 'Range of cells to import (e.g. Sheet1!A1:Z999)'.magenta,
      default: 'Yhteenveto!A1:M999'
    }
  }
};

adapter.storeConfig = (c8, result) => {
  const conf = result;
  fs.readFile(result.authconfig, (err, content) => {
    if (err) {
      console.log('Error loading client secret file: ' + err);
      return;
    }
    Object.assign(conf, JSON.parse(content));
    // console.log(conf);
    const auth = google.auth;
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
    const promptProps = {
      properties: {
        code: {
            description: 'Enter the code shown on page'.magenta,
	    default: conf.code
        },
      }
    }
    prompt.get(promptProps, (err, result) => {
      if (err) {
        console.trace(err);
      }
      else {
        oauth2Client.getToken(result.code, async (err, token) => {
          if (err) {
            console.log('Error while trying to retrieve access token', err);
            return;
          }
          conf.code = result.code;
          conf.credentials = token;

          await c8.config(conf);
          console.log('Access credentials saved.');
          c8.release();
          process.exit;
        });
      }
    });
  });
}

adapter.importData = (c8, conf, opts) => {
  return new Promise(async (fulfill, reject) => {
    const clientSecret = conf.installed.client_secret;
    const clientId = conf.installed.client_id;
    const redirectUrl = conf.installed.redirect_uris[0];
    const auth = google.auth;
    const oauth2Client = new auth.OAuth2(clientId, clientSecret, redirectUrl);
    oauth2Client.credentials = conf.credentials;
    const sheets = google.sheets('v4');
    const getParams = {
      auth: oauth2Client,
      spreadsheetId: conf.sheetID,
      valueRenderOption: 'UNFORMATTED_VALUE',
      dateTimeRenderOption: 'FORMATTED_STRING'
    };
    if (conf.range) {
      getParams.range = conf.range;
    }
    try {
      const response = await sheets.spreadsheets.values.get(getParams);
      const rows = response.data.values;
      if (rows.length == 0) {
        console.log('No data found.');
      } else {
        // console.log('Found ' + rows.length + ' rows:');
        const bulk = [];
        const costBulk = [];
        for (var i = 1; i < rows.length; i++) {
          const row = rows[i];
          if (!row[1]) {
            break;
          }
          const timeArray = row[1].split('-');
          // const ts = new Date(timeArray[0], parseInt(timeArray[1])-1 , timeArray[2]);
          const ts = moment([parseInt(timeArray[0]), parseInt(timeArray[1])-1 , parseInt(timeArray[2])]);
          const values = {
            "@timestamp": ts.format(),
            "ecs": {
              "version": "1.9.0"
            },
            "event": {
              "created": ts.format(),
              "dataset": "googlesheets.car",
              "ingested": new Date(),
              "kind": "event",
              "module": "all",
              "original": JSON.stringify(row),
              "start": ts.format(),
            },
            "time_slice": time2slice(ts),
            "date_details": {
              "year": ts.format('YYYY'),
              "month": {
                "number": ts.format('M'),
                "name": ts.format('MMMM'),
              },
              "week_number": ts.format('W'),
              "day_of_year": ts.format('DDD'),
              "day_of_month": ts.format('D'),
              "day_of_week": {
                "number": ts.format('d'),
                "name": ts.format('dddd'),
              }
            },
            "sheet": {
              carModel: row[0],
              odometer: parseInt(row[2]),
              odometerSinceLast: parseInt(row[3]),
              fuelCost: parseFloat(row[4]),
              fuelLitres: parseFloat(row[5]),
              station: row[6],
              fuelCostPerLitre: parseFloat(row[7]),
              fuelConsumption: parseFloat(row[8]),
              insuranceCost: parseFloat(row[9]),
              maintenanceCost: parseFloat(row[10]),
              otherCost: parseFloat(row[11]),
              otherCostDescription: row[12]
            }
          }
          if (values.sheet.fuelCost) {
            costBulk.push({index: {_index: c8.type(adapter.types[1].name)._index, _id: ts + "-fuel"}});
            costBulk.push({
              "@timestamp": values["@timestamp"],
              "event": values.event,
              "time_slice": values.time_slice,
              "date_details": values.date_details,
              "sheet": {
                carModel: values.sheet.carModel,
                type: 'Fuel',
                cost: values.sheet.fuelCost,
                desc: values.sheet.fuelLitres + ' @ ' + values.sheet.fuelCostPerLitre +
                  ' ('  + values.sheet.station + ')'
              }
            });
            costBulk[costBulk.length-1].event.module = "fuel";
          }
          if (values.sheet.insuranceCost) {
            costBulk.push({index: {_index: c8.type(adapter.types[1].name)._index, _id: ts + "-insurance"}});
            costBulk.push({
              "@timestamp": values["@timestamp"],
              "event": values.event,
              "time_slice": values.time_slice,
              "date_details": values.date_details,
              "sheet": {
                carModel: values.sheet.carModel,
                type: 'Insurance',
                cost: values.sheet.insuranceCost
              }
            });
            costBulk[costBulk.length-1].event.module = "insurance";
          }
          if (values.sheet.maintenanceCost) {
            costBulk.push({index: {_index: c8.type(adapter.types[1].name)._index, _id: ts + "-maintenance"}});
            costBulk.push({
              "@timestamp": values["@timestamp"],
              "event": values.event,
              "time_slice": values.time_slice,
              "date_details": values.date_details,
              "sheet": {
                carModel: values.sheet.carModel,
                type: 'Maintenance',
                cost: values.sheet.maintenanceCost
              }
            });
            costBulk[costBulk.length-1].event.module = "maintenance";
          }
          if (values.sheet.otherCost) {
            costBulk.push({index: {_index: c8.type(adapter.types[1].name)._index, _id: ts + "-other"}});
            costBulk.push({
              "@timestamp": values["@timestamp"],
              "event": values.event,
              "time_slice": values.time_slice,
              "date_details": values.date_details,
              "sheet": {
                carModel: values.sheet.carModel,
                type: 'Other',
                cost: values.sheet.otherCost
              }
            });
            costBulk[costBulk.length-1].event.module = "other";
          }
  	  // console.log(row[0]);
          console.log(ts.format('YYYY-MM-DD') + ': ' + row[5] + ' litres @ ' + Math.round(row[7]*100)/100 + ' €/l (' + row[6]+ ')');
          bulk.push({index: {_index: c8.type(adapter.types[0].name)._index, _id: ts}});
          bulk.push(values);
        }
        // console.log(bulk);
        // console.log(costBulk, null, 2);
        if (bulk.length > 0) {
          const response = await c8.type(adapter.types[0].name).bulk(bulk);
          let result = c8.trimBulkResults(response);
          if (result.errors) {
            const messages = [];
            for (var i=0; i<result.items.length; i++) {
              if (result.items[i].index.error) {
                messages.push(i + ': ' + result.items[i].index.error.reason);
              }
            }
            console.error(new Error(messages.length + ' errors in bulk insert:\n ' + messages.join('\n ')));
          }
          console.log('Indexed ' + result.items.length + ' documents in ' + result.took + ' ms.');
        }
        else {
          fulfill('No data available');
        }
        if (costBulk.length > 0) {
          const response = await c8.type(adapter.types[1].name).bulk(costBulk);
          let result = c8.trimBulkResults(response);
          if (result.errors) {
            const messages = [];
            for (var i=0; i<result.items.length; i++) {
              if (result.items[i].index.error) {
                messages.push(i + ': ' + result.items[i].index.error.reason);
              }
            }
            console.error(new Error(messages.length + ' errors in bulk insert:\n ' + messages.join('\n ')));
          }
          console.log('Indexed ' + result.items.length + ' cost entries in ' + result.took + ' ms.');
        }
        else {
          console.log('No cost data available');
        }
        fulfill('Indexed ' + bulk.length + ' rows, + ' + costBulk.length + ' cost entries.');
      }
    }
    catch(e) {
      reject(new Error(e));
    }
  });
};

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
