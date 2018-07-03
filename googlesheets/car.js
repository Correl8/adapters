var fs = require('fs');
var google = require('googleapis');
var googleAuth = require('google-auth-library');
var prompt = require('prompt');

var adapter = {};

var SCOPES = ['https://www.googleapis.com/auth/spreadsheets.readonly'];

adapter.sensorName = 'googlesheets-car';

adapter.types = [
  {
    name: 'googlesheets-car',
    fields: {
      "timestamp": "date",
      "date": "date",
      "carModel": "string",
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
  },
  {
    name: 'googlesheets-car-cost',
    fields: {
      timestamp: "date",
      date: "date",
      carModel: "string",
      cost: "float",
      type: "keyword",
      desc: "keyword"
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

adapter.storeConfig = function(c8, result) {
  var conf = result;
  fs.readFile(result.authconfig, function (err, content) {
    if (err) {
      console.log('Error loading client secret file: ' + err);
      return;
    }
    Object.assign(conf, JSON.parse(content));
    // console.log(conf);
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
            description: 'Enter the code shown on page'.magenta,
	    default: conf.code
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
          conf.code = result.code;
          conf.credentials = token;

          c8.config(conf).then(function(){
            console.log('Access credentials saved.');
            c8.release();
            process.exit;
          });
        });
      }
    });
  });
}

adapter.importData = function(c8, conf, opts) {
  return new Promise(function (fulfill, reject){
    var clientSecret = conf.installed.client_secret;
    var clientId = conf.installed.client_id;
    var redirectUrl = conf.installed.redirect_uris[0];
    var auth = new googleAuth();
    var oauth2Client = new auth.OAuth2(clientId, clientSecret, redirectUrl);
    oauth2Client.credentials = conf.credentials;
    var sheets = google.sheets('v4');
    var getParams = {
      auth: oauth2Client,
      spreadsheetId: conf.sheetID,
      valueRenderOption: 'UNFORMATTED_VALUE',
      dateTimeRenderOption: 'FORMATTED_STRING'
    };
    if (conf.range) {
      getParams.range = conf.range;
    }
    sheets.spreadsheets.values.get(getParams, function(err, response) {
      if (err) {
        reject(err);
        return;
      }
      var rows = response.values;
      if (rows.length == 0) {
        console.log('No data found.');
      } else {
        console.log('Found ' + rows.length + ' rows:');
        var bulk = [];
        var costBulk = [];
        for (var i = 0; i < rows.length; i++) {
          var row = rows[i];
          if (!row[1]) {
            break;
          }
          var timeArray = row[1].split('-');
          var ts = new Date(timeArray[0], parseInt(timeArray[1])-1 , timeArray[2]);
          var values = {
            timestamp: ts,
            date: ts,
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
          if (values.fuelCost) {
            costBulk.push({index: {_index: c8.type(adapter.types[1].name)._index, _type: c8._type, _id: ts}});
            costBulk.push({
              timestamp: values.timestamp,
              date: values.date,
              carModel: values.carModel,
              type: 'Fuel',
              cost: values.fuelCost,
              desc: values.fuelLitres + ' @ ' + values.fuelCostPerLitre +
                    ' ('  + values.station + ')'
            });
          }
          if (values.insuranceCost) {
            costBulk.push({index: {_index: c8.type(adapter.types[1].name)._index, _type: c8._type, _id: ts}});
            costBulk.push({
              timestamp: values.timestamp,
              date: values.date,
              carModel: values.carModel,
              type: 'Insurance',
              cost: values.insuranceCost
            });
          }
          if (values.maintenanceCost) {
            costBulk.push({index: {_index: c8.type(adapter.types[1].name)._index, _type: c8._type, _id: ts}});
            costBulk.push({
              timestamp: values.timestamp,
              date: values.date,
              carModel: values.carModel,
              type: 'Maintenance',
              cost: values.maintenanceCost
            });
          }
          if (values.otherCost) {
            costBulk.push({index: {_index: c8.type(adapter.types[1].name)._index, _type: c8._type, _id: ts}});
            costBulk.push({
              timestamp: values.timestamp,
              date: values.date,
              carModel: values.carModel,
              type: 'Other',
              cost: values.otherCost,
              desc: values.otherCostDescription
            });
          }
  	  // console.log(row[0]);
          console.log(ts + ': ' + row[5] + ' litres @ ' + row[7] + ' e/l (' + row[6]+ ')');
          bulk.push({index: {_index: c8.type(adapter.types[0].name)._index, _type: c8._type, _id: ts}});
          bulk.push(values);
        }
        // console.log(bulk);
        console.log(costBulk, null, 2);
        let promises = [];
        if (bulk.length > 0) {
          let bp = c8.type(adapter.types[0].name).bulk(bulk).then(function(result) {
            if (result.errors) {
              var messages = [];
              for (var i=0; i<result.items.length; i++) {
                if (result.items[i].index.error) {
                  messages.push(i + ': ' + result.items[i].index.error.reason);
                }
              }
              console.error(new Error(messages.length + ' errors in bulk insert:\n ' + messages.join('\n ')));
            }
            console.log('Indexed ' + result.items.length + ' documents in ' + result.took + ' ms.');
          }).catch(function(error) {
            console.error(error);
            bulk = null;
          });
          promises.push(bp);
        }
        else {
          fulfill('No data available');
        }
        if (costBulk.length > 0) {
          let cbp = c8.type(adapter.types[1].name).bulk(costBulk).then(function(result) {
            if (result.errors) {
              var messages = [];
              for (var i=0; i<result.items.length; i++) {
                if (result.items[i].index.error) {
                  messages.push(i + ': ' + result.items[i].index.error.reason);
                }
              }
              console.error(new Error(messages.length + ' errors in bulk insert:\n ' + messages.join('\n ')));
            }
            console.log('Indexed ' + result.items.length + ' cost entries in ' + result.took + ' ms.');
          }).catch(function(error) {
            console.error(error);
            costBulk = null;
          });
          promises.push(cbp);
        }
        else {
          console.log('No data available');
        }
        Promise.all(promises).then(res => {
          fulfill('Indexed ' + bulk.length + ' rows, + ' + costBulk.length + ' cost entries.');
        }).catch(e => {
          reject(new Error(e));
        });
      }
    });
  });
};

module.exports = adapter;
