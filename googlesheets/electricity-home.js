var fs = require('fs');
var google = require('googleapis');
var googleAuth = require('google-auth-library');
var prompt = require('prompt');

var adapter = {};

var SCOPES = ['https://www.googleapis.com/auth/spreadsheets.readonly'];

adapter.sensorName = 'googlesheets-electricity-home';

adapter.types = [
  {
    name: 'googlesheets-electricity-home',
    fields: {
      "timestamp": "date",
      "date": "date",
      "consumptionEnergyDay": "float",
      "consumptionEnergyNight": "float",
      "consumptionEnergyTotal": "float",
      "days": "integer"
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
      default: '1T4gQTusGUNayFCzq68bgXPlsx5kJcXd2sPs3xHNP1_g'
    },
    range: {
      description: 'Range of cells to import (e.g. Sheet1!A1:Z999)'.magenta,
      default: 'A3:E999'
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
        for (var i = 0; i < rows.length; i++) {
          var row = rows[i];
          var timeArray = row[0].split('.');
            var ts = new Date(timeArray[2], parseInt(timeArray[1])-1 , timeArray[0]);
          if (!row[1] && !row[2]) {
            break;
          }
          var values = {
            timestamp: ts,
            date: ts,
            consumptionEnergyDay: Math.round(row[4]*100)/100,
            consumptionEnergyNight: Math.round(row[5]*100)/100,
            consumptionEnergyTotal: Math.round(row[6]*100)/100,
            days: row[3]
          }
  	  // console.log(row[0]);
          console.log(ts + ': ' + Math.round(row[6]*100)/100 + ' kWh');
          bulk.push({index: {_index: c8._index, _type: c8._type, _id: ts}});
          bulk.push(values);
        }
        // console.log(bulk);
        if (bulk.length > 0) {
          c8.bulk(bulk).then(function(result) {
            if (result.errors) {
              var messages = [];
              for (var i=0; i<result.items.length; i++) {
                if (result.items[i].index.error) {
                  messages.push(i + ': ' + result.items[i].index.error.reason);
                }
              }
              reject(new Error(messages.length + ' errors in bulk insert:\n ' + messages.join('\n ')));
            }
            fulfill('Indexed ' + result.items.length + ' documents in ' + result.took + ' ms.');
          }).catch(function(error) {
            reject(error);
            bulk = null;
          });
        }
        else {
          fulfill('No data available');
        }
      }
    });
  });
};

module.exports = adapter;
