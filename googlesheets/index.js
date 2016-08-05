var fs = require('fs');
var google = require('googleapis');
var googleAuth = require('google-auth-library');
var prompt = require('prompt');

var adapter = {};

var SCOPES = ['https://www.googleapis.com/auth/spreadsheets.readonly'];

adapter.sensorName = 'googlesheets-activity';

adapter.types = [
  {
    name: 'googlesheets-activity',
    fields: {
      "timestamp": "timestamp",
      "date": "date",
      "stepsWiiU": "integer",
      "caloriesWiiU": "integer"
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
      description: 'ID of the sheet'.magenta
    },
    range: {
      description: 'Range of cells to import (e.g. Sheet1!A1:Z999)'.magenta
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
          description: 'Enter the code shown on page'.magenta
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
  var clientSecret = conf.installed.client_secret;
  var clientId = conf.installed.client_id;
  var redirectUrl = conf.installed.redirect_uris[0];
  var auth = new googleAuth();
  var oauth2Client = new auth.OAuth2(clientId, clientSecret, redirectUrl);
  oauth2Client.credentials = conf.credentials;
  var sheets = google.sheets('v4');
  var getParams = {
    auth: oauth2Client,
    spreadsheetId: conf.sheetID
  };
  if (conf.range) {
    getParams.range = conf.range;
  }
  sheets.spreadsheets.values.get(getParams, function(err, response) {
    if (err) {
      console.log('The API returned an error: ' + err);
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
        var timeValue = row[conf.timeColumn];
        var ts = c8.guessTime(timeValue);
        if (!ts) {
          console.warn('Could not parse timestamp on row %d: %s', i, timeValue);
          continue;
        }
        if (!row[1] && !row[2]) {
          // don't import rows witout values
          continue;
        }
        var values = {
          timestamp: ts,
          date: row[0],
          stepsWiiU: row[1] ? parseInt(row[1].replace(/\s/, '')) : null,
          caloriesWiiU: row[2] ? parseInt(row[2].replace(/\s/, '')) : null
        }
        console.log(row.join(', '));
        bulk.push({index: {_index: c8._index, _type: c8._type, _id: row[0]}});
        bulk.push(values);
      }
      // console.log(bulk);
      c8.bulk(bulk).then(function(result) {
        console.log('Indexed ' + result.items.length + ' rows in ' + result.took + ' ms.');
        bulk = null;
      }).catch(function(error) {
        console.trace(error);
        bulk = null;
      });
    }
  }).catch(function(error) {
    console.trace(error);
    bulk = null;
  });
};

module.exports = adapter;
