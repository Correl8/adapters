var fs = require('fs');
var google = require('googleapis');
var googleAuth = require('google-auth-library');
var prompt = require('prompt');
var activityTypes = require('google-fit-activity-types');

var adapter = {};

var MS_IN_DAY = 24 * 60 * 60 * 1000;
var MAX_DAYS = 365;
var SCOPES = [
  'https://www.googleapis.com/auth/fitness.activity.read',
];

adapter.sensorName = 'googlefit';

adapter.types = [
  {
    name: 'googlefit-session',
    fields: {
      id: 'keyword',
      timestamp: 'date',
      startTimeMillis: 'date',
      endTimeMillis: 'date',
      modifiedTimeMillis: 'date',
      duration: 'integer',
      activity: 'keyword',
      activityType: 'integer',
      name: 'keyword',
      description: 'keyword',
      application: {
        packageName: 'keyword',
        version: 'keyword',
      }
    }
  }
];

adapter.promptProps = {
  properties: {
    authconfig: {
      description: 'Configuration file'.magenta,
      default: 'client_secret.json'
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
  return new Promise(function (fulfill, reject){
    var clientSecret = conf.installed.client_secret;
    var clientId = conf.installed.client_id;
    var redirectUrl = conf.installed.redirect_uris[0];
    var auth = new googleAuth();
    var oauth2Client = new auth.OAuth2(clientId, clientSecret, redirectUrl);
    oauth2Client.credentials = conf.credentials;
    var firstDate = new Date();
    firstDate.setTime(firstDate.getTime() - MS_IN_DAY);
    var lastDate = opts.lastDate || new Date();
    c8.type(adapter.types[0].name).search({
      _source: ['timestamp'],
      size: 1,
      sort: [{'timestamp': 'desc'}],
    }).then(function(response) {
      if (opts.firstDate) {
        firstDate = opts.firstDate;
        console.log('Setting first time to ' + firstDate);
      }
      else if (response && response.hits && response.hits.hits && response.hits.hits[0] && response.hits.hits[0]._source && response.hits.hits[0]._source.timestamp) {
        var d = new Date(response.hits.hits[0]._source.timestamp);
        // firstDate = new Date(d.getTime() + 1);
        firstDate = d;
        console.log('Setting first time to ' + firstDate);
      }
      else {
        firstDate = new Date();
        firstDate.setTime(firstDate.getTime() - MS_IN_DAY);
        console.warn('No previously indexed data, setting first time to ' + firstDate);
      }
      if (lastDate.getTime() >= (firstDate.getTime() + (MS_IN_DAY * MAX_DAYS))) {
        lastDate.setTime(firstDate.getTime() + (MS_IN_DAY * MAX_DAYS) - 1);
        console.warn('Max time range ' + MAX_DAYS + ' days, setting end time to ' + lastDate);
      }
      var dsNames = {};
      var dTypes = {};
      var devices = {};
      var fit = google.fitness('v1');
      var listOpts = {
        startTime: firstDate.toISOString(),
        endTime: lastDate.toISOString(),
        auth: oauth2Client,
        userId: 'me'
      };
      fit.users.sessions.list(listOpts, function(err, resp) {
        if (err) {
          reject(new Error('The fitness API returned an error when reading sessions: ' + err));
          return;
        }
        // console.log(resp);
        var bulk = [];
        for (var i=0; i<resp.session.length; i++) {
          var values = resp.session[i];
          values.startTimeMillis = parseInt(values.startTimeMillis);
          values.endTimeMillis = parseInt(values.endTimeMillis);
          values.modifiedTimeMillis = parseInt(values.modifiedTimeMillis);
          values.timestamp = new Date(values.endTimeMillis);
          values.duration = Math.round((values.endTimeMillis - values.startTimeMillis)/1000);
          values.activity = activityTypes[values.activityType];
          if (!values.name) {
            values.name = values.activity;
          }
          bulk.push({index: {_index: c8._index, _type: c8._type, _id: values.id}});
          bulk.push(values);
        }
        // console.log(JSON.stringify(bulk, null, 2));
        if (bulk.length > 0) {
          c8.bulk(bulk).then(function(response) {
            let result = c8.trimBulkResults(response);
            if (result.errors) {
              var messages = [];
              for (var i=0; i<result.items.length; i++) {
                if (result.items[i].index.error) {
                  messages.push(i + ': ' + result.items[i].index.error.reason);
                }
              }
              console.error(messages.length + ' errors in bulk insert:\n ' + messages.join('\n '));
            }
            console.log('Indexed ' + result.items.length + ' documents in ' + result.took + ' ms.');
            fulfill('Got ' + resp.session.length + ' sessions.');
          }).catch(function(error) {
            console.error(error);
          });
        }
        else {
          console.log('No data available');
        }
      });
    }).catch(function(error) {
      console.trace(error);
    });
  });
};

module.exports = adapter;
