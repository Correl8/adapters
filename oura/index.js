var oura = require('oura'),
  moment = require('moment'),
  express = require('express'),
  url = require('url');

var redirectUri = 'http://localhost:6872/authcallback';

var MAX_DAYS = 30;
var MS_IN_DAY = 24 * 60 * 60 * 1000;
var dateFormat = 'YYYY-MM-DD'

var sleepStates = [
  '',
  'deep',
  'light',
  'REM',
  'awake'
];

var activityClasses = [
  'non-wear',
  'rest',
  'inactive',
  'low intensity activity',
  'medium intensity activity',
  'high intensity activity'
]
var adapter = {};

adapter.sensorName = 'oura';

var sleepSummaryIndex = 'oura-sleep-summary';
var activitySummaryIndex = 'oura-activity-summary';
var readinessSummaryIndex = 'oura-readiness-summary';
var sleepStateIndex = 'oura-sleep-state';
var sleepHRIndex = 'oura-sleep-hr';
var sleepHRVIndex = 'oura-sleep-rmssd';
var activityClassIndex = 'oura-activity-class';
var activityMETIndex = 'oura-activity-met';

adapter.types = [
  {
    name: sleepSummaryIndex,
    fields: {
      "timestamp": "date",
      "summary_date": "date",
      "period_id": "integer",
      "is_longest": "boolean",
      "timezone": "integer",
      "bedtime_start": "date",
      "bedtime_end": "date",
      "score": "integer",
      "score_total": "integer",
      "score_disturbances": "integer",
      "score_efficiency": "integer",
      "score_latency": "integer",
      "score_rem": "integer",
      "score_deep": "integer",
      "score_alignment": "integer",
      "total": "integer",
      "duration": "integer",
      "awake": "integer",
      "light": "integer",
      "rem": "integer",
      "deep": "integer",
      "onset_latency": "integer",
      "restless": "integer",
      "efficiency": "integer",
      "midpoint_time": "integer",
      "hr_lowest": "integer",
      "hr_average": "float",
      "rmssd": "integer",
      "breath_average": "float",
      "temperature_delta": "float",
      "hr_low_duration": "integer", // not mentioned in the api docs anymore?
      "wake_up_count": "integer", // not mentioned in the api docs anymore?
      "got_up_count": "integer" // not mentioned in the api docs anymore?
    }
  },
  {
    name: activitySummaryIndex,
    fields: {
      "timestamp": "date",
      "summary_date": "date",
      "day_start": "date",
      "day_end": "date",
      "timezone": "integer",
      "score": "integer",
      "score_stay_active": "integer",
      "score_move_every_hour": "integer",
      "score_meet_daily_targets": "integer",
      "score_training_frequency": "integer",
      "score_training_volume": "integer",
      "score_recovery_time": "integer",
      "daily_movement": "integer",
      "non_wear": "integer",
      "rest": "integer",
      "inactive": "integer",
      "inactivity_alerts": "integer",
      "low": "integer",
      "medium": "integer",
      "high": "integer",
      "steps": "integer",
      "cal_total": "integer",
      "cal_active": "integer",
      "met_min_inactive": "integer",
      "met_min_low": "integer",
      "met_min_medium_plus": "integer",
      "met_min_medium": "integer",
      "met_min_high": "integer",
      "average_met": "float",
    }
  },
  {
    name: readinessSummaryIndex,
    fields: {
      "timestamp": "date",
      "summary_date": "date",
      "period_id": "integer",
      "score": "integer",
      "score_previous_night": "integer",
      "score_sleep_balance": "integer",
      "score_previous_day": "integer",
      "score_activity_balance": "integer",
      "score_resting_hr": "integer",
      "score_recovery_index": "integer",
      "score_temperature": "integer"
    }
  },
  {
    name: sleepStateIndex,
    fields: {
      "timestamp": "date",
      "duration": "integer",
      "state_id": "integer",
      "state": "string"
    }
  },
  {
    name: sleepHRIndex,
    fields: {
      "timestamp": "date",
      "duration": "integer",
      "hr": "integer"
    }
  },
  {
    name: sleepHRVIndex,
    fields: {
      "timestamp": "date",
      "duration": "integer",
      "rmssd": "integer"
    }
  },
  {
    name: activityClassIndex,
    fields: {
      "timestamp": "date",
      "duration": "integer",
      "activity_id": "integer",
      "class": "string"
    }
  },
  {
    name: activityMETIndex,
    fields: {
      "timestamp": "date",
      "duration": "integer",
      "met": "float"
    }
  },
];

adapter.promptProps = {
  properties: {
    client_id: {
      description: 'Enter your ŌURA client ID'.magenta
    },
    client_secret: {
      description: 'Enter your ŌURA client secret'.magenta
    },
    redirect_uri: {
      description: 'Enter your redirect URL'.magenta,
      default: redirectUri
    },
  }
};

adapter.storeConfig = function(c8, result) {
  var conf = result;
  return c8.config(conf).then(function(){
    var defaultUrl = url.parse(conf.redirect_uri);
    var express = require('express');
    var app = express();
    var port = process.env.PORT || defaultUrl.port;

    var options = {
      clientId: conf.client_id,
      clientSecret: conf.client_secret,
      redirectUri: conf.redirect_uri
    };
    var authClient = oura.Auth(options);
    var authUri = authClient.code.getUri();
    var server = app.listen(port, function () {
      console.log("Please, go to \n" + authUri)
    });

    app.get(defaultUrl.pathname, function (req, res) {
      return authClient.code.getToken(req.originalUrl).then(function(auth) {
        return auth.refresh().then(function(refreshed) {
          Object.assign(conf, refreshed.data);
          server.close();
          return c8.config(conf).then(function(){
            res.send('Access token saved.');
            console.log('Configuration stored.');
            c8.release();
            process.exit();
          }).catch(function(error) {
            console.trace(error);
          });
        }).catch(function(error) {
          console.trace(error);
        });
      }).catch(function(error) {
        console.trace(error);
      });
    });
  }).catch(function(error) {
    console.trace(error);
  });
};

adapter.importData = function(c8, conf, opts) {
  return new Promise(function (fulfill, reject){
    c8.type(sleepSummaryIndex).search({
      _source: ['timestamp'],
      size: 1,
      sort: [{'timestamp': 'desc'}],
    }).then(function(response) {
      console.log('Getting first date...');
      c8.search({
        _source: ['timestamp'],
        size: 1,
        sort: [{'timestamp': 'desc'}],
      }).then(function(response) {
        var resp = c8.trimResults(response);
        var firstDate = new Date();
        var lastDate = opts.lastDate || new Date();
        firstDate.setTime(lastDate.getTime() - (MAX_DAYS * MS_IN_DAY));
        if (opts.firstDate) {
          firstDate = new Date(opts.firstDate);
          console.log('Setting first time to ' + firstDate);
        }
        else if (resp && resp.timestamp) {
          var d = new Date(resp.timestamp);
          firstDate.setTime(d.getTime() + 1);
          console.log('Setting first time to ' + firstDate);
        }
        if (lastDate.getTime() > (firstDate.getTime() + MAX_DAYS * MS_IN_DAY)) {
          lastDate.setTime(firstDate.getTime() + MAX_DAYS * MS_IN_DAY);
          console.warn('Setting last date to ' + lastDate);
        }
        importData(c8, conf, firstDate, lastDate).then(function(message) {
          fulfill(message);
        }).catch(function(error) {
          reject(error);
        });
      });
    }).catch(function(error) {
      reject(error);
    });
  });
}

function importData(c8, conf, firstDate, lastDate) {
  return new Promise(function (fulfill, reject){
    var options = {
      clientId: conf.client_id,
      clientSecret: conf.client_secret,
      redirectUri: conf.redirect_uri
    };
    var token = conf.access_token;
    var authClient = oura.Auth(options);
    var auth = authClient.createToken(conf.access_token, conf.refresh_token);
    auth.refresh().then(function(refreshed) {
      Object.assign(conf, refreshed.data);
      c8.config(conf).then(function(){
        var client = new oura.Client(conf.access_token);
        if (!firstDate) {
          reject('No starting date...');
          return;
        }
        var start = moment(firstDate).format(dateFormat);
        var end = moment(lastDate).format(dateFormat);
        Promise.all([
          getSleep(c8, client, start, end),
          getActivity(c8, client, start, end),
          getReadiness(c8, client, start, end),
        ]).then(function(values){
            fulfill(values.join('\n'));
        }).catch(function(error){
          reject(error)
        });
      }).catch(function(error){
        reject(error)
      });
    }).catch(function(error){
      reject(error)
    });
  });
}

function getSleep(c8, client, start, end) {
  return new Promise(function (fulfill, reject){
    client.sleep(start, end).then(function (response) {
      var bulk = [];
      var obj = response.sleep;
      for (var i=0; i<obj.length; i++) {
        obj[i].timestamp = moment(obj[i].bedtime_end);
        obj[i].is_longest = obj[i].is_longest ? true : false;
        // var summaryDate = obj[i].summary_date;
        // var summaryDate = obj[i].timestamp.format('YYYY-MM-DD');
        var sleepStateData = obj[i].hypnogram_5min ? obj[i].hypnogram_5min.split("") : [];
        var sleepHRData = obj[i].hr_5min;
        var sleepHRVData = obj[i].rmssd_5min;
        var id = obj[i].summary_date + '.' + obj[i].period_id;
        console.log(id + ': sleep score ' + obj[i].score + '; ' + Math.round(obj[i].duration/360)/10 + ' hours of sleep');
        bulk.push({index: {_index: c8.type(sleepSummaryIndex)._index, _type: c8._type, _id: id}});
        bulk.push(obj[i]);
        // console.log(JSON.stringify(obj[i], null, 1));
        if (sleepHRData && sleepHRData.length) {
          var d = moment(obj[i].bedtime_start);
          var duration = 5 * 60; // seconds
          for (var j=0; j<sleepHRData.length; j++) {
            d.add(5, 'minutes');
            if ((sleepHRData[j] == 0) || (sleepHRData[j] == 255)) {
              // ignore value, it's likely faulty
              continue;
            }
            bulk.push({index: {_index: c8.type(sleepHRIndex)._index, _type: c8._type, _id: d.format()}});
            bulk.push({timestamp: d.format(), duration: duration, hr: sleepHRData[j]});
          }
        }
        if (sleepHRVData && sleepHRVData.length) {
          var d = moment(obj[i].bedtime_start);
          var duration = 5 * 60; // seconds
          for (var j=0; j<sleepHRVData.length; j++) {
            d.add(5, 'minutes');
            if ((sleepHRVData[j] == 0) || (sleepHRVData[j] == 255)) {
              // ignore value, it's likely faulty
              continue;
            }
            bulk.push({index: {_index: c8.type(sleepHRVIndex)._index, _type: c8._type, _id: d.format()}});
            bulk.push({timestamp: d.format(), duration: duration, hr: sleepHRVData[j]});
          }
        }
        if (sleepStateData && sleepStateData.length) {
          var d = moment(obj[i].bedtime_start);
          var duration = 5 * 60; // seconds
          for (var j=0; j<sleepStateData.length; j++) {
            d.add(5, 'minutes');
            bulk.push({index: {_index: c8.type(sleepStateIndex)._index, _type: c8._type, _id: d.format()}});
            bulk.push({timestamp: d.format(), duration: duration, state_id: sleepStateData[j], state: sleepStates[sleepStateData[j]]});
          }
        }
      }
      if (bulk.length > 0) {
        // console.log(JSON.stringify(bulk, null, 1));
        c8.bulk(bulk).then(function(response) {
          let result = c8.trimBulkResults(response);
          if (result.errors) {
            var messages = [];
            for (var i=0; i<result.items.length; i++) {
              if (result.items[i].index.error) {
                messages.push(i + ': ' + result.items[i].index.error.reason);
              }
            }
            reject(new Error(messages.length + ' errors in bulk insert:\n ' + messages.join('\n ')));
          }
          fulfill('Indexed ' + result.items.length + ' sleep documents in ' + result.took + ' ms.');
        }).catch(function(error) {
          reject(error);
          bulk = null;
        });
      }
      else {
        fulfill('No sleep to import');
      }
    }).catch(function(error){
      reject(error)
    });
  });
}

function getActivity(c8, client, start, end) {
  return new Promise(function (fulfill, reject){
    client.activity(start, end).then(function (response) {
      // console.log(JSON.stringify(response, null, 1));
      var bulk = [];
      var obj = response.activity;
      for (var i=0; i<obj.length; i++) {
        obj[i].timestamp = obj[i].summary_date;
        // console.log(obj[i].summary_date);
        var activityClassData = obj[i].class_5min.split("");
        var activityMETData = obj[i].met_1min;
        var id = obj[i].summary_date;
        console.log(id + ': activity score ' + obj[i].score);
        bulk.push({index: {_index: c8.type(activitySummaryIndex)._index, _type: c8._type, _id: id}});
        bulk.push(obj[i]);
        if (activityMETData && activityMETData.length) {
          var d = moment(obj[i].summary_date).hour(4).minute(0).second(0).millisecond(0);
          var duration = 60; // seconds
          for (var j=0; j<activityMETData.length; j++) {
            d.add(1, 'minutes');
            bulk.push({index: {_index: c8.type(activityMETIndex)._index, _type: c8._type, _id: d.format()}});
            bulk.push({timestamp: d.format(), duration: duration, met: activityMETData[j]});
          }
        }
        if (activityClassData && activityClassData.length) {
          var d = moment(obj[i].summary_date).hour(4).minute(0).second(0).millisecond(0);
          var duration = 5 * 60; // seconds
          for (var j=0; j<activityClassData.length; j++) {
            d.add(5, 'minutes');
            bulk.push({index: {_index: c8.type(activityClassIndex)._index, _type: c8._type, _id: d.format()}});
            bulk.push({timestamp: d.format(), duration: duration, activity_id: activityClassData[j], "class": activityClasses[activityClassData[j]]});
          }
        }
      }
      if (bulk.length > 0) {
        // console.log(JSON.stringify(bulk, null, 1));
        c8.bulk(bulk).then(function(response) {
          let result = c8.trimBulkResults(response);
          fulfill('Indexed ' + result.items.length + ' activity documents in ' + result.took + ' ms.');
        }).catch(function(error) {
          reject(error);
        });
      }
      else {
        fulfill('No activity to import');
      }
    }).catch(function(error) {
      reject(error);
    });
  });
}

function getReadiness(c8, client, start, end) {
  return new Promise(function (fulfill, reject){
    client.readiness(start, end).then(function (response) {
      var bulk = [];
      var obj = response.readiness;
      for (var i=0; i<obj.length; i++) {
        var summaryDate = obj[i].summary_date;
        console.log(summaryDate + ': readiness score ' + obj[i].score);
        obj[i].timestamp = moment(obj[i].summary_date).hour(4).add(1, 'days');
        var id = obj[i].summary_date + '.' + obj[i].period_id;
        bulk.push({index: {_index: c8.type(readinessSummaryIndex)._index, _type: c8._type, _id: id}});
        bulk.push(obj[i]);
      }
      if (bulk.length > 0) {
        c8.bulk(bulk).then(function(response) {
          let result = c8.trimBulkResults(response);
          fulfill('Indexed ' + result.items.length + ' readiness documents in ' + result.took + ' ms.');
        }).catch(function(error) {
          reject(error);
        });
      }
      else {
        fulfill('No readiness data to import');
      }
    }).catch(function(error){
      reject(error)
    });
  });
}

module.exports = adapter;
