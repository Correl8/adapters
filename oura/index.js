var oura = require('oura'),
  moment = require('moment'),
  express = require('express'),
  url = require('url');

var redirectUri = 'http://localhost:6872/authcallback';

var MAX_DAYS = 10;
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
      "bedtime_start": "date",
      "bedtime_end": "date",
      "timezone": "integer",
      "duration": "integer",
      "score": "integer",
      "total": "integer",
      "awake": "integer",
      "rem": "integer",
      "light": "integer",
      "deep": "integer",
      "efficiency": "integer",
      "hr_low_duration": "integer",
      "hr_lowest": "integer",
      "wake_up_count": "integer",
      "onset_latency": "integer",
      "hr_average": "float",
      "midpoint_time": "integer",
      "restless": "integer",
      "got_up_count": "integer",
      "score_total": "integer",
      "score_deep": "integer",
      "score_rem": "integer",
      "score_efficiency": "integer",
      "score_latency": "integer",
      "score_disturbances": "integer",
      "score_alignment": "integer"
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
      "type": "integer",
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
    name: activityClassIndex,
    fields: {
      "timestamp": "date",
      "duration": "integer",
      "type": "integer",
      "class": "string"
    }
  },
  {
    name: activityMETIndex,
    fields: {
      "timestamp": "date",
      "duration": "integer",
      "moveType": "integer"
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
  c8.type(sleepSummaryIndex).search({
    _source: ['timestamp'],
    size: 1,
    sort: [{'timestamp': 'desc'}],
  }).then(function(response) {
    console.log('Getting first date...');
    return c8.search({
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
      importData(c8, conf, firstDate, lastDate);
    });
  }).catch(function(error) {
    console.trace(error);
  });
}

function importData(c8, conf, firstDate, lastDate) {
  var token = conf.access_token;
  var client = new oura.Client(token);
  if (!firstDate) {
    console.warn('No starting date...');
    return;
  }
  var start = moment(firstDate).format(dateFormat);
  var end = moment(lastDate).format(dateFormat);
  client.sleep(start, end).then(function (response) {
    var bulk = [];
    var obj = response.sleep;
    for (var i=0; i<obj.length; i++) {
      console.log(obj[i].summary_date);
      var sleepStateData = obj[i].hypnogram_5min.split("");
      var sleepHRData = obj[i].hr_10min;
      var id = obj[i].timestamp = obj[i].summary_date;
      bulk.push({index: {_index: c8.type(sleepSummaryIndex)._index, _type: c8._type, _id: id}});
      bulk.push(obj[i]);
      if (sleepHRData && sleepHRData.length) {
        var d = moment(obj[i].bedtime_start);
        var duration = 10 * 60; // seconds
        for (var j=0; j<sleepHRData.length; j++) {
          d.add(10, 'minutes');
          bulk.push({index: {_index: c8.type(sleepHRIndex)._index, _type: c8._type, _id: d.format()}});
            bulk.push({timestamp: d.format(), duration: duration, HR: sleepHRData[j]});
        }
      }
      if (sleepStateData && sleepStateData.length) {
        var d = moment(obj[i].bedtime_start);
        var duration = 5 * 60; // seconds
        for (var j=0; j<sleepStateData.length; j++) {
          d.add(5, 'minutes');
          bulk.push({index: {_index: c8.type(sleepStateIndex)._index, _type: c8._type, _id: d.format()}});
          bulk.push({timestamp: d.format(), duration: duration, type: sleepStateData[j], state: sleepStates[sleepStateData[j]]});
        }
      }
    }
    c8.bulk(bulk).then(function(result) {
      console.log('Indexed ' + result.items.length + ' sleep documents in ' + result.took + ' ms.');
    }).catch(function(error) {
      console.trace(error);
    });
  }).catch(function(error){
    console.error(error)
  })
  client.activity(start, end).then(function (response) {
    var bulk = [];
    var obj = response.activity;
    for (var i=0; i<obj.length; i++) {
      console.log(obj[i].summary_date);
      var activityClassData = obj[i].class_5min.split("");
      var activityMETData = obj[i].met_1min;
      var id = obj[i].timestamp = obj[i].summary_date;
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
          bulk.push({timestamp: d.format(), duration: duration, type: activityClassData[j], "class": activityClasses[activityClassData[j]]});
        }
      }
    }
    c8.bulk(bulk).then(function(result) {
      console.log('Indexed ' + result.items.length + ' activity documents in ' + result.took + ' ms.');
    }).catch(function(error) {
      console.trace(error);
    });
  }).catch(function(error){
    console.error(error)
  })
  client.readiness(start, end).then(function (response) {
    var bulk = [];
    var obj = response.readiness;
    for (var i=0; i<obj.length; i++) {
      console.log(obj[i].summary_date);
      var id = obj[i].timestamp = obj[i].summary_date;
      bulk.push({index: {_index: c8.type(readinessSummaryIndex)._index, _type: c8._type, _id: id}});
      bulk.push(obj[i]);
    }
    c8.bulk(bulk).then(function(result) {
      console.log('Indexed ' + result.items.length + ' readiness documents in ' + result.took + ' ms.');
    }).catch(function(error) {
      console.trace(error);
    });
  }).catch(function(error){
    console.error(error)
  })
}

module.exports = adapter;