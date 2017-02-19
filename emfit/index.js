var QS = require('emfit-qs');

var MAX_DAYS = 30;
var MS_IN_DAY = 24 * 60 * 60 * 1000;

var adapter = {};

adapter.sensorName = 'emfit-qs';

var presenceIndex = 'emfit-qs-presence';
var trendIndex = 'emfit-qs-trend';

adapter.types = [
  {
    name: presenceIndex,
    fields: {
    timestamp: "date",
    hrv_rmssd_datapoints: {
    },
    bed_exit_periods: {
      exit_start: "date",
      exit_end: "date",
      exit_duration: "integer"
    },
    tossnturn_datapoints: "date"
    },
    note: "text",
/*
    ignored for now...
    measured_datapoints: {
    },
    nodata_periods: {
    },
    sleep_epoch_datapoints: {
    },
*/
  },
  {
    name: trendIndex,
    fields: {
      "timestamp": "date",
    }
  }
];

adapter.promptProps = {
  properties: {
    username: {
      description: 'Enter your username'.magenta
    },
    password: {
      description: 'Enter your password'.magenta
    }
  }
};

adapter.storeConfig = function(c8, result) {
  var conf = result;
  return c8.config(conf).then(function(){
    console.log('Configuration stored.');
  }).catch(function(error) {
    console.trace(error);
  });
};

adapter.importData = function(c8, conf, opts) {
  return new Promise(function (fulfill, reject){
    c8.type(presenceIndex).search({
      _source: ['timestamp'],
      size: 1,
      sort: [{'timestamp': 'desc'}],
    }).then(function(response) {
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
        var qs = new QS()
        qs.login(conf.username, conf.password).then(function(data) {
          var deviceId = data.device_settings[0].device_id
          Promise.all([getPresence(c8, qs, deviceId), getTrends(c8, qs, deviceId, firstDate, lastDate)]).then(function(messages) {
            fulfill(messages.join('\n'));
          }).catch(function(error) {
            reject(error);
          });
        }).catch(function(error) {
          reject(error);
        });
      }).catch(function(error) {
        reject(error);
      });
    }).catch(function(error) {
      reject(error);
    });
  });
};

/*
        latest.timestamp = new Date(latest.time_end * 1000);
        if (latest.bed_exit_periods && latest.bed_exit_periods.length) {
          for (var i=0; i<latest.bed_exit_periods.length; i++) {
            latest.bed_exit_periods[i] = {
              exit_start: new Date(latest.bed_exit_periods[i][0] * 1000),
              exit_end: new Date(latest.bed_exit_periods[i][1] * 1000),
              exit_duration: latest.bed_exit_periods[i][1] - latest.bed_exit_periods[i][0]
            }
          }
        }
        return c8.type(presenceIndex).insert(latest).then(function(result) {
          console.log(latest.timestamp);
        }).catch(function(error) {
          console.trace(error);
        });
*/

function getPresence(c8, qs, deviceId) {
  return new Promise(function (fulfill, reject){
    qs.latest(deviceId).then(function(latest) {
      if (latest.error) {
        reject(new Error('Could not index latest presence data: ' + latest.error));
      }
      var messages = [];
      if (latest.navigation_data && latest.navigation_data.length) {
        for (var i=0; i<latest.navigation_data.length; i++) {
          var periodId = latest.navigation_data[i].id;
          messages.push('Presence: ' + periodId);
          qs.presence(periodId, deviceId).then(function(presence) {
            presence.time_start = new Date(presence.time_start * 1000);
            presence.time_end = new Date(presence.time_end * 1000);
            presence.timestamp = presence.time_end;
            // TODO: bulk index all the data to separate types?
            delete(presence.measured_datapoints);
            delete(presence.nodata_periods);
            delete(presence.sleep_epoch_datapoints);
            delete(presence.hrv_rmssd_datapoints);
            delete(presence.hrv_rmssd_hist_data);
            delete(presence.navigation_data);
            delete(presence.minitrend_datestamps);
            delete(presence.minitrend_sleep_score);
            delete(presence.minitrend_sleep_efficiency);
            delete(presence.minitrend_sleep_duration);
            delete(presence.minitrend_time_in_bed_duration);
            delete(presence.minitrend_sleep_class_in_rem_duration);
            delete(presence.minitrend_sleep_class_in_light_duration);
            delete(presence.minitrend_sleep_class_in_deep_duration);
            delete(presence.minitrend_sleep_class_in_rem_percent);
            delete(presence.minitrend_sleep_class_in_light_percent);
            delete(presence.minitrend_sleep_class_in_deep_percent);
            delete(presence.minitrend_tossnturn_count);
            delete(presence.minitrend_measured_hr_avg);
            delete(presence.minitrend_measured_hr_max);
            delete(presence.minitrend_measured_hr_min);
            delete(presence.minitrend_measured_rr_avg);
            delete(presence.minitrend_measured_rr_min);
            delete(presence.minitrend_measured_rr_max);
            delete(presence.minitrend_measured_activity_avg);
            delete(presence.minitrend_hrv_rmssd_evening);
            delete(presence.minitrend_hrv_rmssd_morning);
            delete(presence.minitrend_hrv_lf);
            delete(presence.minitrend_hrv_recovery_total);
            delete(presence.minitrend_hrv_recovery_integrated);
            delete(presence.minitrend_hrv_recovery_ratio);
            delete(presence.minitrend_hrv_recovery_rate);
            
            if (presence.bed_exit_periods && presence.bed_exit_periods.length) {
              for (var i=0; i<presence.bed_exit_periods.length; i++) {
                presence.bed_exit_periods[i] = {
                  exit_start: new Date(presence.bed_exit_periods[i][1] * 1000),
                  exit_end: new Date(presence.bed_exit_periods[i][0] * 1000),
                  exit_duration: presence.bed_exit_periods[i][0] - presence.bed_exit_periods[i][1]
                }
              }
            }
            if (presence.tossnturn_datapoints && presence.tossnturn_datapoints.length) {
              for (var i=0; i<presence.tossnturn_datapoints.length; i++) {
                presence.tossnturn_datapoints[i] = new Date(presence.tossnturn_datapoints[i] * 1000);
              }
            }
            c8.type(presenceIndex).insert(presence).then(function(result) {
              console.log(presence.timestamp + ': ' + result.result);
            }).catch(function(error) {
              reject(error);
            });
          });
        }
      }
      fulfill(messages.join('\n'));
    });
  });
}

function getTrends(c8, qs, deviceId, startTime, endTime) {
  return new Promise(function (fulfill, reject){
    qs.trends(deviceId, startTime, endTime).then(function(trends) {
      if (trends.error || !trends.data) {
        if (trends.error == 'Unsufficient dataset') {
          fulfill('No trends available yet.');
        }
        else {
          reject(new Error('Could not index trends: ' + trends.error));
        }
      }
      else {
        var bulk = [];
        for (var i=0; i<trends.data.length; i++) {
          var trend = trends.data[i];
          var id = trend.date;
          trend.timestamp = new Date(trend.date);
          bulk.push({index: {_index: c8.type(trendIndex)._index, _type: c8._type, _id: id}});
          bulk.push(trend);
          console.log('Trend: ' + trend.timestamp);
        }
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
            fulfill('Indexed ' + result.items.length + ' trend documents in ' + result.took + ' ms.');
          }).catch(function(error) {
            reject(error);
          });
        }
        else {
          fulfill('No trends available');
        }
      }
    });
  });
}

module.exports = adapter;
