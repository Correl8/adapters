const oura = require('oura'),
  moment = require('moment'),
  express = require('express'),
  url = require('url');

const { v4: uuidv4 } = require('uuid');

// proxy forward redirectUri to http://localhost:{authPort}
const redirectUri = 'https://correl8.me/authcallback';
const authPort = 4343;

const MAX_DAYS = 30;
const MS_IN_DAY = 24 * 60 * 60 * 1000;
const dateFormat = 'YYYY-MM-DD'

const sleepStates = [
  '',
  'deep',
  'light',
  'REM',
  'awake'
];

const activityClasses = [
  'non-wear',
  'rest',
  'inactive',
  'low intensity activity',
  'medium intensity activity',
  'high intensity activity'
]
const adapter = {};

adapter.sensorName = 'oura';

const sleepSummaryIndex = 'oura-sleep-summary';
const activitySummaryIndex = 'oura-activity-summary';
const readinessSummaryIndex = 'oura-readiness-summary';
const sleepStateIndex = 'oura-sleep-state';
const sleepHRIndex = 'oura-sleep-hr';
const sleepHRVIndex = 'oura-sleep-rmssd';
const activityClassIndex = 'oura-activity-class';
const activityMETIndex = 'oura-activity-met';

adapter.types = [
  {
    name: sleepSummaryIndex,
    fields: {
      "@timestamp": "date",
      "ecs": {
        "version": 'keyword'
      },
      "event": {
        "created": "date",
        "dataset": "keyword",
        "duration": "long",
        "end": "date",
        "module": "keyword",
        "original": "keyword",
        "start": "date",
        "timezone": "keyword"
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
      "sleep": {
        "summary_date": "date",
        "period_id": "long",
        "is_longest": "boolean",
        "timezone": "keyword",
        "bedtime_start": "date",
        "bedtime_end": "date",
        "score": {
          "value": "float",
          "total": "float",
          "disturbances": "float",
          "efficiency": "float",
          "latency": "float",
          "rem": "float",
          "deep": "float",
          "alignment": "float",
        },
        "duration_seconds": "long",
        "total_seconds": "long",
        "awake_seconds": "long",
        "light_seconds": "long",
        "rem_seconds": "long",
        "deep_seconds": "long",
        "onset_latency_seconds": "long",
        "restless_percentage": "float",
        "efficiency_percentage": "float",
        "midpoint_time_seconds": "long",
        "hr_lowest": "float",
        "hr_average": "float",
        "rmssd_milliseconds": "float",
        "breath_average": "float",
        "temperature_delta": "float",
        "hr_low_duration": "long", // not mentioned in the api docs anymore?
        "wake_up_count": "long", // not mentioned in the api docs anymore?
        "got_up_count": "long" // not mentioned in the api docs anymore?
      },
    }
  },
  {
    name: activitySummaryIndex,
    fields: {
      "@timestamp": "date",
      "event": {
        "created": "date",
        "dataset": "keyword",
        "duration": "long",
        "end": "date",
        "module": "keyword",
        "original": "keyword",
        "start": "date",
        "timezone": "keyword"
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
      "activity": {
        "summary_date": "date",
        "period_id": "long",
        "timezone": "keyword",
        "day_start": "date",
        "day_end": "date",
        "score": {
          "value": "float",
          "stay_active": "float",
          "move_every_hour": "float",
          "meet_daily_targets": "float",
          "training_frequency": "float",
          "training_volume": "float",
          "recovery_time": "float",
        },
        "daily_movement_meters": "long",
        "non_wear_minutes": "long",
        "rest_minutes": "long",
        "inactive_minutes": "long",
        "inactivity_alerts": "long",
        "low_minutes": "long",
        "medium_minutes": "long",
        "high_minutes": "long",
        "steps": "long",
        "cal": {
          "total": "long",
          "cal_active": "long",
        },
        "met": {
          "min_inactive": "long",
          "min_low": "long",
          "min_medium_plus": "long",
          "min_medium": "long",
          "min_high": "long",
          "average": "float",
        }
      }
    }
  },
  {
    name: readinessSummaryIndex,
    fields: {
      "@timestamp": "date",
      "event": {
        "created": "date",
        "dataset": "keyword",
        "duration": "long",
        "end": "date",
        "module": "keyword",
        "original": "keyword",
        "start": "date",
        "timezone": "keyword"
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
      "readiness": {
        "summary_date": "date",
        "period_id": "long",
        // "timezone": "keyword", // not mentioned in API docs
        // "day_start": "date", // not mentioned in API docs
        // "day_end": "date", // not mentioned in API docs
        "score": {
          "value": "float",
          "previous_night": "float",
          "sleep_balance": "float",
          "previous_day": "float",
          "activity_balance": "float",
          "resting_hr": "float",
          "recovery_index": "float",
          "temperature": "float",
        },
      }
    }
  },
  {
    name: sleepStateIndex,
    fields: {
      "@timestamp": "date",
      "event": {
        "created": "date",
        "dataset": "keyword",
        "duration": "long",
        "end": "date",
        "module": "keyword",
        "original": "keyword",
        "start": "date",
        "timezone": "keyword"
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
      "sleep": {
        "state": {
          "id": "long",
          "name": "keyword"
        }
      }
    }
  },
  {
    name: sleepHRIndex,
    fields: {
      "@timestamp": "date",
      "event": {
        "created": "date",
        "dataset": "keyword",
        "duration": "long",
        "end": "date",
        "module": "keyword",
        "original": "keyword",
        "start": "date",
        "timezone": "keyword"
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
      "sleep": {
        "hr": "float"
      }
    }
  },
  {
    name: sleepHRVIndex,
    fields: {
      "@timestamp": "date",
      "event": {
        "created": "date",
        "dataset": "keyword",
        "duration": "long",
        "end": "date",
        "module": "keyword",
        "original": "keyword",
        "start": "date",
        "timezone": "keyword"
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
      "sleep": {
        "hrv": {
          "rmssd": "float",
        }
      }
    }
  },
  {
    name: activityClassIndex,
    fields: {
      "@timestamp": "date",
      "event": {
        "created": "date",
        "dataset": "keyword",
        "duration": "long",
        "end": "date",
        "module": "keyword",
        "original": "keyword",
        "start": "date",
        "timezone": "keyword"
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
      "activity": {
        "class": {
          "id": "long",
          "name": "keyword"
        }
      }
    }
  },
  {
    name: activityMETIndex,
    fields: {
      "@timestamp": "date",
      "event": {
        "created": "date",
        "dataset": "keyword",
        "duration": "long",
        "end": "date",
        "module": "keyword",
        "original": "keyword",
        "start": "date",
        "timezone": "keyword"
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
      "activity": {
        "met": {
          "average": "float"
        }
      }
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

adapter.storeConfig = async function(c8, result) {
  let conf = result;

  try {
    await c8.config(conf);
    const defaultUrl = url.parse(conf.redirect_uri);
    const express = require('express');
    const app = express();
    const state = uuidv4();
    
    const options = {
      clientId: conf.client_id,
      clientSecret: conf.client_secret,
      redirectUri: conf.redirect_uri,
      state: state
    };
    const authClient = oura.Auth(options);
    const authUri = authClient.code.getUri();
    const server = app.listen(authPort, function () {
      console.log("Please, go to \n" + authUri)
    });
    
    app.get(defaultUrl.pathname, async function (req, res) {
      const auth = await authClient.code.getToken(req.originalUrl);
      const refreshed = await auth.refresh();
      Object.assign(conf, refreshed.data);
      server.close();
      await c8.config(conf);
      res.send('Access token saved.');
      console.log('Configuration stored.');
      c8.release();
      process.exit();
    });
  }
  catch (error) {
    console.error(error);
  }
};

adapter.importData = function(c8, conf, opts) {
  return new Promise(async function (fulfill, reject){
    try {
      let response = await c8.type(sleepSummaryIndex).search({
        _source: ['@timestamp'],
        size: 1,
        sort: [{'@timestamp': 'desc'}],
      });
      console.log('Getting first date...');
      response = await c8.search({
        _source: ['@timestamp'],
        size: 1,
        sort: [{'@timestamp': 'desc'}],
      });
      const resp = c8.trimResults(response);
      let firstDate = new Date();
      let lastDate = opts.lastDate || new Date();
      firstDate.setTime(lastDate.getTime() - (MAX_DAYS * MS_IN_DAY));
      if (opts.firstDate) {
        firstDate = new Date(opts.firstDate);
        console.log('Setting first time to ' + firstDate);
      }
      else if (resp && resp["@timestamp"]) {
        const d = new Date(resp["@timestamp"]);
        firstDate.setTime(d.getTime() + 1);
        console.log('Setting first time to ' + firstDate);
      }
      if (lastDate.getTime() > (firstDate.getTime() + MAX_DAYS * MS_IN_DAY)) {
        lastDate.setTime(firstDate.getTime() + MAX_DAYS * MS_IN_DAY);
        console.warn('Setting last date to ' + lastDate);
      }
      const message = importData(c8, conf, firstDate, lastDate);
      fulfill(message);
    }
    catch (error) {
      reject(error);
    }
  });
}

function importData(c8, conf, firstDate, lastDate) {
  return new Promise(async function (fulfill, reject){
    const options = {
      clientId: conf.client_id,
      clientSecret: conf.client_secret,
      redirectUri: conf.redirect_uri
    };
    const token = conf.access_token;
    const authClient = oura.Auth(options);
    const auth = authClient.createToken(conf.access_token, conf.refresh_token);
    try {
      let refreshed = await auth.refresh();
      Object.assign(conf, refreshed.data);
      await c8.config(conf);
      const client = new oura.Client(conf.access_token);
      if (!firstDate) {
        // reject('No starting date...');
        return;
      }
      let start = moment(firstDate).format(dateFormat);
      let end = moment(lastDate).format(dateFormat);
      const values = await Promise.all([
        getSleep(c8, client, start, end),
        getActivity(c8, client, start, end),
        getReadiness(c8, client, start, end),
      ]);
      fulfill(values.join('\n'));
    }
    catch (error){
      reject(error);
    }
  });
}

function getSleep(c8, client, start, end) {
  return new Promise(async function (fulfill, reject){
    let response = await client.sleep(start, end);
    let bulk = [];
    let obj = response.sleep;
    // console.log(JSON.stringify(obj, null, 1));
    for (var i=0; i<obj.length; i++) {
      let sleep = obj[i];
      let t = moment(sleep.bedtime_start);
      let data = {
        "@timestamp": moment(sleep.bedtime_end),
        "ecs": {
          "version": "1.6.0"
        },
        "event": {
          "created": moment(sleep.bedtime_end).format(),
          "dataset": "oura.sleep",
          "duration": sleep.duration * 1E9,
          "end": moment(sleep.bedtime_end).format(),
          "ingested": new Date(),
          "kind": "event",
          "module": "oura",
          "original": JSON.stringify(sleep),
          "sequence": sleep.period_id,
          "start": t.format(),
          "timezone": mins2ts(sleep.timezone)
        },
        "date_details": {
          "year": t.format('YYYY'),
          "month": {
            "number": t.format('M'),
            "name": t.format('MMMM'),
          },
          "week_number": t.format('W'),
          "day_of_year": t.format('DDD'),
          "day_of_month": t.format('D'),
          "day_of_week": {
            "number": t.format('d'),
            "name": t.format('dddd'),
          }
        },
        "sleep": {
          "summary_date": sleep.summary_date,
          "period_id": sleep.period_id,
          "is_longest": sleep.is_longest ? true : false,
          "timezone": sleep.timezone,
          "bedtime_start": moment(sleep.bedtime_start),
          "bedtime_end": moment(sleep.bedtime_end),
          "score": {
            "value": sleep.score,
            "total": sleep.score_total,
            "disturbances": sleep.score_disturbances,
            "efficiency": sleep.score.efficiency,
            "latency": sleep.score.latency,
            "rem": sleep.score_rem,
            "deep": sleep.score_deep,
            "alignment": sleep.score.alignment,
          },
          "duration_seconds": sleep.duration,
          "total_seconds": sleep.total,
          "awake_seconds": sleep.awake,
          "light_seconds": sleep.light,
          "rem_seconds": sleep.rem,
          "deep_seconds": sleep.deep,
          "onset_latency_seconds": sleep.onset_latency,
          "restless_percentage": sleep.restlessness,
          "efficiency_percentage": sleep.efficiency,
          "midpoint_time_seconds": sleep.midpoint_time,
          "hr_lowest": sleep.hr_lowest,
          "hr_average": sleep.hr_average,
          "rmssd_milliseconds": sleep.rmssd,
          "breath_average": sleep.breath_average,
          "temperature_delta": sleep.temperature_delta,
          "hr_low_duration": sleep.hr_low_duration,
          "wake_up_count": sleep.wake_up_count,
          "got_up_count": sleep.got_up_count
        }
      }
      let sleepStateData = sleep.hypnogram_5min ? sleep.hypnogram_5min.split("") : [];
      let sleepHRData = sleep.hr_5min;
      let sleepHRVData = sleep.rmssd_5min;
      let id = sleep.summary_date + '.' + sleep.period_id;
      console.log(id + ': sleep score ' + sleep.score + '; ' + Math.round(sleep.duration/360)/10 + ' hours of sleep');
      bulk.push({index: {_index: c8.type(sleepSummaryIndex)._index, _id: id}});
      bulk.push(data);
      // console.log(JSON.stringify(data, null, 1));
      if (sleepHRData && sleepHRData.length) {
        let d = moment(obj[i].bedtime_start);
        let duration = 5 * 60; // seconds
        for (var j=0; j<sleepHRData.length; j++) {
          // console.log('Found ' + j + ': ' + d.format());
          if ((sleepHRData[j] == 0) || (sleepHRData[j] == 255)) {
            // ignore value, it's likely faulty
            d.add(duration, 'seconds');
            // console.log('Skipping ' + d.format());
            continue;
          }
          let data = {
            "@timestamp": d.format(),
            "ecs": {
              "version": "1.6.0"
            },
            "date_details": {
              "year": d.format('YYYY'),
              "month": {
                "number": d.format('M'),
                "name": d.format('MMMM'),
              },
              "week_number": d.format('W'),
              "day_of_year": d.format('DDD'),
              "day_of_month": d.format('D'),
              "day_of_week": {
                "number": d.format('d'),
                "name": d.format('dddd'),
              }
            },
            "time_slice": time2slice(d),
            "event": {
              "created": moment(sleep.bedtime_end).format(),
              "dataset": "oura.sleep",
              "duration": duration * 1E9,
              "ingested": new Date(),
              "kind": "metric",
              "module": "oura",
              "original": sleepHRData[j],
              "sequence": j,
              "start": d.format(),
              "end": d.add(duration, 'seconds').format(),
            },
            "sleep": {
              "hr": sleepHRData[j]
            }
          };
          bulk.push({index: {_index: c8.type(sleepHRIndex)._index, _id: d.format()}});
          bulk.push(data);
        }
      }
      if (sleepHRVData && sleepHRVData.length) {
        let d = moment(obj[i].bedtime_start);
        let duration = 5 * 60; // seconds
        for (var j=0; j<sleepHRVData.length; j++) {
          if ((sleepHRVData[j] == 0) || (sleepHRVData[j] == 255)) {
            // ignore value, it's likely faulty
            d.add(duration, 'seconds');
            continue;
          }
          let data = {
            "@timestamp": d.format(),
            "ecs": {
              "version": "1.6.0"
            },
            "date_details": {
              "year": d.format('YYYY'),
              "month": {
                "number": d.format('M'),
                "name": d.format('MMMM'),
              },
              "week_number": d.format('W'),
              "day_of_year": d.format('DDD'),
              "day_of_month": d.format('D'),
              "day_of_week": {
                "number": d.format('d'),
                "name": d.format('dddd'),
              }
            },
            "time_slice": time2slice(d),
            "event": {
              "created": moment(sleep.bedtime_end).format(),
              "dataset": "oura.sleep",
              "duration": duration * 1E9,
              "ingested": new Date(),
              "kind": "metric",
              "module": "oura",
              "original": sleepHRVData[j],
              "sequence": j,
              "start": d.format(),
              "end": d.add(duration, 'seconds').format(),
            },
            "sleep": {
              "hrv": {
                "rmssd": sleepHRVData[j]
              }
            }
          };
          bulk.push({index: {_index: c8.type(sleepHRVIndex)._index, _id: d.format()}});
          bulk.push(data);
        }
      }
      if (sleepStateData && sleepStateData.length) {
        let d = moment(obj[i].bedtime_start);
        let duration = 5 * 60; // seconds
        for (var j=0; j<sleepStateData.length; j++) {
          let data = {
            "@timestamp": d.format(),
            "ecs": {
              "version": "1.6.0"
            },
            "date_details": {
              "year": d.format('YYYY'),
              "month": {
                "number": d.format('M'),
                "name": d.format('MMMM'),
              },
              "week_number": d.format('W'),
              "day_of_year": d.format('DDD'),
              "day_of_month": d.format('D'),
              "day_of_week": {
                "number": d.format('d'),
                "name": d.format('dddd'),
              }
            },
            "time_slice": time2slice(d),
            "event": {
              "created": moment(sleep.bedtime_end).format(),
              "dataset": "oura.sleep",
              "duration": duration * 1E9,
              "ingested": new Date(),
              "kind": "state",
              "module": "oura",
              "original": sleepHRVData[j],
              "sequence": j,
              "original": sleepStateData[j],
              "start": d.format(),
              "end": d.add(duration, 'seconds').format(),
            },
            "sleep": {
              "state": {
                "id": sleepStateData[j],
                "name": sleepStates[sleepStateData[j]]
              }
            }
          };
          bulk.push({index: {_index: c8.type(sleepStateIndex)._index, _id: d.format()}});
          bulk.push(data);
        }
      }
    }
    if (bulk.length > 0) {
      // console.log(JSON.stringify(bulk, null, 1));
      try {
        let response = await c8.bulk(bulk);
        let result = c8.trimBulkResults(response);
        if (result && result.errors) {
          let messages = [];
          for (var i=0; i<result.items.length; i++) {
            if (result.items[i].index.error) {
              messages.push(i + ': ' + result.items[i].index.error.reason);
            }
          }
          reject(new Error(messages.length + ' errors in bulk insert:\n ' + messages.join('\n ')));
        }
        else if (!result) {
          reject(new Error(result));
        }
        fulfill('Indexed ' + result.items.length + ' sleep documents in ' + result.took + ' ms.');
      }
      catch (error) {
        reject(error);
        bulk = null;
      }
    }
    else {
      fulfill('No sleep to import');
    }
  });
}

function getActivity(c8, client, start, end) {
  return new Promise(async function (fulfill, reject){
    let response = await client.activity(start, end);
    let obj = response.activity;
    let bulkResponses = [];
    for (var i=0; i<obj.length; i++) {
      let bulk = [];
      let activity = obj[i];
      let startMoment = moment(activity.day_start);
      let endMoment = moment(activity.day_end);
      let data = {
        "@timestamp": moment(activity.summary_date),
        "ecs": {
          "version": "1.6.0"
        },
        "event": {
          "created": endMoment.format(),
          "dataset": "oura.activity",
          "duration": endMoment.diff(startMoment) * 1E6, // moment diff is milliseconds, want nanos
          "end": endMoment.format(),
          "ingested": new Date(),
          "kind": "event",
          "module": "oura",
          "original": JSON.stringify(activity),
          "sequence": activity.period_id,
          "start": startMoment.format(),
          "timezone": mins2ts(activity.timezone)
        },
        "date_details": {
          "year": startMoment.format('YYYY'),
          "month": {
            "number": startMoment.format('M'),
            "name": startMoment.format('MMMM'),
          },
          "week_number": startMoment.format('W'),
          "day_of_year": startMoment.format('DDD'),
          "day_of_month": startMoment.format('D'),
          "day_of_week": {
            "number": startMoment.format('d'),
            "name": startMoment.format('dddd'),
          }
        },
        "activity": {
          "summary_date": activity.summary_date,
          "period_id": activity.period_id,
          "timezone": activity.timezone,
          "day_start": startMoment.format(),
          "day_end": endMoment.format(),
          "score": {
            "value": activity.score,
            "stay_active": activity.score_stay_active,
            "move_every_hour": activity.score_move_every_hour,
            "meet_daily_targets": activity.score_meet_daily_targets,
            "training_frequency": activity.score_training_frequency,
            "training_volume": activity.score_training_volume,
            "recovery_time": activity.score_recovery_time,
          },
        },
        "daily_movement_meters": activity.daily_movement,
        "non_wear_minutes": activity.non_wear,
        "rest_minutes": activity.rest,
        "inactive_minutes": activity.inactive,
        "inactivity_alerts": activity.inactivity_alerts,
        "low_minutes": activity.low,
        "medium_minutes": activity.medium,
        "high_minutes": activity.high,
        "steps": activity.steps,
        "cal": {
          "total": activity.cal_total,
          "cal_active": activity.cal_active,
        },
        "met": {
          "min_inactive": activity.met_min_active,
          "min_low": activity.met_min_low,
          "min_medium_plus": activity.met_min_medium_plus,
          "min_medium": activity.met_min_medium,
          "min_high": activity.met_min_high,
          "average": activity.average_met,
        }
      }
      
      let activityClassData = activity.class_5min.split("");
      let activityMETData = activity.met_1min;
      let id = activity.summary_date;
      console.log(id + ': activity score ' + activity.score);
      bulk.push({index: {_index: c8.type(activitySummaryIndex)._index, _id: id}});
      bulk.push(data);
      if (activityMETData && activityMETData.length) {
        let d = moment(activity.summary_date).hour(4).minute(0).second(0).millisecond(0);
        let duration = 60; // seconds
        for (var j=0; j<activityMETData.length; j++) {
          let data = {
            "@timestamp": d.format(),
            "ecs": {
              "version": "1.6.0"
            },
            "date_details": {
              "year": d.format('YYYY'),
              "month": {
                "number": d.format('M'),
                "name": d.format('MMMM'),
              },
              "week_number": d.format('W'),
              "day_of_year": d.format('DDD'),
              "day_of_month": d.format('D'),
              "day_of_week": {
                "number": d.format('d'),
                "name": d.format('dddd'),
              }
            },
            "time_slice": time2slice(d),
            "event": {
              "created": endMoment.format(),
              "dataset": "oura.activity",
              "duration": duration * 1E9,
              "ingested": new Date(),
              "kind": "metric",
              "module": "oura",
              "original": activityMETData[j],
              "sequence": j,
              "start": d.format(),
              "end": d.add(duration, 'seconds').format(),
            },
            "activity": {
              "met": {
                "average": activityMETData[j]
              }
            }
          };
          bulk.push({index: {_index: c8.type(activityMETIndex)._index, _id: d.format()}});
          bulk.push(data);
        }
      }
      if (activityClassData && activityClassData.length) {
        let d = moment(activity.summary_date).hour(4).minute(0).second(0).millisecond(0);
        let duration = 5 * 60; // seconds
        for (var j=0; j<activityClassData.length; j++) {
          let data = {
            "@timestamp": d.format(),
            "ecs": {
              "version": "1.6.0"
            },
            "date_details": {
              "year": d.format('YYYY'),
              "month": {
                "number": d.format('M'),
                "name": d.format('MMMM'),
              },
              "week_number": d.format('W'),
              "day_of_year": d.format('DDD'),
              "day_of_month": d.format('D'),
              "day_of_week": {
                "number": d.format('d'),
                "name": d.format('dddd'),
              }
            },
            "time_slice": time2slice(d),
            "event": {
              "created": endMoment.format(),
              "dataset": "oura.activity",
              "duration": duration * 1E9,
              "ingested": new Date(),
              "kind": "state",
              "module": "oura",
              "original": activityClassData[j],
              "sequence": j,
              "start": d.format(),
              "end": d.add(duration, 'seconds').format(),
            },
            "activity": {
              "class": {
                "id": activityClassData[j],
                "name": activityClasses[activityClassData[j]]
              }
            }
          };
          bulk.push({index: {_index: c8.type(activityClassIndex)._index, _id: d.format()}});
          bulk.push(data);
        }
      }
      if (bulk.length > 0) {
        // console.log(bulk.length);
        // console.log(JSON.stringify(bulk, null, 1));
        bulkResponses.push(c8.bulk(bulk));
      }
    }
    try {
      let responses = await Promise.all(bulkResponses);
      let totalDocuments = 0;
      let totalTime = 0;
      responses.forEach((response) => {
        let result = c8.trimBulkResults(response);
        totalDocuments += result.items.length;
        totalTime += result.took;
      });
      if (totalDocuments == 0) {
        fulfill('No activity to import');
      }
      else {
        fulfill('Indexed ' + totalDocuments + ' activity documents in ' + totalTime + ' ms.');
      }
    }
    catch (error) {
      reject(error);
    }
  });
}

function getReadiness(c8, client, start, end) {
  return new Promise(async function (fulfill, reject){
    let response = await client.readiness(start, end);
    let bulk = [];
    let obj = response.readiness;
    for (var i=0; i<obj.length; i++) {
      let readiness = obj[i];
      let startMoment = moment(readiness.summary_date);
      let endMoment = moment(readiness.summary_date).add(1, 'days');
      let data = {
        "@timestamp": moment(readiness.summary_date).hour(4).add(1, 'days'),
        "ecs": {
          "version": "1.6.0"
        },
        "event": {
          "created": endMoment.format(),
          "dataset": "oura.readiness",
          "ingested": new Date(),
          "kind": "event",
          "module": "oura",
          "original": JSON.stringify(readiness),
          "sequence": readiness.period_id,
          "start": startMoment,
          "end": endMoment,
        },
        "date_details": {
          "year": startMoment.format('YYYY'),
          "month": {
            "number": startMoment.format('M'),
            "name": startMoment.format('MMMM'),
          },
          "week_number": startMoment.format('W'),
          "day_of_year": startMoment.format('DDD'),
          "day_of_month": startMoment.format('D'),
          "day_of_week": {
            "number": startMoment.format('d'),
            "name": startMoment.format('dddd'),
          }
        },
        "readiness": {
          "summary_date": readiness.summary_date,
          "period_id": readiness.period_id,
          "score": {
            "value": readiness.score,
            "previous_night": readiness.score_previous_night,
            "sleep_balance": readiness.score_sleep_balance,
            "previous_day": readiness.score_previous_day,
            "activity_balance": readiness.score_activity_balance,
            "resting_hr": readiness.score_resting_hr,
            "recovery_index": readiness.score_recovery_index,
            "temperature": readiness.score_temperature,
          },
        },
      };
      let id = readiness.summary_date + '.' + readiness.period_id;
      bulk.push({index: {_index: c8.type(readinessSummaryIndex)._index, _id: id}});
      bulk.push(data);
      console.log(id + ' (' + readiness.period_id + '): readiness score ' + readiness.score);
    }
    if (bulk.length > 0) {
      try {
        let response = await c8.bulk(bulk);
        let result = c8.trimBulkResults(response);
        fulfill('Indexed ' + result.items.length + ' readiness documents in ' + result.took + ' ms.');
      }
      catch (error) {
        reject(error);
      }
    }
    else {
      fulfill('No readiness data to import');
    }
  });
}

function mins2ts(mins) {
  let hours = Math.abs(Math.round(mins/60));
  let minutes = Math.abs(mins%60);
  // console.log(mins + ' minutes is ' + hours + ' hours and ' + minutes + ' minutes.');
  let str = (mins < 0 ? '-' : '+') + (hours < 10 ? '0' : '') + hours + ':' + (minutes < 10 ? '0' : '') + minutes
  // console.log(str);
  return str;
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
