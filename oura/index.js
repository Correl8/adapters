const oura = require('oura'),
  moment = require('moment'),
  express = require('express'),
  url = require('url');

const { v4: uuidv4 } = require('uuid');

// proxy forward redirectUri to http://localhost:{authPort}
const redirectUri = 'https://correl8.me/authcallback';
const authPort = 4343;

const MAX_DAYS = 1;
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
const heartrateIndex = 'oura-hr';
const sessionIndex = 'oura-session';
const sessionHRIndex = 'oura-session-hr';
const sessionHRVIndex = 'oura-session-hrv';
const sessionMotionIndex = 'oura-session-motion';
const tagIndex = 'oura-tag';
const workoutIndex = 'oura-workout';

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
  {
    name: heartrateIndex,
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
      "hr": {
        "bpm": "float",
        "source": "keyword"
      }
    }
  },
  {
    name: sessionIndex,
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
      "session": {
        "day": "date",
        "start_datetime": "date",
        "end_datetime": "date",
        "type": "keyword",
        "mood": "keyword",
      }
    }
  },
  {
    name: sessionHRIndex,
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
      "session": {
        "hr": "float",
      }
    }
  },
  {
    name: sessionHRVIndex,
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
      "session": {
        "rmssd": "float",
      }
    }
  },
  {
    name: sessionMotionIndex,
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
      "session": {
        "motion_count": "float",
      }
    }
  },
  {
    name: tagIndex,
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
      "tag": {
        "day": "date",
        "text": "text",
        "timestamp": "date",
        "tags": "keyword"
      }
    }
  },
  {
    name: workoutIndex,
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
      "workout": {
        "activity": "keyword",
        "calories": "float",
        "day": "date",
        "distance": "float",
        "intensity": "keyword",
        "label": "keyword",
        "source": "keyword",
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

adapter.importData = async function(c8, conf, opts) {
  try {
    let response = await c8.type(sleepSummaryIndex).search({
      _source: ['@timestamp'],
      size: 1,
      sort: [{'@timestamp': 'desc'}],
    });
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
    return importData(c8, conf, firstDate, lastDate);
  }
  catch (error) {
    throw new Error(error.name + ': ' + error.message);
  }
}

async function importData(c8, conf, firstDate, lastDate) {
  try {
    const options = {
      clientId: conf.client_id,
      clientSecret: conf.client_secret,
      redirectUri: conf.redirect_uri
    };
    const token = conf.access_token;
    const authClient = oura.Auth(options);
    const auth = authClient.createToken(conf.access_token, conf.refresh_token);
    let refreshed = await auth.refresh();
    Object.assign(conf, refreshed.data);
    await c8.config(conf);
    const client = new oura.Client(conf.access_token);
    if (!firstDate) {
      return;
    }
    let start = moment(firstDate);
    let end = moment(lastDate);
    const values = await Promise.all([
      getSleep(c8, client, start.format(dateFormat), end.format(dateFormat)),
      getActivity(c8, client, start.format(dateFormat), end.format(dateFormat)),
      getReadiness(c8, client, start.format(dateFormat), end.format(dateFormat)),
      getHR(c8, client, start.format(), end.format()), // dateTime instead of date
      getSessions(c8, client, start.format(dateFormat), end.format(dateFormat)),
      getTags(c8, client, start.format(dateFormat), end.format(dateFormat)),
      getWorkouts(c8, client, start.format(dateFormat), end.format(dateFormat)),
    ]);
    return values.join('\n');
  }
  catch (error){
    throw new Error(error.name + ': ' + error.message);
  }
}

async function getSleep(c8, client, start, end) {
  try {
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
          let id = d.format();
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
              "timezone": d.format('Z'),
            },
            "sleep": {
              "hr": sleepHRData[j]
            }
          };
          bulk.push({index: {_index: c8.type(sleepHRIndex)._index, _id: id}});
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
          let id = d.format();
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
              "timezone": d.format('Z'),
            },
            "sleep": {
              "hrv": {
                "rmssd": sleepHRVData[j]
              }
            }
          };
          bulk.push({index: {_index: c8.type(sleepHRVIndex)._index, _id: id}});
          bulk.push(data);
        }
      }
      if (sleepStateData && sleepStateData.length) {
        let d = moment(obj[i].bedtime_start);
        let duration = 5 * 60; // seconds
        for (var j=0; j<sleepStateData.length; j++) {
          let id = d.format();
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
              "timezone": d.format('Z'),
            },
            "sleep": {
              "state": {
                "id": sleepStateData[j],
                "name": sleepStates[sleepStateData[j]]
              }
            }
          };
          bulk.push({index: {_index: c8.type(sleepStateIndex)._index, _id: id}});
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
          return messages.length + ' errors in bulk insert:\n ' + messages.join('\n ');
        }
        else if (!result) {
          return 'Failed to index sleep data!';
        }
        return 'Indexed ' + result.items.length + ' sleep documents in ' + result.took + ' ms.';
      }
      catch (error) {
        bulk = null;
        throw new Error('Failed to index sleep data! ' + error.name + ': ' + error.message);
      }
    }
    else {
      return 'No sleep data to import';
    }
  }
  catch (error) {
    bulk = null;
    throw new Error('Failed to get sleep data! ' + error.name + ': ' + error.message);
  }
}

async function getActivity(c8, client, start, end) {
  try {
    let response = await client.activity(start, end);
    let obj = response.data;
    let bulkResponses = [];
    for (var i=0; i<obj.length; i++) {
      let bulk = [];
      let activity = obj[i];
      let startMoment = moment(activity.timestamp);
      let endMoment = moment(startMoment).add(1, 'days').subtract(1, 'seconds');
      let data = {
        "@timestamp": endMoment,
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
          "start": startMoment.format(),
          "timezone": startMoment.format('Z')
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
          "summary_date": activity.day,
          "period_id": i,
          // "timezone": activity.timestamp,
          "day_start": startMoment.format(),
          "day_end": endMoment.format(),
          "score": {
            "value": activity.score,
            "stay_active": activity.contributors.stay_active,
            "move_every_hour": activity.contributors.move_every_hour,
            "meet_daily_targets": activity.contributors.meet_daily_targets,
            "training_frequency": activity.contributors.training_frequency,
            "training_volume": activity.contributors.training_volume,
            "recovery_time": activity.contributors.recovery_time,
          },
        },
        "daily_movement_meters": activity.equivalent_walking_distance,
        "non_wear_minutes": Math.floor(activity.non_wear_time/60),
        "rest_minutes": Math.floor(activity.resting_time/60),
        "inactive_minutes": Math.floor(activity.sedentary_time/60),
        "inactivity_alerts": activity.inactivity_alerts,
        "low_minutes": activity.low_activity_met_minutes,
        "medium_minutes": activity.medium_activity_met_minutes,
        "high_minutes": activity.high_activity_met_minutes,
        "steps": activity.steps,
        "cal": {
          "total": activity.total_calories,
          "cal_active": activity.active_calories,
        },
        "met": {
          "min_inactive": activity.sedentary_met_minutes,
          "min_low": activity.low_activity_met_minutes,
          // "min_medium_plus": ,
          "min_medium": activity.medium_activity_met_minutes,
          "min_high": activity.high_activity_met_minutes,
          "average": activity.average_met_minutes,
        }
      }
      let activityClassData = activity.class_5_min.split("");
      let activityMETData = activity.met.items;
      let id = activity.day;
      // let id = endMoment.format(dateFormat);
      console.log(id + ': activity score ' + activity.score);
      bulk.push({index: {_index: c8.type(activitySummaryIndex)._index, _id: id}});
      bulk.push(data);
      if (activityMETData && activityMETData.length) {
        let d = moment(activity.met.timestamp);
        let duration = activity.met.interval; // seconds
        for (var j=0; j<activityMETData.length; j++) {
          let id = d.format();
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
              "timezone": d.format('Z'),
            },
            "activity": {
              "met": {
                "average": activityMETData[j]
              }
            }
          };
          bulk.push({index: {_index: c8.type(activityMETIndex)._index, _id: id}});
          bulk.push(data);
        }
      }
      if (activityClassData && activityClassData.length) {
        let d = moment(activity.timestamp);
        let duration = 5 * 60; // seconds
        for (var j=0; j<activityClassData.length; j++) {
          let id = d.format();
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
              "timezone": d.format('Z'),
            },
            "activity": {
              "class": {
                "id": activityClassData[j],
                "name": activityClasses[activityClassData[j]]
              }
            }
          };
          bulk.push({index: {_index: c8.type(activityClassIndex)._index, _id: id}});
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
        return 'No activity to import';
      }
      else {
        return 'Indexed ' + totalDocuments + ' activity documents in ' + totalTime + ' ms.';
      }
    }
    catch (error) {
      throw new Error('Failed to index activity data! ' + error.name + ': ' + error.message);
    }
  }
  catch (error) {
    throw new Error('Failed to get activity data! ' + error.name + ': ' + error.message);
  }
}

async function getReadiness(c8, client, start, end) {
  try {
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
        "event": {
          "created": endMoment.format(),
          "dataset": "oura.readiness",
          "duration": endMoment.diff(startMoment) * 1E6, // moment diff is milliseconds, want nanos
          "ingested": new Date(),
          "kind": "event",
          "module": "oura",
          "original": JSON.stringify(readiness),
          "sequence": readiness.period_id,
          "start": startMoment,
          "end": endMoment,
          "timezone": startMoment.format('Z'),
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
        return 'Indexed ' + result.items.length + ' readiness documents in ' + result.took + ' ms.';
      }
      catch (error) {
        throw new Error('Failed to index readiness data! ' + error.name + ': ' + error.message);
      }
    }
    else {
      return 'No readiness data to import';
    }
  }
  catch (error) {
      throw new Error('Failed to get readiness data! ' + error.name + ': ' + error.message);
  }
}

async function getHR(c8, client, start, end) {
  try {
    let response = await client.heartrate(start, end);
    let obj = response.data;
    let bulk = [];
    for (var i=0; i<obj.length; i++) {
      let hr = obj[i];
      let startMoment = moment(hr.timestamp);
      let data = {
        "@timestamp": hr.timestamp,
        "ecs": {
          "version": "1.6.0"
        },
        "event": {
          "created": startMoment.format(),
          "dataset": "oura.heartrate",
          "ingested": new Date(),
          "kind": "metric",
          "module": "oura",
          "original": JSON.stringify(hr),
          "start": startMoment.format(),
          "timezone": startMoment.format('Z'),
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
        "hr": {
          "bpm": hr.bpm,
          "source": hr.source,
        }
      }
      let id = hr.timestamp;
      // console.log(id + ': HR ' + hr.bpm);
      bulk.push({index: {_index: c8.type(heartrateIndex)._index, _id: id}});
      bulk.push(data);
    }
    let totalDocuments = 0;
    let totalTime = 0;
    if (bulk.length > 0) {
      try {
        // console.log(bulk.length);
        // console.log(JSON.stringify(bulk, null, 1));
        const response = await c8.bulk(bulk);
        let result = c8.trimBulkResults(response);
        totalDocuments += result.items.length;
        totalTime += result.took;
      }
      catch (error) {
        throw new Error('Failed to index heartrate data! ' + error.name + ': ' + error.message);
      }
    }
    if (totalDocuments == 0) {
      return 'No heartrate to import';
    }
    else {
      return 'Indexed ' + totalDocuments + ' heartrate documents in ' + totalTime + ' ms.';
    }
  }
  catch (error) {
    throw new Error('Failed to get heartrate data! ' + error.name + ': ' + error.message);
  }
}

async function getSessions(c8, client, start, end) {
  try {
    let response = await client.session(start, end);
    let obj = response.data;
    let bulk = [];
    for (var i=0; i<obj.length; i++) {
      let session = obj[i];
      let startMoment = moment(session.start_datetime);
      let endMoment = moment(session.end_datetime);
      let data = {
        "@timestamp": session.start_datetime,
        "ecs": {
          "version": "1.6.0"
        },
        "event": {
          "created": startMoment.format(),
          "dataset": "oura.session",
          "duration": endMoment.diff(startMoment) * 1E6, // moment diff is milliseconds, want nanos
          "ingested": new Date(),
          "kind": "event",
          "module": "oura",
          "original": JSON.stringify(session),
          "sequence": readiness.period_id,
          "start": startMoment,
          "end": endMoment,
          "timezone": startMoment.format('Z'),
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
        "session": {
          "day": session.day,
          "start_datetime": session.start_datetime,
          "end_datetime": session.end_datetime,
          "type": session.type,
          "mood": session.mood,
        }
      }
      let id = session.start_datetime;
      bulk.push({index: {_index: c8.type(sessionIndex)._index, _id: id}});
      bulk.push(data);
      if (session.heart_rate) {
        let d = moment(session.heart_rate.timestamp);
        const duration = session.heart_rate.interval; // seconds
        for (var shr of session.heart_rate.items) {
          let id = d.format();
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
              "created": d.format(),
              "dataset": "oura.session",
              "duration": duration * 1E9,
              "ingested": new Date(),
              "kind": "metric",
              "module": "oura",
              "original": JSON.stringify(shr),
              "start": d,
              "timezone": d.format('Z'),
              "end": d.add(duration, 'seconds').format(),
            },
            "session": {
              "hr": shr
            }
          }
          bulk.push({index: {_index: c8.type(sessionHRIndex)._index, _id: id}});
          bulk.push(data);
        }
      }
      if (session.heart_rate_variability) {
        let d = moment(session.heart_rate_variability.timestamp);
        const duration = session.heart_rate_variability.interval; // seconds
        for (var shrv of session.heart_rate_variability.items) {
          let id = d.format();
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
              "created": d.format(),
              "dataset": "oura.session",
              "duration": duration * 1E9,
              "ingested": new Date(),
              "kind": "metric",
              "module": "oura",
              "original": JSON.stringify(shrv),
              "start": d,
              "timezone": d.format('Z'),
              "end": d.add(duration, 'seconds').format(),
            },
            "session": {
              "rmssd": shrv
            }
          }
          bulk.push({index: {_index: c8.type(sessionHRVIndex)._index, _id: id}});
          bulk.push(data);
        }
      }
      if (session.motion_count) {
        let d = moment(session.motion_count.timestamp);
        const duration = session.motion_count.interval; // seconds
        for (var smc of session.motion_count.items) {
          let id = d.format();
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
              "created": d.format(),
              "dataset": "oura.session",
              "duration": duration * 1E9,
              "ingested": new Date(),
              "kind": "metric",
              "module": "oura",
              "original": JSON.stringify(smc),
              "start": d,
              "timezone": d.format('Z'),
              "end": d.add(duration, 'seconds').format(),
            },
            "session": {
              "motion_count": smc
            }
          }
          bulk.push({index: {_index: c8.type(sessionMotionIndex)._index, _id: id}});
          bulk.push(data);
        }
      }
    }
    let totalDocuments = 0;
    let totalTime = 0;
    if (bulk.length > 0) {
      try {
        // console.log(bulk.length);
        // console.log(JSON.stringify(bulk, null, 1));
        const response = await c8.bulk(bulk);
        let result = c8.trimBulkResults(response);
        totalDocuments += result.items.length;
        totalTime += result.took;
      }
      catch (error) {
        throw new Error('Failed to index sessions. ' + error.name + ': ' + error.message);
      }
    }
    if (totalDocuments == 0) {
      return 'No sessions to import';
    }
    else {
      return 'Indexed ' + totalDocuments + ' session documents in ' + totalTime + ' ms.';
    }
  }
  catch (error) {
    throw new Error('Failed to get sessions. ' + error.name + ': ' + error.message);
  }
}

async function getTags(c8, client, start, end) {
  try {
    let response = await client.tag(start, end);
    let obj = response.data;
    let bulk = [];
    for (var i=0; i<obj.length; i++) {
      let tag = obj[i];
      let startMoment = moment(tag.timestamp);
      let data = {
        "@timestamp": tag.timestamp,
        "ecs": {
          "version": "1.6.0"
        },
        "event": {
          "created": startMoment.format(),
          "dataset": "oura.tag",
          "ingested": new Date(),
          "kind": "event",
          "module": "oura",
          "original": JSON.stringify(tag),
          "start": startMoment,
          "timezone": startMoment.format('Z'),
        },
        "time_slice": time2slice(startMoment),
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
        "tag": tag
      }
      let id = tag.timestamp;
      bulk.push({index: {_index: c8.type(tagIndex)._index, _id: id}});
      bulk.push(data);
    }
    let totalDocuments = 0;
    let totalTime = 0;
    if (bulk.length > 0) {
      try {
        // console.log(bulk.length);
        // console.log(JSON.stringify(bulk, null, 1));
        const response = await c8.bulk(bulk);
        let result = c8.trimBulkResults(response);
        totalDocuments += result.items.length;
        totalTime += result.took;
      }
      catch (error) {
        throw new Error('Failed to index tags. ' + error.name + ': ' + error.message);
      }
    }
    if (totalDocuments == 0) {
      return 'No tags to import';
    }
    else {
      return 'Indexed ' + totalDocuments + ' tag documents in ' + totalTime + ' ms.';
    }
  }
  catch (error) {
    throw new Error('Failed to get tags. ' + error.name + ': ' + error.message);
  }
}

async function getWorkouts(c8, client, start, end) {
  try {
    let response = await client.workout(start, end);
    let obj = response.data;
    let bulk = [];
    for (var i=0; i<obj.length; i++) {
      let workout = obj[i];
      let startMoment = moment(workout.start_datetime);
      let endMoment = moment(workout.end_datetime);
      delete workout.start_datetime;
      delete workout.end_datetime;
      let data = {
        "@timestamp": startMoment,
        "ecs": {
          "version": "1.6.0"
        },
        "event": {
          "created": startMoment.format(),
          "dataset": "oura.workout",
          "duration": endMoment.diff(startMoment) * 1E6, // moment diff is milliseconds, want nanos
          "ingested": new Date(),
          "kind": "event",
          "module": "oura",
          "original": JSON.stringify(workout),
          "start": startMoment,
          "end": endMoment,
          "timezone": startMoment.format('Z'),
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
        "workout": workout,
      }
      let id = startMoment.format();
      bulk.push({index: {_index: c8.type(workoutIndex)._index, _id: id}});
      bulk.push(data);
    }
    let totalDocuments = 0;
    let totalTime = 0;
    if (bulk.length > 0) {
      try {
        // console.log(bulk.length);
        // console.log(JSON.stringify(bulk, null, 1));
        const response = await c8.bulk(bulk);
        let result = c8.trimBulkResults(response);
        totalDocuments += result.items.length;
        totalTime += result.took;
      }
      catch (error) {
        throw new Error('Failed to index workouts. ' + error.name + ': ' + error.message);
      }
    }
    if (totalDocuments == 0) {
      return 'No workouts to import';
    }
    else {
      return 'Indexed ' + totalDocuments + ' workout documents in ' + totalTime + ' ms.';
    }
  }
  catch (error) {
    throw new Error('Failed to get workouts. ' + error.name + ': ' + error.message);
  }
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
  let hour = t.format('H');
  let minute = (5 * Math.floor(t.format('m') / 5 )) % 60;
  let idTime = parseInt(hour) + parseInt(minute)/60;
  let time_slice = {
    id: Math.round((idTime + (idTime >= 4 ? -4 : 20)) * 12),
    name: [hour, minute].join(':'),
    start_hour: parseInt(hour)
  };
  if (minute < 10) {
    time_slice.name = [hour, '0' + minute].join(':');
  }
  return time_slice;
}

module.exports = adapter;
