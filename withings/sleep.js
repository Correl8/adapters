const // withings = require('withings'),
moment = require('moment'),
express = require('express'),
url = require('url'),
rp = require('request-promise');
const { v4: uuidv4 } = require('uuid/');

const MAX_DAYS = 7;
const MS_IN_DAY = 24 * 60 * 60 * 1000;
const FIFE_MINUTES_IN_NANOS = 5 * 60 * 1000 * 1E6;

const states = ['awake', 'light', 'deep', 'REM'];

// proxy forward redirectUri to http://localhost:{authPort}
const redirectUri = 'https://correl8.me/authcallback';
const authPort = 4343;

const baseUrl = 'https://account.withings.com';
const dateFormat = 'X';
const ymdFormat = 'YYYY-MM-DD';

let adapter = {};

adapter.sensorName = 'withings';
let sleepIndex = adapter.sensorName + '-sleep';
let sleepSummaryIndex = adapter.sensorName + '-sleep-summary';

adapter.types = [
  {
    // emtpy "type" to share config with body adapter
    name: adapter.sensorName,
    fields: {}
  },
  {
    name: sleepIndex,
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
        "ingested": "date",
        "kind": "keyword",
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
      "withings": {
        "startdate": "date",
        "enddate": "date",
        "modified": "date",
        "state": "long",
        "hr": "float",
        "rr": "float",
        "snoring": "float",
        "duration": "long",
        "model": "keyword"
      },
      "sleep": {
        "period_id": "long",
        "state": {
          "id": "integer",
          "name": "keyword",
        },
        "hr": "float",
        "breath": "float",
        "snoring": "float",
      }
    }
  },
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
        "ingested": "date",
        "kind": "keyword",
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
      "withings": {
        "date": "date",
        "startdate": "date",
        "enddate": "date",
        "modified": "date",
        "model": "keyword",
        "timezone": "keyword",
        "totalsleepduration": "float",
        "data": {
          "breathing_disturbances_intensity": "float",
          "wakeupduration": "float",
          "lightsleepduration": "float",
          "deepsleepduration": "float",
          "remsleepduration": "float",
          "durationtosleep": "float",
          "durationtowakeup": "float",
          "wakeupcount": "float",
          "hr_average": "float",
          "hr_max": "float",
          "hr_min": "float",
          "rr_average": "float",
          "rr_max": "float",
          "rr_min": "float",
          "sleep_score": "float",
          "snoring": "float",
          "snoringepisodecount": "float",
          // "night_events": "float",
          "out_of_bed_count": "float",
          "nb_rem_episodes": "float",
          "sleep_efficiency": "float",
          "sleep_latency": "float",
          "total_sleep_time": "float",
          "total_timeinbed": "float",
          "wakeup_latency": "float",
          "waso": "float",
        }
      },
      "sleep": {
        "summary_date": "date",
        "period_id": "long",
        "timezone": "keyword",
        "bedtime_start": "date",
        "bedtime_end": "date",
        "score": {
          "value": "float",
        },
        "duration_seconds": "long",
        "total_seconds": "long",
        "awake_seconds": "long",
        "light_seconds": "long",
        "rem_seconds": "long",
        "deep_seconds": "long",
        "onset_latency_seconds": "long",
        "hr_lowest": "float",
        "hr_average": "float",
        "breath_average": "float",
        "breath_lowest": "float",
        "wake_up_count": "long",
      }, 
    }
  }
];

adapter.promptProps = {
  properties: {
    clientId: {
      description: 'Enter your Withings client ID'.magenta
    },
    clientSecret: {
      description: 'Enter your Withings client secret'.magenta
    },
    redirectUri: {
      description: 'Enter your redirect URL (authentiation callback)'.magenta,
      default: redirectUri
    },
    authPort: {
      description: 'Enter the port number for authentication callback service'.magenta,
      default: authPort
    },
  }
};

adapter.storeConfig = async (c8, gotConfig) => {
  var conf = gotConfig;
  conf.url = url.parse(baseUrl);
  // const configPromise = c8.config(conf);
  // await configPromise;
  const defaultUrl = url.parse(conf.redirectUri);
  const express = require('express');
  const app = express();
  const port = parseInt(conf.authPort);
  const scopes = [
    'user.info',
    'user.metrics',
    'user.activity'
  ];
  const state = uuidv4();
  
  const step1 = {
    protocol: conf.url.protocol,
    host: conf.url.host,
    pathname: '/oauth2_user/authorize2',
    query: {
      response_type: 'code',
      client_id: conf.clientId,
      redirect_uri: conf.redirectUri,
      scope: scopes.join(','),
      state: state
    }
  }
  var server = app.listen(port, () => {
    console.log("Please, go to \n" + url.format(step1))
  });
  
  app.get(defaultUrl.pathname, async (req, res) => {
    // console.log(url.parse(req.originalUrl));
    if (req.query && req.query.code) {
      if (req.query.state == state) {
        conf.code = req.query.code;
        console.log('Got Authorization Code, requesting Access Token');
        const step2 = {
          method: 'POST',
          uri: baseUrl + '/oauth2/token',
          form: {
            grant_type: 'authorization_code',
            client_id: conf.clientId,
            client_secret: conf.clientSecret,
            code: conf.code,
            redirect_uri: conf.redirectUri
          }
        };
        const tokenBody = await rp(step2);
        Object.assign(conf, JSON.parse(tokenBody));
        server.close();
        await c8.config(conf);
        res.send('Access token saved.');
        console.log('Configuration stored.');
        c8.release();
        process.exit();
      }
      else {
        res.send('Authentication error: invalid state!');
        console.warn('Invalid state. ' + req.query.state + ' does not match ' + state);
      }
    }
  });
};

adapter.importData = (c8, conf, opts) => {
  return new Promise(async (fulfill, reject) => {
    try {
      console.log('Getting first date...');
      const response = await c8.index(sleepSummaryIndex).search({
        // _source: ['withings.modified'],
        // sort: [{'withings.modified': 'desc'}],
        _source: ['withings.startdate'],
        sort: [{'withings.startdate': 'desc'}],
        size: 1,
      });
      const resp = c8.trimResults(response);
      let firstDate = new Date();
      let lastDate = opts.lastDate || new Date();
      firstDate.setTime(lastDate.getTime() - (MAX_DAYS * MS_IN_DAY));
      if (opts.firstDate) {
        firstDate = new Date(opts.firstDate);
        console.log('Setting first time to ' + firstDate);
      }
      // else if (resp && resp.withings && resp.withings.modified) {
      else if (resp && resp.withings && resp.withings.startdate) {
        // firstDate = new Date(resp.withings.modified);
        firstDate = new Date(resp.withings.startdate);
        console.log('Setting first time to ' + firstDate);
      }
      else {
        console.log("No first time set!");
      }
      if (lastDate.getTime() > (firstDate.getTime() + MAX_DAYS * MS_IN_DAY)) {
        lastDate.setTime(firstDate.getTime() + MAX_DAYS * MS_IN_DAY);
        console.warn('Setting last date to ' + lastDate);
      }
      const step3 = {
        method: 'POST',
        uri: baseUrl + '/oauth2/token',
        form: {
          grant_type: 'refresh_token',
          client_id: conf.clientId,
          client_secret: conf.clientSecret,
          refresh_token: conf.refresh_token
        }
      };
      const tokenBody = await rp(step3);
      Object.assign(conf, JSON.parse(tokenBody));
      await c8.config(conf);
      let messages = [];
      messages.push(await importSleepSummary(c8, conf, firstDate, lastDate));
      await c8.index(sleepIndex);
      messages.push(await importSleep(c8, conf, firstDate, lastDate));
      fulfill(messages.length + ' dataset' + (messages.length == 1 ? '' : 's') + '.\n' + messages.join('\n'));
    }
    catch(e) {
      reject(new Error(e));
    }
  });
};

function importSleepSummary(c8, conf, firstDate, lastDate) {
  return new Promise(async (fulfill, reject) => {
    try {
      const step4 = {
        method: 'GET',
        uri: 'https://wbsapi.withings.net/v2/sleep',
        form: {
          action: 'getsummary',
          data_fields: Object.keys(adapter.types[2].fields.withings.data).join(',')
        },
        headers: {
          Authorization: 'Bearer ' + conf.access_token
        }
      };
      if (lastDate) {
        step4.form.enddateymd = moment(lastDate).format(ymdFormat);
        if (firstDate) {
          step4.form.startdateymd = moment(firstDate).format(ymdFormat);
        }
      }
      else if (firstDate) {
        step4.form.lastupdate = moment(firstDate).format(dateFormat);
      }
      const body = await rp(step4);
      let obj = JSON.parse(body);
      // console.log(obj);
      let bulk = [];
      if (!obj.body) {
        reject(new Error(JSON.stringify(obj)));
        return false;
      }
      if (!obj.body.series) {
        fulfill('No sleep summaries to import');
        return false;
      }
      obj.body.series.forEach((s) => {
        // console.log(s);
        let d = moment(s.date);
        let id = s.id;
        s.startdate = 1000 * s.startdate;
        s.enddate = 1000 * s.enddate;
        let modTime = moment();
        if (s.modified) {
          s.modified = 1000 * s.modified;
          modTime = moment(s.modified);
        }
        const startTime = moment(s.startdate);
        const endTime =  moment(s.enddate);
        const duration_seconds = Math.round(endTime.diff(startTime) / 1E3);
        let data = {
          "@timestamp": d.format(),
          "ecs": {
            "version": "1.6.0"
          },
          "event": {
            "created": modTime.format(),
            "dataset": "withings.sleep.summary",
            "ingested": new Date(),
            "kind": "event",
            "module": "withings",
            "original": JSON.stringify(s),
            "start": startTime.format(),
            "end": endTime.format(),
            "duration": duration_seconds * 1E9
          },
          "date_details": {
            "year": startTime.format('YYYY'),
            "month": {
              "number": startTime.format('M'),
              "name": startTime.format('MMMM'),
            },
            "week_number": startTime.format('W'),
            "day_of_year": startTime.format('DDD'),
            "day_of_month": startTime.format('D'),
            "day_of_week": {
              "number": startTime.format('d'),
              "name": startTime.format('dddd'),
            }
          },
          "withings": s,
          "sleep": {
            "summary_date": d.format(),
            "period_id": s.id,
            "timezone": s.timezone,
            "bedtime_start": startTime.format(),
            "bedtime_end": endTime.format(),
            "score": {
              "value": s.data.sleep_score,
            },
            "duration_seconds": duration_seconds,
            "total_seconds": s.data.total_sleep_time,
            "awake_seconds": s.data.sleep_latency + s.data.waso + s.data.wakeup_latency,
            "light_seconds": s.data.lightsleepduration,
            "rem_seconds": s.data.remsleepduration,
            "deep_seconds": s.data.deepsleepduration,
            "onset_latency_seconds": s.data.sleep_latency,
            "hr_lowest": s.data.hr_min,
            "hr_average": s.data.hr_average,
            "breath_average": s.data.rr_average,
            "breath_lowest": s.data.rr_min,
            "wake_up_count": s.data.wakeupcount,
          }, 
        };
        // console.log(data);
        bulk.push({index: {_index: c8._index, _id: id}});
        bulk.push(data);
        console.log('Sleep summary: ' + d.format());
      });
      if (bulk.length > 0) {
        // console.log(JSON.stringify(bulk, null, 1));
        // return;
        const response = await c8.bulk(bulk);
        const result = c8.trimBulkResults(response);
        if (result.errors) {
          var messages = [];
          for (var i=0; i<result.items.length; i++) {
            if (result.items[i].index.error) {
              messages.push(i + ': ' + result.items[i].index.error.reason);
            }
          }
          reject(new Error(messages.length + ' errors in bulk insert:\n ' + messages.join('\n ')));
        }
        fulfill('Indexed ' + result.items.length + ' sleep summar' + (result.items.length == 1 ? 'y' : 'ies') + ' in ' + result.took + ' ms.');
      }
      else {
        fulfill('No sleep summaries to import');
      }
    }
    catch (e) {
      reject(new Error(e));
    }
  });
}

function importSleep(c8, conf, firstDate, lastDate) {
  return new Promise(async (fulfill, reject) => {
    try {      
      const step4 = {
        method: 'GET',
        uri: 'https://wbsapi.withings.net/v2/sleep',
        form: {
          action: 'get',
          data_fields: 'hr,rr,snoring'
        },
        headers: {
          Authorization: 'Bearer ' + conf.access_token
        }
      };
      if (firstDate) {
        step4.form.startdate = moment(firstDate).format(dateFormat);
      }
      if (lastDate) {
        step4.form.enddate = moment(lastDate).format(dateFormat);
      }
      const body = await rp(step4);
      let obj = JSON.parse(body);
      // console.log(JSON.stringify(obj.body, null, 1));
      let bulk = [];
      if (!obj.body) {
        reject(new Error(JSON.stringify(obj)));
        return false;
      }
      if (!obj.body.series) {
        fulfill('No sleep periods to import');
        return false;
      }
      let eventCount = 0;
      let periodCount = 0;
      obj.body.series.forEach(s => {
        periodCount++;
        let orig = JSON.stringify(s);
        let modTime = moment();
        if (s.modified) {
          s.modified = 1000 * s.modified;
          modTime = moment(s.modified);
        }
        let hrCount = 0;
        if (s.hr) {
          for (const ts in s.hr) {
            let value = s.hr[ts];
            if (!value) {
              continue;
            }
            let item = {};
            let id = ts + '-hr';
            item[ts] = value;
            let t = moment(ts * 1000);
            let hrData = {
              "@timestamp": moment(ts * 1000).format(),
              "ecs": {
                "version": "1.0.1"
              },
              "event": {
                "created": modTime.format(),
                "dataset": "withings.sleep.hr",
                "module": "withings",
                "original": JSON.stringify(item),
                "start": t.format(),
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
                "hr": value
              }
            };
            bulk.push({index: {_index: c8._index, _id: id}});
            bulk.push(hrData);
            hrCount++;
          }
          delete s.hr;
          eventCount += hrCount;
        }
        let rrCount = 0;
        if (s.rr) {
          for (const ts in s.rr) {
            let value = s.rr[ts];
            if (!value) {
              continue;
            }
            let item = {};
            let id = ts + '-rr';
            item[ts] = value;
            let t = moment(ts * 1000);
            let rrData = {
              "@timestamp": moment(ts * 1000).format(),
              "ecs": {
                "version": "1.0.1"
              },
              "event": {
                "created": modTime.format(),
                "dataset": "withings.sleep.rr",
                "module": "withings",
                "original": JSON.stringify(item),
                "start": t.format(),
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
                "breath": value
              }
            };
            bulk.push({index: {_index: c8._index, _id: id}});
            bulk.push(rrData);
            rrCount++;
          }
          delete s.rr;
          eventCount += rrCount;
        }
        let snoringCount = 0;
        if (s.snoring) {
          for (const ts in s.snoring) {
            let value = s.snoring[ts];
            if (!value) {
              continue;
            }
            let item = {};
            let id = ts + '-snoring';
            item[ts] = value;
            let t = moment(ts * 1000);
            let snoringData = {
              "@timestamp": t.format(),
              "ecs": {
                "version": "1.0.1"
              },
              "event": {
                "created": modTime.format(),
                "dataset": "withings.sleep.snoring",
                "module": "withings",
                "original": JSON.stringify(item),
                "start": t.format(),
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
                "snoring": value
              }
            };
            bulk.push({index: {_index: c8._index, _id: id}});
            bulk.push(snoringData);
            snoringCount++;
          }
          delete s.snoring;
          eventCount += snoringCount;
        }
        let d = moment(s.startdate * 1000);
        const startTime = moment(s.startdate * 1000);
        const endTime =  moment(s.enddate * 1000);
        let t = startTime;
        let subPeriodCount = 0;
        while (t.valueOf() < endTime.valueOf()) {
          subPeriodCount++;
          let et = t.clone();
          et.add(5, 'minutes');
          if (subPeriodCount == 1) {
            et.minutes((5 * Math.floor(et.format('m') / 5 )) % 60);
          }
          if (endTime.isBefore(et)) {
            et = endTime;
          }
          let id = startTime.format() + '-' + subPeriodCount;
          let slice = time2slice(t);
          let clone = {};
          Object.assign(clone, s);
          clone.startdate = t.format();
          clone.enddate = et.format();
          clone.duration = moment.duration(et.diff(t)).as('s'); // in seconds
          // console.log(t.format() + ' - ' + et.format() + ' : ' + clone.duration + ' / ' + subPeriodCount);
          let data = {
            "@timestamp": t.format(),
            "ecs": {
              "version": "1.6.0"
            },
            "event": {
              "created": modTime.format(),
              "dataset": "withings.sleep.state",
              "ingested": new Date(),
              "kind": "state",
              "module": "withings",
              "original": orig,
              "start": t.format(),
              "end": et.format(),
              "duration": clone.duration * 1E9, // s to ns
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
            "time_slice": slice,
            "withings": clone,
            "sleep": {
              "period_id": subPeriodCount,
              "state": {
                "id": s.state,
                "name": states[s.state]
              }
            }
          };
          bulk.push({index: {_index: c8._index, _id: id}});
          bulk.push(data);
          t = et;
        }
          return;
      });
      if (bulk.length > 0) {
        // console.log(JSON.stringify(bulk, null, 1));
        const response = await c8.bulk(bulk);
        const result = c8.trimBulkResults(response);
        if (result.errors) {
          var messages = [];
          for (var i=0; i<result.items.length; i++) {
            if (result.items[i].index.error) {
              messages.push(i + ': ' + result.items[i].index.error.reason);
            }
          }
          reject(new Error(messages.length + ' errors in bulk insert:\n ' + messages.join('\n ')));
        }
        fulfill(`Found ${periodCount} sleep period${result.items.length == 1 ? '' : 's'} and ${eventCount} hr, hr and snoring events. Indexed ${result.items.length} records in ${result.took} ms.`);
      }
      else {
        fulfill('No sleep periods to import');
      }
    }
    catch (e) {
      reject(new Error(e));
    }
  });
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
