const fs = require('fs');
const {google} = require('googleapis');
const moment = require('moment');
const prompt = require('prompt');
const activityTypes = require('google-fit-activity-types');

const adapter = {};

const MS_IN_DAY = 24 * 60 * 60 * 1000;
const MAX_DAYS = 1;
const MAX_SESSION_DAYS = 180;
const MAX_EVENTS = 100;
const SCOPES = [
  'https://www.googleapis.com/auth/fitness.activity.read',
  'https://www.googleapis.com/auth/fitness.blood_glucose.read',
  'https://www.googleapis.com/auth/fitness.blood_pressure.read',
  'https://www.googleapis.com/auth/fitness.body.read',
  'https://www.googleapis.com/auth/fitness.body_temperature.read',
  'https://www.googleapis.com/auth/fitness.heart_rate.read',
  'https://www.googleapis.com/auth/fitness.location.read',
  'https://www.googleapis.com/auth/fitness.nutrition.read',
  'https://www.googleapis.com/auth/fitness.oxygen_saturation.read',
  'https://www.googleapis.com/auth/fitness.reproductive_health.read',
  'https://www.googleapis.com/auth/fitness.sleep.read'
];

adapter.sensorName = 'googlefit';

adapter.types = [
  {
    name: 'googlefit-dataset',
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
      "message": 'text',
      "fit": {
        "dataSource": {
          "dataStreamId": 'keyword',
          "name": 'keyword',
          "dataStreamName": 'keyword',
          "type": 'keyword',
          "dataType": {
            "name": 'keyword',
            "field": {
              "name": "keyword",
              "format": "keyword",
              "optional": "boolean",
            },
          },
          "device": {
            "uid": 'keyword',
            "type": 'keyword',
            "version": 'keyword',
            "model": 'keyword',
            "manufacturer": 'keyword',
          },
          "application": {
            "detailsUrl": 'keyword',
            "name": 'keyword',
            "packageName": 'keyword',
            "version": 'keyword'
          },
          "dataQualityStandard": 'keyword',
          'minStartTimeNs': 'long',
          'maxEndTimeNs': 'long',
        },
        "point": {
          'startTimeNanos': 'long',
          'endTimeNanos': 'long',
          'originDataSourceId': 'keyword',
          'modifiedTimeMillis': 'long',
          "rawTimestampNanos": 'long',
          "computationTimeMillis": 'long',
        },
        'accuracy': 'float',
        'activityId': 'integer',
        'activity': 'keyword',
        'altitude': 'float',
        'bpm': 'float',
        'calories': 'float',
        'confidence': 'float',
        'dataSourceId': 'keyword',
        'distance': 'float',
        'grams': 'float',
        'height': 'float',
        'IU': 'float',
        'latitude': 'float',
        'longitude': 'float',
        'manEndTimeNs': 'long',
        'rpm': 'float',
        'resistance': 'float',
        'speed': 'float',
        'steps': 'float',
        'timestamp': 'date',
        'watts': 'float',
        'weight': 'float'
      },
      'position': {
        "geo": {
          "location":  'geo_point',
        },
      },
    }
  },
  {
    name: 'googlefit-session',
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
      "message": 'text',
      "fit": {
        "application": {
          "detailsUrl": 'keyword',
          "name": 'keyword',
          "packageName": 'keyword',
          "version": 'keyword'
        },
        "activity": 'keyword',
        "activityId": 'integer',
        "session": {
          "id": 'keyword',
          "name": 'text',
          "description": 'text',
          "activityType": 'long',
          "endTimeMillis": 'long',
          "startTimeMillis": 'long',
          "modifiedTimeMillis": 'long',
        }
      },
      'position': {
        "geo": {
          "location":  'geo_point',
        },
      },
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

adapter.storeConfig = (c8, result) => {
  let conf = result;
  fs.readFile(result.authconfig, (err, content) => {
    if (err) {
      console.log('Error loading client secret file: ' + err);
      return;
    }
    Object.assign(conf, JSON.parse(content));
    // console.log(conf);
    const auth = google.auth;
    const clientSecret = conf.installed.client_secret;
    const clientId = conf.installed.client_id;
    const redirectUrl = conf.installed.redirect_uris[0];
    const oauth2Client = new auth.OAuth2(clientId, clientSecret, redirectUrl);
    const authUrl = oauth2Client.generateAuthUrl({
      access_type: 'offline',
      scope: SCOPES
    });
    console.log('Authorize this app by visiting this url\n', authUrl, '\n\n');
    prompt.start();
    prompt.message = '';
    let promptProps = {
      properties: {
        code: {
          description: 'Enter the code shown on page'.magenta
        },
      }
    }
    prompt.get(promptProps, (err, result) => {
      if (err) {
        console.trace(err);
      }
      else {
        oauth2Client.getToken(result.code, async (err, token) => {
          if (err) {
            console.log('Error while trying to retrieve access token', err);
            return;
          }
          conf.credentials = token;
          await c8.config(conf);
          console.log('Access credentials saved.');
          c8.release();
          process.exit;
        });
      }
    });
  });
}

adapter.importData = (c8, conf, opts) => {
  return new Promise(async (fulfill, reject) => {
    const clientSecret = conf.installed.client_secret;
    const clientId = conf.installed.client_id;
    let redirectUrl = conf.installed.redirect_uris[0];
    const auth = google.auth;
    opts.oauth2Client = new auth.OAuth2(clientId, clientSecret, redirectUrl);
    opts.oauth2Client.credentials = conf.credentials;
    const datasetResult = await importDatasets(c8, conf, opts);
    const sessionsResult = await importSessions(c8, conf, opts);
    fulfill(datasetResult + '\n' + sessionsResult);
  });
}

importDatasets = async (c8, conf, opts) => {
  let firstDate = new Date();
  firstDate.setTime(firstDate.getTime() - MS_IN_DAY);
  let lastDate = opts.lastDate || new Date();
  let response = await c8.type(adapter.types[0].name).search({
    _source: ['@timestamp'],
    size: 1,
    sort: [{'@timestamp': 'desc'}],
  });
  if (opts.firstDate) {
    firstDate = opts.firstDate;
    console.log('Setting first time to ' + firstDate);
  }
  else if (response && response.body && response.body.hits && response.body.hits.hits && response.body.hits.hits[0] && response.body.hits.hits[0]._source && response.body.hits.hits[0]._source['@timestamp']) {
    let d = new Date(response.body.hits.hits[0]._source['@timestamp']);
    // firstDate = new Date(d.getTime() + 1);
    firstDate = d;
    console.log('Setting first time to ' + firstDate);
  }
  else {
    firstDate = new Date(2014, 10, 14); // Google Fit Launch Date
    firstDate.setTime(firstDate.getTime() - MS_IN_DAY);
    console.warn('No previously indexed data, setting first time to ' + firstDate);
  }
  if (lastDate.getTime() >= (firstDate.getTime() + (MS_IN_DAY * MAX_DAYS))) {
    lastDate.setTime(firstDate.getTime() + (MS_IN_DAY * MAX_DAYS) - 1);
    console.warn('Max time range ' + MAX_DAYS + ' days, setting end time to ' + lastDate);
  }
  const fit = google.fitness('v1');
  const resp = await fit.users.dataSources.list({auth: opts.oauth2Client, userId: 'me'});
  // console.log(resp);
  for (var i=0; i<resp.data.dataSource.length; i++) {
    let ds = resp.data.dataSource[i];
    let device = ds.device;
    let app = ds.application;
    let dqs = ds.dataQualityStandard;
    let dType = ds.dataType;
    let datasetId = (firstDate.getTime() * 1000000).toString() + '-' +
      (((lastDate.getTime() + 1) * 1000000)-1).toString(); // don't miss a ns
    let params = {
      auth: opts.oauth2Client,
      userId: 'me',
      dataSourceId: ds.dataStreamId,
      datasetId: datasetId
    }
    // console.log(params);
    const subresp = await fit.users.dataSources.datasets.get(params);
    // console.log(subresp);
    if (subresp && subresp.data && subresp.data.point && subresp.data.point.length > 0) {
      let points = subresp.data.point;
      // console.log(points);
      // console.log(points.length + ' points of ' + shortName);
      let bulk = [];
      for (var j=0; j<points.length; j++) {
        let item = points[j];
        let st = parseInt(item.startTimeNanos);
        let et = parseInt(item.endTimeNanos);
        let id = [st, ds.dataStreamId, dType.name].join('-');
        let start = moment(st/1E6);
        let end = moment(et/1E6);
        let values = {
          "@timestamp": start.format(),
          "ecs": {
            "version": "1.6.0"
          },
          "event": {
            "created": subresp.data.modifiedTimeMillis || subresp.data.computationTimeMillis,
            "dataset": "google.fit",
            "ingested": new Date(),
            "kind": "event",
            "module": ds.dataStreamName,
            "original": JSON.stringify(item),
            "start": start.format(),
            "end": end.format(),
            "duration": (et - st)
          },
          "time_slice": time2slice(start),
          "date_details": {
            "year": start.format('YYYY'),
            "month": {
              "number": start.format('M'),
              "name": start.format('MMMM'),
            },
            "week_number": start.format('W'),
            "day_of_year": start.format('DDD'),
            "day_of_month": start.format('D'),
            "day_of_week": {
              "number": start.format('d'),
              "name": start.format('dddd'),
            }
          },
          "fit": {
          }
        };
        providerAttrs = [];
        if (device) {
          if (device.manufacturer) {
            providerAttrs.push(device.manufacturer);
          }
          if (device.model) {
            providerAttrs.push(device.model);
          }
          if (device.version) {
            providerAttrs.push(device.version);
          }
        }
        if (app) {
          if (app.packageName) {
            providerAttrs.push(app.packageName);
          }
          else if (app.name) {
            providerAttrs.push(app.name);
          }
          if (app.version) {
            providerAttrs.push(app.version);
          }
        }
        if (providerAttrs.length) {
          values.event.provider = providerAttrs.join('.');;
        }
        let ll = [];
        for (var k=0; k<dType.field.length; k++) {
          if (!item.value[k] || item.value[k] === 0) {
            continue;
          }
          values.fit[dType.field[k].name] = getValue(item.value[k]);
          if (dType.field[k].name == 'activity') {
            values.fit['activityId'] = item.value[k].intVal;
            let activity = activityTypes[item.value[k].intVal];
            if (activity) {
              values.fit.activity = activity;
            }
          }
          else if (dType.field[k].name == 'latitude') {
            ll[0] = item.value[k].fpVal;
          }
          else if (dType.field[k].name == 'longitude') {
            ll[1] = item.value[k].fpVal;
          }
        }
        if (ds.dataStreamName) {
          values.fit.shortName = ds.dataStreamName.replace(/^(.*?)\:.*\:(.*?)$/,'$1...$2');
        }
        values.fit.point = item;
        values.fit.dataSource = ds;
        values.fit.dataSource.minStartTimeNs = subresp.data.minStartTimeNs;
        values.fit.dataSource.maxEndTimeNs = subresp.data.maxEndTimeNs;
        values.fit.dataSource.dataSourceId = subresp.data.dataSourceId;
        values.fit.dataSource.minStartTimeNs = subresp.data.minStartTimeNs;
        if (ll.length == 2) {
          values.position = {
            geo: ll.join(',')
          };
        }
        // console.log(JSON.stringify(values, null, 4));
        // console.log('%s: %d', dsName, j+1);
        bulk.push({index: {_index: c8._index, _id: id}});
        bulk.push(values);
      }
      // console.log(JSON.stringify(bulk, null, 2));
      if (bulk.length > 0) {
        let response = await c8.bulk(bulk);
        let result = c8.trimBulkResults(response);
        if (result.errors) {
          let messages = [];
          for (var k=0; k<result.items.length; k++) {
            if (result.items[k].index.error) {
              messages.push(k + ': ' + result.items[k].index.error.reason);
            }
          }
          console.error(messages.length + ' errors in bulk insert:\n ' + messages.join('\n '));
        }
        // console.log('Indexed ' + result.items.length + ' documents in ' + result.took + ' ms.');
      }
      else {
        console.log('No data available');
      }
    }
    else {
      // let sd = new Date(subresp.data.minStartTimeNs/1E6);
      // let ed = new Date(subresp.data.maxEndTimeNs/1E6);
      // console.log('No data between ' + sd.toISOString() + ' and ' + ed.toISOString());
    }
  }
  return 'Checked ' + resp.data.dataSource.length + ' data sources.'
}

importSessions = async (c8, conf, opts) => {
  let firstDate = new Date();
  firstDate.setTime(firstDate.getTime() - MS_IN_DAY);
  let lastDate = opts.lastDate || new Date();
  let response = await c8.type(adapter.types[1].name).search({
    _source: ['@timestamp'],
    size: 1,
    sort: [{'@timestamp': 'desc'}],
  });
  if (opts.firstDate) {
    firstDate = opts.firstDate;
    console.log('Setting first time to ' + firstDate);
  }
  else if (response && response.body && response.body.hits && response.body.hits.hits && response.body.hits.hits[0] && response.body.hits.hits[0]._source && response.body.hits.hits[0]._source['@timestamp']) {
    let d = new Date(response.body.hits.hits[0]._source['@timestamp']);
    // firstDate = new Date(d.getTime() + 1);
    firstDate = d;
    console.log('Setting first time to ' + firstDate);
  }
  else {
    firstDate = new Date(2014, 10, 14); // Google Fit Launch Date
    console.warn('No previously indexed data, setting first time to ' + firstDate);
  }
  if (lastDate.getTime() >= (firstDate.getTime() + (MS_IN_DAY * MAX_SESSION_DAYS))) {
    lastDate.setTime(firstDate.getTime() + (MS_IN_DAY * MAX_SESSION_DAYS) - 1);
    console.warn('Max time range ' + MAX_SESSION_DAYS + ' days, setting end time to ' + lastDate);
  }
  const sessionOpts = {
    auth: opts.oauth2Client,
    userId: 'me'
  };
  
  sessionOpts.endTime = moment(lastDate).utc().format('YYYY-MM-DDTHH:mm:ssZ');
  if (firstDate) {
    sessionOpts.startTime = moment(firstDate).utc().format('YYYY-MM-DDTHH:mm:ssZ');
  }
  const fit = google.fitness('v1');
  // console.log(sessionOpts);
  const resp = await fit.users.sessions.list(sessionOpts);
  let bulk = [];
  for (var i=0; i<resp.data.session.length; i++) {
    const session = resp.data.session[i]; 
    const app = Object.assign({}, session.application);
    delete(session.application);
    // console.log(session);
    // return "In progress...";
    const type = session.activityType;
    const id = session.id;
    const start = moment(parseInt(session.startTimeMillis));
    const end = moment(parseInt(session.endTimeMillis));
    let values = {
      "@timestamp": start.format(),
      "ecs": {
        "version": "1.6.0"
      },
      "event": {
        "created": session.modifiedTimeMillis,
        "dataset": "google.fit",
        "ingested": new Date(),
        "kind": "event",
        "module": "sessions",
        "original": JSON.stringify(session),
        "start": start.format(),
        "end": end.format(),
        "duration": (session.endTimeMillis - session.startTimeMillis) * 1E6
      },
      "time_slice": time2slice(start),
      "date_details": {
        "year": start.format('YYYY'),
        "month": {
          "number": start.format('M'),
          "name": start.format('MMMM'),
        },
        "week_number": start.format('W'),
        "day_of_year": start.format('DDD'),
        "day_of_month": start.format('D'),
        "day_of_week": {
          "number": start.format('d'),
          "name": start.format('dddd'),
        }
      },
      "fit": {
        activity: activityTypes[type],
        activityId: type,
        session: session,
        application: app,
      }
    };
    providerAttrs = [];
    if (app) {
      if (app.packageName) {
        providerAttrs.push(app.packageName);
      }
      else if (app.name) {
        providerAttrs.push(app.name);
      }
      if (app.version) {
        providerAttrs.push(app.version);
      }
    }
    if (providerAttrs.length) {
      values.event.provider = providerAttrs.join('.');;
    }
    bulk.push({index: {_index: c8._index, _id: id}});
    bulk.push(values);
  }
  // console.log(JSON.stringify(bulk, null, 2));
  // return "in progress...";
  if (bulk.length > 0) {
    let response = await c8.bulk(bulk);
    let result = c8.trimBulkResults(response);
    if (result.errors) {
      let messages = [];
      for (var k=0; k<result.items.length; k++) {
        if (result.items[k].index.error) {
          messages.push(k + ': ' + result.items[k].index.error.reason);
        }
      }
      console.error(messages.length + ' errors in bulk insert:\n ' + messages.join('\n '));
      return messages.length + ' errors in sessions';
    }
    return 'Indexed ' + (bulk.length/2) + ' sessions in ' + result.took + ' ms.';
  }
  else {
    return 'No sessions available';
  }
}

function getValue(obj) {
    // what about string types?
    if (obj.stringVal || obj.stringVal === 0) {
	return obj.stringVal;
    }
    if (obj.fpVal || obj.fpVal === 0) {
	return obj.fpVal;
    }
    if (obj.intVal || obj.intVal === 0) {
	return obj.intVal;
    }
    if (obj.mapVal) {
      let values = [];
      for (var i=0; i<obj.mapVal.length; i++) {
	let tmp = {};
	tmp[obj.mapVal[i].key] = obj.mapVal[i].value.fpVal;
	values.push(tmp);
      }
      if (values.length) {
	return values;
      }
    }
    if (obj.value || obj.value === 0) {
	return obj.value;
    }
  // console.trace(obj);
  return null;
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
