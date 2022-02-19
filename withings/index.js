// const withings = require('withings'),
const moment = require('moment');
// const express = require('express'), // required later only when used
const url = require('url');
const rp = require('request-promise');
const { v4: uuidv4 } = require('uuid');

var MAX_DAYS = 7;
var MS_IN_DAY = 24 * 60 * 60 * 1000;
var measTypes = [];
measTypes[1] = {name: 'Weight', unit: 'kg'};
measTypes[4] = {name: 'Height', unit: 'meter'};
measTypes[5] = {name: 'Fat Free Mass', unit: 'kg'};
measTypes[6] = {name: 'Fat Ratio', unit: '%'};
measTypes[8] = {name: 'Fat Mass Weight', unit: 'kg'};
measTypes[9] = {name: 'Diastolic Blood Pressure', unit: 'mmHg'};
measTypes[10] = {name: 'Systolic Blood Pressure', unit: 'mmHg'};
measTypes[11] = {name: 'Heart Pulse', unit: 'bpm'};
measTypes[12] = {name: 'Room Temperature', unit: '°C'};
measTypes[54] = {name: 'SP02', unit: '%'};
measTypes[71] = {name: 'Body Temperature', unit: '°C'};
measTypes[73] = {name: 'Skin Temperature', unit: '°C'};
measTypes[76] = {name: 'Muscle Mass', unit: 'kg'};
measTypes[77] = {name: 'Hydration', unit: '?'};
measTypes[88] = {name: 'Bone Mass', unit: 'kg'};
measTypes[91] = {name: 'Pulse Wave Velocity', unit: 'm/s'};

// proxy forward redirectUri to http://localhost:{authPort}
const redirectUri = 'https://correl8.me/authcallback';
const authPort = 4343;

// const baseUrl = 'https://account.withings.com';
// const baseUrl = 'https://wbsapi.us.withingsmed.net';
const apiBase = 'https://wbsapi.withings.net/';
const accountUrl = 'https://account.withings.com';
const dateFormat = 'X';

let adapter = {};

adapter.sensorName = 'withings';
let measureIndex = adapter.sensorName + '-measure';

adapter.types = [
  {
    // emtpy "type" to share config with sleep adapter
    name: adapter.sensorName,
    fields: {}
  },
  {
    name: measureIndex,
    fields: {
      "@timestamp": "date",
      "ecs": {
        "version": 'keyword'
      },
      "event": {
        "created": "date",
        "dataset": "keyword",
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
        "updatetime": "date",
        "category": "keyword",
        "group_id": "long",
        "measures": {
          "value": "float",
          "type": "integer",
          "unit": "integer",
        }
      }
    }
  }
];
for (var i=1; i<measTypes.length; i++) {
  if (!measTypes[i]) continue;
  adapter.types[1].fields.withings[measTypes[i].name] = 'float';
}

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
  conf.url = url.parse(accountUrl);
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
          // uri: baseUrl + '/oauth2/token',
          uri: apiBase + '/v2/oauth2',
          form: {
            action: 'requesttoken',
            client_id: conf.clientId,
            client_secret: conf.clientSecret,
            grant_type: 'authorization_code',
            code: conf.code,
            redirect_uri: conf.redirectUri
          }
        };
        const tokenBody = JSON.parse(await rp(step2));
        if (!tokenBody.body) {
          throw new Error('Failed to get access token: ' + tokenBody.status);
        }
        Object.assign(conf, tokenBody.body);
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

adapter.importData = async (c8, conf, opts) => {
  try {
    console.log('Getting first date...');
    const response = await c8.index(measureIndex).search({
      _source: ['withings.updatetime'],
      // _source: ['@timestamp'],
      size: 1,
      sort: [{'withings.updatetime': 'desc'}],
      // sort: [{'@timestamp': 'desc'}],
    });
    const resp = c8.trimResults(response);
    let firstDate = new Date();
    let lastDate = opts.lastDate || new Date();
    firstDate.setTime(lastDate.getTime() - (MAX_DAYS * MS_IN_DAY));
    if (opts.firstDate) {
      firstDate = new Date(opts.firstDate);
      console.log('Setting first time to ' + firstDate);
    }
    /*
      else if (resp && resp['@timestamp']) { // temp, for re-indexing
      firstDate = new Date(resp['@timestamp']);
      firstDate.setTime(firstDate.getTime() + 1000);
      console.log('Setting first time to ' + firstDate);
      }
    */
    else if (resp && resp.withings && resp.withings.updatetime) {
      firstDate = new Date(resp.withings.updatetime);
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
      uri: apiBase + '/v2/oauth2',
      form: {
        action: 'requesttoken',
        client_id: conf.clientId,
        client_secret: conf.clientSecret,
        grant_type: 'refresh_token',
        refresh_token: conf.refresh_token
      }
    };
    const tokenBody = JSON.parse(await rp(step3));
    Object.assign(conf, tokenBody.body);
    await c8.config(conf);
    let messages = [];
    messages.push(await importMeasures(c8, conf, firstDate, lastDate));
    return messages.length + ' dataset' + (messages.length == 1 ? '' : 's') + '.\n' + messages.join('\n');
  }
  catch(e) {
    throw (new Error(e));
  }
};

async function importMeasures(c8, conf, firstDate, lastDate) {
  try {
    const step4 = {
      method: 'GET',
      uri: apiBase + 'measure',
      form: {
        action: 'getmeas',
        // get both real measures and objectives
        // category: 1,
      },
      headers: {
        Authorization: 'Bearer ' + conf.access_token
      }
    };
    if (firstDate) {
      step4.form.lastupdate = moment(firstDate).format(dateFormat);
    }
    const body = await rp(step4);
    let obj = JSON.parse(body);
    // console.log(obj);
    let ts = moment(obj.body.updatetime * 1000);
    let bulk = [];
    obj.body.measuregrps.forEach((grp) => {
      // console.log(grp);
      let d = moment(grp.date * 1000);
      let id = grp.date + '-' + grp.grpid + '-' + grp.category;
      let slice = time2slice(d);
      let data = {
        "@timestamp": d.format(),
        "ecs": {
          "version": "1.6.0"
        },
        "event": {
          "created": grp.created * 1000,
          "dataset": "withings.measure",
          "ingested": new Date(),
          "kind": "metric",
          "module": "withings",
          "original": JSON.stringify(grp),
          "start": d.format(),
        },
        "date_details": {
          "year": parseInt(d.format('YYYY')),
          "month": {
            "number": parseInt(d.format('M')),
            "name": d.format('MMMM'),
          },
          "week_number": parseInt(d.format('W')),
          "day_of_year": parseInt(d.format('DDD')),
          "day_of_month": parseInt(d.format('D')),
          "day_of_week": {
            "number": parseInt(d.format('d')),
            "name": d.format('dddd'),
          }
        },
        "time_slice": slice,
        "withings": {
          "updatetime": ts,
          "group_id": grp.id,
          "category": grp.category,
          "measures": grp.measures
        }
      };
      if (grp.comment) {
        data.withings.comment = grp.comment;
      }
      // console.log(data);
      grp.measures.forEach((m) => {
        let t = m.type;
        let v = m.value;
        let u = m.unit;
        let realValue = v * Math.pow(10, u);
        data.withings[measTypes[t].name] = realValue;
      });
      bulk.push({index: {_index: c8._index, _id: id}});
      bulk.push(data);
      console.log('Measures: ' + d.format());
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
        throw(new Error(messages.length + ' errors in bulk insert:\n ' + messages.join('\n ')));
      }
      return 'Indexed ' + result.items.length + ' measure group' + (result.items.length == 1 ? '' : 's')+ ' in ' + result.took + ' ms.';
    }
    else {
      return 'No measures to import';
    }
  }
  catch (e) {
    throw(new Error(e));
  }
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
