const // withings = require('withings'),
moment = require('moment'),
express = require('express'),
url = require('url'),
rp = require('request-promise');
const { v4: uuidv4 } = require('uuid/');

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

const baseUrl = 'https://account.withings.com';
const dateFormat = 'X';

let adapter = {};

adapter.sensorName = 'withings';
let measureIndex = adapter.sensorName + '-measure';

adapter.types = [
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
        "duration": "long",
        "end": "date",
        "module": "keyword",
        "original": "keyword",
        "start": "date",
        "timezone": "keyword"
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
  adapter.types[0].fields.withings[measTypes[i].name] = 'float';
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
      const response = await c8.search({
        _source: ['withings.updatetime'],
        size: 1,
        sort: [{'withings.updatetime': 'desc'}],
      });
      const resp = c8.trimResults(response);
      let firstDate = new Date();
      let lastDate = opts.lastDate || new Date();
      firstDate.setTime(lastDate.getTime() - (MAX_DAYS * MS_IN_DAY));
      if (opts.firstDate) {
        firstDate = new Date(opts.firstDate);
        console.log('Setting first time to ' + firstDate);
      }
      else if (resp && resp.withings.updatetime) {
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
      messages.push(await importMeasures(c8, conf, firstDate, lastDate));
      fulfill(messages.length + ' dataset' + (messages.length == 1 ? '' : 's') + '.\n' + messages.join('\n'));
    }
    catch(e) {
      reject(new Error(e));
    }
  });
};

function importMeasures(c8, conf, firstDate, lastDate) {
  return new Promise(async (fulfill, reject) => {
    try {
      const step4 = {
        method: 'GET',
        uri: 'https://wbsapi.withings.net/measure',
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
        let data = {
          "@timestamp": d.format(),
          "ecs": {
            "version": "1.0.1"
          },
          "event": {
            "created": grp.created,
            "dataset": "withings.measure",
            "module": "withings",
            "original": JSON.stringify(grp),
            "start": d.format(),
          },
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
          reject(new Error(messages.length + ' errors in bulk insert:\n ' + messages.join('\n ')));
        }
        fulfill('Indexed ' + result.items.length + ' measure group' + (result.items.length == 1 ? '' : 's')+ ' in ' + result.took + ' ms.');
      }
      else {
        fulfill('No measures to import');
      }
    }
    catch (e) {
      reject(new Error(e));
    }
  });
}

module.exports = adapter;
