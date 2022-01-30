const ClientOAuth2 = require('client-oauth2');
  express = require('express'),
  https = require('https'),
  moment = require('moment'),
  url = require('url'),
  uuid = require('uuid');
const { v4: uuidv4 } = require('uuid');

const MAX_DAYS = 5;
const MS_IN_DAY = 24 * 60 * 60 * 1000;

const apiEndpoints = {
  stations: 'https://api.netatmo.com/api/getstationsdata',
  measure: 'https://api.netatmo.com/api/getmeasure', // not used
};
const authOptions = {
  authorizationUri: 'https://api.netatmo.com/oauth2/authorize',
  accessTokenUri: 'https://api.netatmo.com/oauth2/token',
  redirectUri: 'https://correl8.me/authcallback',
  scopes: ['read_station']
};
const authPort = 4343;

var adapter = {};
adapter.sensorName = 'netatmo';

adapter.types = [
  {
    name: adapter.sensorName,
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
      "netatmo": {
        "_id": "keyword",
        "module_name": "keyword",
        "station_name": "keyword",
        "date_setup": "date",
        "last_setup": "date",
        "last_message": "date",
        "last_seen": "date",
        "type": "keyword",
        "last_status_store": "date",
        "module_name": "keyword",
        "firmware": "long",
        "rf_status": "long",
        "wifi_status": "long",
        "reachable": "boolean",
        "battery_vp": "long",
        "co2_calibrating": "boolean",
        "data_type": "keyword",
        "place": {
           "altitude": "long",
           "city": "keyword",
          "country": "keyword",
          "timezone": "keyword",
          "location": "geo_point"
        },
        "dashboard_data": {
          "time_utc": "date",
          "Temperature": "float",
          "CO2": "float",
          "Humidity": "float",
          "Noise": "float",
          "Pressure": "float",
          "AbsolutePressure": "float",
          "min_temp": "float",
          "max_temp": "float",
          "date_max_temp": "date",
          "date_min_temp": "date",
          "temp_trend": "keyword",
          "pressure_trend": "keyword"
        }
      },
    }
  }
];

adapter.promptProps = {
  properties: {
    clientId: {
      description: 'Enter your client ID'.magenta
    },
    clientSecret: {
      description: 'Enter your client secret'.magenta
    },
  }
};

adapter.storeConfig = async function(c8, result) {
  let conf = result;
  try {
    const express = require('express');
    const app = express();
    const defaultUrl = url.parse(authOptions.redirectUri);

    authOptions.state = uuidv4();
    Object.assign(authOptions, conf);
    const authClient = new ClientOAuth2(authOptions);
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
      let response = await c8.search({
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
    const authClient = new ClientOAuth2(authOptions);
    const auth = authClient.createToken(conf);
    const refreshed = await auth.refresh(conf);
    c8.config(Object.assign(conf, refreshed.data));
    const signed = refreshed.sign({url: apiEndpoints.stations});
    const api = url.parse(signed.url);
    api.headers = signed.headers;
    api.method = 'get';
    let stationData = '';
    const req = https.request(api, (res) => {
      res.on('data', (chunk) => {
        stationData += chunk;
      });
      res.on('end', async () => {
        try {
          const all = JSON.parse(stationData).body;
          // console.log(JSON.stringify(all, null, 3));
          all.devices.forEach(device => {
            device.modules.forEach(module => {
              const data = prepareECS(module);
              // console.log(JSON.stringify(data, null, 1));
              console.log(moment(data.netatmo.dashboard_data.time_utc).format() + ': ' + data.netatmo.dashboard_data.Temperature + ' °C ' + data.netatmo.module_name);
              c8.insert(data);
            });
            delete(device.modules);
            const data = prepareECS(device);
            // console.log(JSON.stringify(data, null, 1));
            console.log(moment(data.netatmo.dashboard_data.time_utc).format() + ': ' + data.netatmo.dashboard_data.Temperature + ' °C ' + data.netatmo.module_name);
            c8.insert(data);
          });
        }
        catch(error) {
          console.error(error);
        }
      });
    }).end();
    // console.log(data);
    // const data = await authClient.request(auth.sign(opts));
    // const data = await authClient.request(opts);
    // console.log(data);
  });
}

function prepareECS(obj) {
  obj.dashboard_data.time_utc = obj.dashboard_data.time_utc * 1000;
  obj.dashboard_data.date_max_temp = obj.dashboard_data.date_max_temp * 1000;
  obj.dashboard_data.date_min_temp = obj.dashboard_data.date_min_temp * 1000;
  obj.date_setup = obj.date_setup * 1000;
  obj.last_setup = obj.last_setup * 1000;
  if (obj.last_status_store) {
    obj.last_status_store = obj.last_status_store * 1000;
  }
  if (obj.last_seen) {
    obj.last_seen = obj.last_seen * 1000;
    obj.last_message = obj.last_message * 1000;
  }
  const t = moment(obj.dashboard_data.time_utc);
  return {
    "_id": t.format() + '-' + obj._id,
    "@timestamp": t.format(),
    "ecs": {
      "version": "1.0.1"
    },
    "event": {
      "created": moment().format(),
      "dataset": "netatmo.weather",
      "module": "netatmo",
      "original": JSON.stringify(obj),
      "start": t.format(),
      "timezone": obj.place ? obj.place.timezone : null
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
    "netatmo": obj
  };
}

module.exports = adapter;
