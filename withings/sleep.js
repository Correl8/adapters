const express = require('express'),
  moment = require('moment'),
  url = require('url'),
  Withings = require('withings-oauth2').Withings

const redirectUri = 'http://localhost:9484/authcallback';

const MAX_DAYS = 7;
const MS_IN_DAY = 24 * 60 * 60 * 1000;
const states = ['awake', 'light sleep', 'deep sleep', 'REM sleep'];

let adapter = {};

adapter.sensorName = 'withings';
let indexName = 'withings-sleep';

adapter.types = [
  {
    name: indexName,
    fields: {
      "timestamp": "date",
      "startdate": "date",
      "enddate": "date",
      "state": "keyword",
      "stateid": "integer",
      "duration": "float"
    }
  },
  {
    name: indexName + 'summary',
    fields: {
      "timestamp": "date",
      "startdate": "date",
      "enddate": "date",
      "modified": "date",
      "date": "keyword",
      "timezone": "keyword",
      "totalsleepduration": "float",
      "data": {
        "wakeupduration": "float",
        "lightsleepduration": "float",
        "deepsleepduration": "float",
        "remsleepduration": "float",
        "durationtosleep": "float",
        "durationtowakeup": "float",
        "wakeupcount": "float"
      }
    }
  }
];

adapter.promptProps = {
  properties: {
    consumerKey: {
      description: 'Enter your consumerKey'.magenta
    },
    consumerSecret: {
      description: 'Enter your consumerSecret'.magenta
    },
    callbackUrl: {
      description: 'Enter your redirect URL'.magenta,
      default: redirectUri
    },
  }
};

adapter.storeConfig = function(c8, result) {
  let conf = result;
  return c8.config(conf).then(function(){
    let defaultUrl = url.parse(conf.callbackUrl);
    let express = require('express');
    let app = express();
    let port = process.env.PORT || defaultUrl.port;

    let client = new Withings(conf);
    let server = app.listen(port, function () {
      client.getRequestToken(function (err, token, tokenSecret) {
        if (err) {
          return;
        }
        conf.token = token;
        conf.tokenSecret = tokenSecret;
        return c8.config(conf).then(function(){
          let authUri = client.authorizeUrl(token, tokenSecret);
          console.log("Please, go to \n" + authUri);
        });
      });
    });

    app.get(defaultUrl.pathname, function (req, res) {
      let verifier = req.query.oauth_verifier;
      conf.userID = req.query.userid;
      let client = new Withings(conf);

      return client.getAccessToken(conf.token, conf.tokenSecret, verifier,
        function (err, token, secret) {
          if (err) {
            console.error(err);
            return;
          }
          conf.accessToken = token;
          conf.accessTokenSecret = secret;
          server.close();
          return c8.config(conf).then(function(){
            res.send('Access token saved.');
            console.log('Configuration stored.');
            c8.release();
            process.exit();
          }).catch(function(error) {
            console.trace(error);
          });
        }
      );
    });
  }).catch(function(error) {
    console.trace(error);
  });
};

adapter.importData = function(c8, conf, opts) {
  return new Promise(function (fulfill, reject){
    c8.type(indexName).search({
      _source: ['timestamp'],
      size: 1,
      sort: [{'timestamp': 'desc'}],
    }).then(function(response) {
      // console.log('Getting first date...');
      c8.search({
        _source: ['timestamp'],
        size: 1,
        sort: [{'timestamp': 'desc'}],
      }).then(function(response) {
        let resp = c8.trimResults(response);
        let firstDate = new Date();
        let lastDate = opts.lastDate || new Date();
        firstDate.setTime(lastDate.getTime() - (MAX_DAYS * MS_IN_DAY));
        if (opts.firstDate) {
          firstDate = new Date(opts.firstDate);
          console.log('Setting first time to ' + firstDate);
        }
        else if (resp && resp.timestamp) {
          let d = new Date(resp.timestamp);
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
    let client = new Withings(conf);
    let promises = [];
    let params = {
      startdate: formatDate(firstDate),
      enddate: formatDate(lastDate)
    };
    promises[0] = client.getAsync('sleep', 'get', params).then(function(data) {
      let bulk = [];
      // console.log(JSON.stringify(data));
      if (!data || !data.body || !data.body.series) {
        reject(new Error('Invalid API response: ' + JSON.stringify(data)));
      }
      let obj = data.body.series;
      for (let i=0; i<obj.length; i++) {
        // console.log(JSON.stringify(obj[i]));
        let values = obj[i];
        values.timestamp = values.enddate * 1000;
        values.duration = values.enddate - values.startdate;
        values.startdate = new Date(values.startdate * 1000);
        values.enddate = new Date(values.enddate * 1000);
        values.modified = new Date(values.modified * 1000);
        values.stateid = values.state;
        values.state = states[values.stateid];
        bulk.push({index: {_index: c8.type(adapter.types[0].name)._index, _type: c8._type, _id: values.timestamp}});
        bulk.push(values);
        // console.log(values);
        // console.log(new Date(values.timestamp));
      }
      if (bulk.length > 0) {
        // console.log(JSON.stringify(bulk, null, 2));
        c8.bulk(bulk).then(function(response) {
          let result = c8.trimBulkResults(response);
          if (result.errors) {
            let messages = [];
            for (var i=0; i<result.items.length; i++) {
              if (result.items[i].index.error) {
                messages.push(i + ': ' + result.items[i].index.error.reason);
              }
            }
            reject(new Error(messages.length + ' errors in bulk insert:\n ' + messages.join('\n ')));
          }
          console.log('Indexed ' + result.items.length + ' sleep measure documents in ' + result.took + ' ms.');
        }).catch(function(error) {
          reject(error);
        });
      }
      else {
        console.log('No measure data available for import.');
      }
    }).catch(function(error){
      reject(error);
    });
    params = {
      startdateymd: formatDate(firstDate, true),
      enddateymd: formatDate(lastDate, true)
    };
    promises[1] = client.getAsync('sleep', 'getsummary', params).then(function(data) {
      let bulk = [];
      if (!data || !data.body || !data.body.series) {
        reject(new Error('Invalid API response: ' + JSON.stringify(data)));
      }
      let obj = data.body.series;
      for (var i=0; i<obj.length; i++) {
        // console.log(JSON.stringify(obj[i]));
        let values = obj[i];
        values.timestamp = values.enddate * 1000;
        values.startdate = new Date(values.startdate * 1000);
        values.enddate = new Date(values.enddate * 1000);
        values.modified = new Date(values.modified * 1000);
        values.totalsleepduration = values.data.lightsleepduration + values.data.deepsleepduration + values.data.remsleepduration;
        bulk.push({index: {_index: c8.type(adapter.types[1].name)._index, _type: c8._type, _id: values.id}});
        bulk.push(values);
        // console.log(values);
        console.log(values.date + ': ' + Math.round(values.totalsleepduration/360)/10 + ' hours of sleep');
      }
      if (bulk.length > 0) {
        // console.log(JSON.stringify(bulk, null, 2));
        c8.bulk(bulk).then(function(response) {
          let result = c8.trimBulkResults(response);
          if (result.errors) {
            let messages = [];
            for (var i=0; i<result.items.length; i++) {
              if (result.items[i].index.error) {
                messages.push(i + ': ' + result.items[i].index.error.reason);
              }
            }
            reject(new Error(messages.length + ' errors in bulk insert:\n ' + messages.join('\n ')));
          }
          console.log('Indexed ' + result.items.length + ' sleep summary documents in ' + result.took + ' ms.');
        }).catch(function(error) {
          reject(error);
        });
      }
      else {
        console.log('No summary data available for import.');
      }
    }).catch(function(error){
      reject(error);
    });
    Promise.all(promises).then((res) => {
      fulfill('Indexed measures and summaries.');
    });
  });
}

function formatDate(date, ymd) {
  if (!ymd) {
    return Math.round(date.getTime()/1000);
  }
  let y = date.getFullYear();
  let m = date.getMonth() + 1;
  let d = date.getDate();
  return y + '-' + (m<10 ? '0' : '') + m + '-' + (d<10 ? '0' : '') + d;
}

module.exports = adapter;
