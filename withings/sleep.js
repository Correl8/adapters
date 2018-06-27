var express = require('express'),
  moment = require('moment'),
  url = require('url'),
  Withings = require('withings-oauth2').Withings

var redirectUri = 'http://localhost:9484/authcallback';

var MAX_DAYS = 7;
var MS_IN_DAY = 24 * 60 * 60 * 1000;
var states = ['awake', 'light sleep', 'deep sleep', 'REM sleep'];

var adapter = {};

adapter.sensorName = 'withings';
var indexName = 'withings-sleep';

adapter.types = [
  {
    name: indexName,
    fields: {
      "timestamp": "date",
      "startdate": "date",
      "enddate": "date",
      "state": "keyword",
      "stateid": "integer",
      "duration": "integer"
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
  var conf = result;
  return c8.config(conf).then(function(){
    var defaultUrl = url.parse(conf.callbackUrl);
    var express = require('express');
    var app = express();
    var port = process.env.PORT || defaultUrl.port;

    var client = new Withings(conf);
    var server = app.listen(port, function () {
      client.getRequestToken(function (err, token, tokenSecret) {
        if (err) {
          return;
        }
        conf.token = token;
        conf.tokenSecret = tokenSecret;
        return c8.config(conf).then(function(){
          var authUri = client.authorizeUrl(token, tokenSecret);
          console.log("Please, go to \n" + authUri);
        });
      });
    });

    app.get(defaultUrl.pathname, function (req, res) {
      var verifier = req.query.oauth_verifier;
      conf.userID = req.query.userid;
      var client = new Withings(conf);

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
    var client = new Withings(conf);
    var params = {
      startdate: formatDate(firstDate),
      enddate: formatDate(lastDate)
    };
    client.getAsync('sleep', 'get', params).then(function(data) {
      var bulk = [];
      // console.log(JSON.stringify(data));
      if (!data || !data.body || !data.body.series) {
        reject(new Error('Invalid API response: ' + JSON.stringify(data)));
      }
      var obj = data.body.series;
      for (var i=0; i<obj.length; i++) {
        // console.log(JSON.stringify(obj[i]));
        var values = obj[i];
        values.timestamp = values.startdate * 1000;
        values.duration = values.enddate - values.startdate;
        values.stateid = values.state;
        values.state = states[values.stateid];
        bulk.push({index: {_index: c8._index, _type: c8._type, _id: values.timestamp}});
        bulk.push(values);
        // console.log(values);
        console.log(new Date(values.timestamp));
      }
      if (bulk.length > 0) {
        // console.log(bulk);
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
          fulfill('Indexed ' + result.items.length + ' documents in ' + result.took + ' ms.');
        }).catch(function(error) {
          reject(error);
        });
      }
      else {
        fulfill('No data available for import.');
      }
    }).catch(function(error){
      reject(error);
    });
  });
}

function formatDate(date) {
  return Math.round(date.getTime()/1000);
}

module.exports = adapter;
