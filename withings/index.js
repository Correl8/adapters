var express = require('express'),
  moment = require('moment'),
  url = require('url'),
  Withings = require('withings-oauth2').Withings

var redirectUri = 'http://localhost:9484/authcallback';

var MAX_DAYS = 30;
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

var adapter = {};

adapter.sensorName = 'withings';
var indexName = 'withings';

adapter.types = [
  {
    name: indexName,
    fields: {
      "timestamp": "date",
      "measures": {
        "value": "float",
        "type": "integer",
        "unit": "integer",
      }
    }
  }
];
for (var i=1; i<measTypes.length; i++) {
  if (!measTypes[i]) continue;
  adapter.types[0].fields[measTypes[i].name] = 'float';
}

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
    client.getMeasuresAsync(null, firstDate, lastDate).then(function(data) {
      var bulk = [];
      var obj = data.body.measuregrps;
      for (var i=0; i<obj.length; i++) {
        var meta = {
          date: obj[i].date,
          cat: (obj[i].category === 2 ? 'objective' : 'real')
        }
        // console.log(JSON.stringify(obj[i]));
        var values = {
          timestamp: meta.date * 1000,
          category: meta.cat,
          measures: obj[i].measures
        }
        // var id = meta.date + '-' + meta.cat;
        var id = meta.date + '-' + obj[i].grpid + '-' + meta.cat;
        for (var j=0; j<obj[i].measures.length; j++) {
          var t = obj[i].measures[j].type;
          var v = obj[i].measures[j].value;
          var u = obj[i].measures[j].unit;
          var realValue = v * Math.pow(10, u);
          values[measTypes[t].name] = realValue;
          var d = new Date(meta.date * 1000);
          // console.log(d + ': ' + measTypes[t].name + ' = ' + realValue);
        }
        bulk.push({index: {_index: c8._index, _type: c8._type, _id: id}});
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

module.exports = adapter;
