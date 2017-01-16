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
  c8.type(indexName).search({
    _source: ['timestamp'],
    size: 1,
    sort: [{'timestamp': 'desc'}],
  }).then(function(response) {
    console.log('Getting first date...');
    return c8.search({
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
      importData(c8, conf, firstDate, lastDate);
    });
  }).catch(function(error) {
    console.trace(error);
  });
}

function importData(c8, conf, firstDate, lastDate) {
  var client = new Withings(conf);
  client.getMeasuresAsync(null, firstDate, lastDate).then(function(data) {
    var bulk = [];
    var obj = data.body.measuregrps;
    for (var i=0; i<obj.length; i++) {
      var meta = {
        date: obj[i].date,
        cat: (obj[i].category === 2 ? 'objective' : 'real')
      }
      for (var j=0; j<obj[i].measures.length; j++) {
        var t = obj[i].measures[j].type;
        var v = obj[i].measures[j].value;
        var u = obj[i].measures[j].unit;
        var values = {
          timestamp: meta.date * 1000,
          category: meta.cat,
          standardValue: v,
          factor: u,
          unit: measTypes[t].unit
        }
        var realValue = v * Math.pow(10, u);
        values[measTypes[t].name] = realValue;
        var id = meta.date + '-' + t;
        var d = new Date(meta.date * 1000);
        console.log(d + ': ' + measTypes[t].name + ' = ' + realValue);
        bulk.push({index: {_index: c8._index, _type: c8._type, _id: id}});
        bulk.push(values);
      }
    }
    if (bulk.length > 0) {
      return c8.bulk(bulk).then(function(result) {
        console.log('Indexed ' + result.items.length + ' documents in ' + result.took + ' ms.');
      }).catch(function(error) {
        console.trace(error);
      });
    }
  }).catch(function(error){
    console.error(error)
  })
}

module.exports = adapter;