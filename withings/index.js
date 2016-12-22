var express = require('express'),
  moment = require('moment'),
  url = require('url'),
  Withings = require('withings-oauth2').Withings

var redirectUri = 'http://localhost:9484/authcallback';

var MAX_DAYS = 30;
var MS_IN_DAY = 24 * 60 * 60 * 1000;
var measTypes = [];
measTypes[1] = 'Weight';
measTypes[4] = 'Height';
measTypes[5] = 'Fat Free Mass';
measTypes[6] = 'Fat Ratio';
measTypes[8] = 'Fat Mass Weight';
measTypes[9] = 'Diastolic Blood Pressure';
measTypes[10] = 'Systolic Blood Pressure';
measTypes[11] = 'Heart Pulse';
measTypes[12] = 'Room Temperature';
measTypes[54] = 'SP02';
measTypes[71] = 'Body Temperature';
measTypes[73] = 'Skin Temperature';
measTypes[76] = 'Muscle Mass';
measTypes[77] = 'Hydration';
measTypes[88] = 'Bone Mass';
measTypes[91] = 'Pulse Wave Velocity';

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
    var defaultUrl = url.parse(conf.redirect_uri);
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

      return client.getAccessToken(conf.requestToken, conf.requestTokenSecret, verifier,
        function (err, token, secret) {
          if (err) {
            console.error(err);
            return;
          }
          conf.accessToken = token;
          conf.accessTokenSecret = secret;
          return c8.config(conf);
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
        var s = obj[i].measures[j].value;
        var u = obj[i].measures[j].unit;
        var values = {
          timestamp: meta.date,
          category: meta.category,
          standardValue: v,
          unit: u
        }
        var realValue = v * Math.pow(10, u);
        values[measTypes[t]] = realValue;
        var id = meta.date + '-' + t;
        console.log(id + ': ' + realValue);
        bulk.push({index: {_index: c8._index, _type: c8._type, _id: id}});
        bulk.push(values);
      }
    }
    console.log(bulk);
    return;
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
