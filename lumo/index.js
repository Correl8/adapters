var lumolift = require('lumolift'),
  moment = require('moment'),
  express = require('express'),
  url = require('url');

var redirectUri = 'http://localhost:5866/authcallback';

var MAX_DAYS = 30;
var MS_IN_DAY = 24 * 60 * 60 * 1000;

var adapter = {};

adapter.sensorName = 'lumo';

var postureIndex = 'lumo';

adapter.types = [
  {
    name: postureIndex,
    fields: {
      "timestamp": "date",
      "localTime": "date",
      "dataSource": "string",
      "dataType": "string",
      "value": "integer",
      "uploadTime": "date",
      "badPostureDuration": "integer",
      "goodPostureDuration": "integer",
      "totalCalories": "integer",
      "walkingCalories": "integer",
      "runningCalories": "integer",
      "walkingDuration": "integer",
      "runningDuration": "integer",
      "totalDistance": "integer",
      "runningDistance": "integer",
      // "walkingDistance": "integer",
      "totalSteps": "integer",
      "runningSteps": "integer",
      // "walkingSteps": "integer"
    }
  }
];

adapter.promptProps = {
  properties: {
    client_id: {
      description: 'Enter your client ID'.magenta
    },
    client_secret: {
      description: 'Enter your client secret'.magenta
    },
    redirect_uri: {
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

    var options = {
      clientId: conf.client_id,
      clientSecret: conf.client_secret,
      redirectUri: conf.redirect_uri
    };
    var authClient = lumolift.Auth(options);
    var authUri = authClient.code.getUri();
    var server = app.listen(port, function () {
      console.log("Please, go to \n" + authUri)
    });

    app.get(defaultUrl.pathname, function (req, res) {
      return authClient.code.getToken(req.originalUrl).then(function(auth) {
        return auth.refresh().then(function(refreshed) {
          Object.assign(conf, refreshed.data);
          server.close();
          return c8.config(conf).then(function(){
            res.send('Access token saved.');
            console.log('Configuration stored.');
            c8.release();
            process.exit();
          }).catch(function(error) {
            console.trace(error);
          });
        }).catch(function(error) {
          console.trace(error);
        });
      }).catch(function(error) {
        console.trace(error);
      });
    });
  }).catch(function(error) {
    console.trace(error);
  });
};

adapter.importData = function(c8, conf, opts) {
  c8.type(postureIndex).search({
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
  var options = {
    clientId: conf.client_id,
    clientSecret: conf.client_secret,
    redirectUri: conf.redirect_uri
  };
  var token = conf.access_token;
  var authClient = lumolift.Auth(options);
  var auth = authClient.createToken(conf.access_token, conf.refresh_token);
  auth.refresh().then(function(refreshed) {
    Object.assign(conf, refreshed.data);
    return c8.config(conf).then(function(){
      var client = new lumolift.Client(conf.access_token);
      if (!firstDate) {
        console.warn('No starting date...');
        return;
      }
      var start = Math.floor(firstDate.getTime()/1000);
      var end = Math.ceil(lastDate.getTime()/1000);
      client.activities(start, end).then(function (response) {
        var bulk = [];
        var obj = response.data;
        var index = postureIndex;
        for (var i=0; i<obj.length; i++) {
          obj[i].localTime = obj[i].localTime * 1000;
          obj[i].uploadTime = obj[i].uploadTime * 1000;
          // local time will be handled as if it was UTC
          var ts = new Date(obj[i].localTime);
          var tzOffset = ts.getTimezoneOffset() * 60 * 1000;
          ts.setTime(ts.getTime() + tzOffset);
          obj[i].localTime += tzOffset;    
          obj[i].timestamp = ts;
          var type = obj[i].dataType;
          if (type == 'TOTAL_STEPS') {
            obj[i].totalSteps = obj[i].value;
          }
          else if (type == 'RUNNING_STEPS') {
            obj[i].runningSteps = obj[i].value;
          }
          else if (type == 'TOTAL_DISTANCE') {
            obj[i].totalDistance = obj[i].value;
          }
          else if (type == 'RUNNING_DISTANCE') {
            obj[i].runningDistance = obj[i].value;
          }
          else if (type == 'TOTAL_CALORIES') {
            obj[i].totalCalories = obj[i].value;
          }
          else if (type == 'WALKING_CALORIES') {
            obj[i].walkingCalories = obj[i].value;
          }
          else if (type == 'TIME_WALKING') {
            obj[i].walkingDuration = obj[i].value;
          }
          else if (type == 'RUNNING_CALORIES') {
            obj[i].runningCalories = obj[i].value;
          }
          else if (type == 'TIME_IN_GOOD_POSTURE') {
            obj[i].goodPostureDuration = obj[i].value;
          }
          else if (type == 'TIME_IN_BAD_POSTURE') {
            obj[i].badPostureDuration = obj[i].value;
          }
          var id = ts.toISOString() + '-' + obj[i].dataSource + '-' + type;
          console.log(id + ': ' + obj[i].value);
          bulk.push({index: {_index: c8._index, _type: c8._type, _id: id}});
          bulk.push(obj[i]);
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
    }).catch(function(error){
      console.error(error)
    });
  }).catch(function(error){
    console.error(error)
  });
}

module.exports = adapter;
