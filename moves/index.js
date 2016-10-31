var Moves = require('moves');
var dates = require('moves-date');
var express = require('express');
var moves;

var defaultPort = 3321;
var defaultUrl = 'http://localhost:' + defaultPort + '/';
var MAX_DAYS = 50;
var MS_IN_DAY = 24 * 60 * 60 * 1000;

var adapter = {};

adapter.sensorName = 'moves';

var summaryIndex = 'moves-summary';
var summaryType = 'moves-summary';
var moveIndex = 'moves-move';
var moveType = 'moves-move';
var placeIndex = 'moves-place';
var placeType = 'moves-place';

adapter.types = [
  {
    name: 'moves-summary',
    fields: {
      timestamp: 'date',
      summary: {
        activity: 'string',
        group: 'string',
        duration: 'float',
        distance: 'float',
        steps: 'integer',
        calories: 'integer'
      },
      caloriesIdle: 'integer',
      lastUpdate: 'date'
    }
  },
  {
    name: 'moves-move',
    fields: {
      type: 'string',
      startTime: 'date',
      endTime: 'date',
      activities: {
        activity: 'string',
        group: 'string',
        manual: 'boolean',
        startTime: 'date',
        endTime: 'date',
        duration: 'float',
        distance: 'float',
        steps: 'integer',
        calories: 'integer',
        trackPoints: {
          lat: 'float',
          lon: 'float',
          position: 'geo_point',
          time: 'date'
        }
      },
      'lastUpdate': 'date'
    }
  },
  {
    name: 'moves-place',
    fields: {
      type: 'string',
      startTime: 'date',
      endTime: 'date',
      duration: 'float',
      activities: {
        activity: 'string',
        group: 'string',
        manual: 'boolean',
        startTime: 'date',
        endTime: 'date',
        duration: 'float',
        distance: 'float',
        steps: 'integer',
        calories: 'integer',
        trackPoints: {
          lat: 'float',
          lon: 'float',
          position: 'geo_point',
          time: 'date'
        }
      },
      place: {
        id: 'integer',
        type: 'string',
        foursquareId: 'string',
        foursquareCategoryIds: 'string',
        // position: 'geo_point',
        location: {
          lat: 'float',
          lon: 'float',
          position: 'geo_point'
        }
      },
      'lastUpdate': 'date'
    }
  }
];

adapter.promptProps = {
  properties: {
    client_id: {
      description: 'Enter your Moves client ID'.magenta
    },
    client_secret: {
      description: 'Enter your Moves client secret'.magenta
    },
    redirect_uri: {
      description: 'Enter your redirect URL'.magenta,
      default: defaultUrl
    },
  }
};

adapter.storeConfig = function(c8, result) {
  var conf = result;
  var app = express();
  var server = app.listen(defaultPort, function () {
    var host = server.address().address;
    var port = server.address().port;
    // console.log('Temporary Moves auth server listening at http://%s:%s', host, port);
  });
  moves = new Moves(result);
  var auth_url = moves.authorize({
    scope: ['activity', 'location']
  });
  console.log('Authorize the app by opening the followin url in your browser:\n ' + auth_url);
  app.all('/', function (req, res) {
    if (req.query && req.query.code) {
      // moves = new Moves(result);
      moves.token(req.query.code, function(error, response, body) {
        if (error) {
          console.trace(error);
          res.json(error);
          return;
        }
        var rb = JSON.parse(response.body);
        if (rb) {
          Object.assign(conf, rb);
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
        else {
          res.send('Authorization failed');
          console.error('Authorization failed');
          console.trace(res);
        }
      });
    }
    else {
      res.send('Waiting for code in query string...');
    }
  });
}

adapter.importData = function(c8, conf, opts) {
  moves = new Moves(conf);
  return c8.type(summaryType).search({
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
        // refetch yesterday for updated summary
        firstDate.setTime(d.getTime() - MS_IN_DAY);
        console.log('Setting first time to ' + firstDate);
      }
      else {
        moves.get('/user/profile', conf.access_token, function(error, response, body) {
          if (error) {
            console.trace(error);
            return;
          }
          else if (response.body === 'expired_access_token') {
            console.warn('access token expired');
            refreshToken(c8, conf);
          }
          else {
            var rb = JSON.parse(response.body);
            if (!rb || rb.error) {
              console.trace(rb.error);
              refreshToken(c8, conf);
            }
            else {
              var user = JSON.parse(body);
              // console.log(user);
              // console.log('Setting first time to Moves date ' + user.profile.firstDate);
              firstDate = dates.parseISODate(user.profile.firstDate);
            }
          }
        });
      }
      if (lastDate.getTime() > (firstDate.getTime() + MAX_DAYS * MS_IN_DAY)) {
        lastDate.setTime(firstDate.getTime() + MAX_DAYS * MS_IN_DAY);
        console.warn('Setting last date to ' + lastDate);
      }
      return importData(c8, conf, firstDate, lastDate);
    });
  }).catch(function(error) {
    console.trace(error);
  });
}

function importData(c8, conf, firstDate, lastDate) {
  if (!firstDate) {
    console.warn('No starting date...');
    return;
  }
  var startTime = firstDate;
  // startTime.setDate(startTime.getDate() + 1);
  var fromDate = dates.day(startTime);
  var end = dates.day(lastDate);
  if (fromDate === dates.day(new Date())) {
    console.log('Today\'s data already exists! Try again tomorrow...');
  }
  var dayCount = 0;
  var doneCount = 0;
  while (fromDate < end) {
    toDate = fromDate;
    moves.get('/user/storyline/daily?from=' + fromDate + '&to=' + toDate + '&trackPoints=true', conf.access_token, function(error, response, body) {
      if (error) {
        console.warn(error);
        return;
      }
      else if (response.body === 'expired_access_token') {
        console.warn('access token expired');
        return refreshToken(c8, conf);
      }
      else if (!response.body) {
        console.warn('No response body in history!');
        return;
      }
      else if (response.body.substr(0, 1) != '[') {
        console.warn('Invalid response body in history: ' + response.body)
        return;
      }
      else {
        var rb = JSON.parse(response.body);
          if (!rb || rb.error) {
            if (rb.error) {
              console.warn(rb.error);
            }
          return refreshToken(c8, conf);
        }
        else {
          var document = JSON.parse(body)[0];
          console.log(document.date);
          var bulk = splitToBulk(c8, prepareForElastic(document));
          return c8.bulk(bulk).then(function(result) {
            console.log('Indexed ' + result.items.length + ' documents in ' + result.took + ' ms.');
          }).catch(function(error) {
            console.trace(error);
          });
        }
      }
      if (parseInt(response.headers['x-ratelimit-minuteremaining']) < 1) {
        console.warn('Minute limit exceeded!')
        dayCount = MAX_DAYS;
      }
    });
    if (dayCount++ >= MAX_DAYS) {
      return;
    }
    startTime.setDate(startTime.getDate() + 1);
    fromDate = dates.day(startTime);
  }
}

function refreshToken(c8, conf) {
  moves.refresh_token(conf.refresh_token, function(error, response, body) {
    if (error) {
      console.warn('Refresh got error: ' + error);
      return;
    }
    var rb = JSON.parse(response.body);
    console.log(rb);
    if (!rb || rb.error) {
      if (rb.error === 'invalid_grant') {
        console.trace(rb.error);
      }
      console.trace(rb.error);
    }
    else {
      c8.conf(Object.assign(conf, rb)).then(function() {
        console.log('Token refreshed. Try again!')
      });
    }
  });
}

function prepareForElastic(document) {
  for (var prop in document) {
    // console.log(prop + ' is ' + typeof(document[prop]));
    if (typeof(document[prop]) === 'object') {
      // console.log('Object ' + prop);
      document[prop] = prepareForElastic(document[prop]);
    }
    else if (typeof(document[prop]) === 'array') {
      // console.log('Array ' + prop);
      for (var i=0; i<document[prop].length; i++) {
        document[prop][i] = prepareForElastic(document[prop][i]);
      }
    }
    else if ((prop === 'startTime') || (prop === 'endTime') ||
             (prop === 'date') || (prop === 'time')) {
      // console.log('Date ' + prop);
      document[prop] = dates.parseISODate(document[prop]);
    }
    else if (prop === 'lat') {
      // console.log('Position ' + prop);
      document['position'] = document.lat + ', ' + document.lon;
    }
  }
  return document;
}

function splitToBulk(c8, document) {
  var d = document.date;
  var bulk = [];
  // console.log(JSON.stringify(document));
  bulk.push({index: {_index: c8.type(summaryType)._index, _type: c8._type, _id: d}});
  bulk.push({_id: d, timestamp: d, summary: document.summary, caloriesIdle: document.caloriesIdle, lastUpdate: document.lastUpdate});
  if (document.segments && document.segments.length) {
    for (var i=0; i<document.segments.length; i++) {
      var seg = document.segments[i];
      seg.timestamp = seg.startTime;
      if (seg.type === 'place') {
        // seg.position = seg.place.location.position;
        seg.duration = dates.parseISODate(seg.endTime) - dates.parseISODate(seg.startTime);
        seg.id = seg.timestamp + '-' + seg.place.id;
        c8.type(placeType);
      }
      else {
        c8.type(moveType);
        seg.id = seg.timestamp + '-' + seg.activity;
      }
      bulk.push({index: {_index: c8._index, _type: c8._type, _id: seg.id}});
      bulk.push(seg);
    }
  }
  return bulk;
}

module.exports = adapter;
