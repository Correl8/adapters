var movesApi = require('moves-api').MovesApi;
var dates = require('moves-date');
var express = require('express');
var moves;

var defaultPort = 3321;
var defaultUrl = 'http://localhost:' + defaultPort + '/';
// var MAX_DAYS = 50;
var MAX_DAYS = 6;
var MS_IN_DAY = 24 * 60 * 60 * 1000;

var adapter = {};

adapter.sensorName = 'moves';

var summaryType = 'moves-summary';
var moveType = 'moves-move';
var placeType = 'moves-place';

adapter.types = [
  {
    name: summaryType,
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
    name: moveType,
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
    name: placeType,
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
    clientId: {
      description: 'Enter your Moves client ID'.magenta
    },
    clientSecret: {
      description: 'Enter your Moves client secret'.magenta
    },
    redirectUri: {
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

  moves = new movesApi(result);
  var authUrl = moves.generateAuthUrl();
  console.log('Authorize the app by opening the followin url in your browser:\n ' + authUrl);
  app.all('/', function (req, res) {
    if (req.query && req.query.code) {
      moves.getAccessToken(req.query.code, function(error, authData) {
        if (error) {
          console.trace(error);
          res.json(error);
          return;
        }
        conf.accessToken = authData.access_token;
        conf.refreshToken = authData.refresh_token;
        server.close();
        return c8.config(conf).then(function(){
          res.send('Access token saved.');
          console.log('Configuration stored.');
          c8.release();
          process.exit();
        }).catch(function(error) {
          console.trace(error);
        });
      });
    }
    else {
      res.send('Waiting for code in query string...');
    }
  });
}

adapter.importData = function(c8, conf, opts) {
  moves = new movesApi(conf);
  return c8.type(summaryType).search({
    _source: ['timestamp'],
    size: 1,
    sort: [{'timestamp': 'desc'}],
  }).then(function(response) {
    // console.log('Getting first date...');
    return c8.search({
      _source: ['timestamp'],
      size: 1,
      sort: [{'timestamp': 'desc'}],
    }).then(function(response) {
      var resp = c8.trimResults(response);
      var firstDate = new Date();
      var lastDate = opts.lastDate || new Date();
      var goneAsync = false;
      firstDate.setTime(lastDate.getTime() - (MAX_DAYS * MS_IN_DAY));
      if (opts.firstDate) {
        firstDate = new Date(opts.firstDate);
        // console.log('Setting first time to argument ' + firstDate);
      }
      else if (resp && resp.timestamp) {
        var d = new Date(resp.timestamp);
        // refetch yesterday for updated summary
        firstDate.setTime(d.getTime());
        // console.log('Setting first time to latest stored date ' + firstDate);
      }
      else {
        goneAsync = true;
        return moves.getProfile(function(error, user) {
          if (error) {
            console.trace(error);
            return;
          }
          // console.log('Setting first time to Moves profile date ' + user.profile.firstDate);
          firstDate = dates.parseISODate(user.profile.firstDate);
          return importData(c8, conf, firstDate, lastDate);
        });
      }
      if (!goneAsync) {
        return importData(c8, conf, firstDate, lastDate);
      }
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
  if (lastDate.getTime() > (firstDate.getTime() + MAX_DAYS * MS_IN_DAY)) {
    lastDate.setTime(firstDate.getTime() + MAX_DAYS * MS_IN_DAY);
    // console.warn('Time range of ' + MAX_DAYS + ' days exceeded. Setting last date to ' + lastDate);
  }
  var startTime = firstDate;
  // startTime.setDate(startTime.getDate() + 1);
  var fromDate = dates.day(startTime);
  var toDate = dates.day(lastDate);
  // var end = dates.day(lastDate);
  if (fromDate == dates.day(new Date())) {
    console.log('Today\'s data already exists! Try again tomorrow...');
  }
  var opts = {from: fromDate, to: toDate, trackPoints: true};
  // console.log(opts);
  moves.getStoryline(opts, function(error, documents) {
    if (error) {
      console.warn(error);
      return;
    }
    var bulk = [];
    for (var i=0; i<documents.length; i++) {
      var document = documents[i];
      var prepared = splitToBulk(c8, prepareForElastic(document));
      console.log(document.date + ': ' + prepared.length/2 + ' documents');
      bulk = bulk.concat(prepared);
    }
    // for (var i=0; i<bulk.length; i++) {
    //   console.log(JSON.stringify(bulk[i]));
    // }
    return c8.bulk(bulk).then(function(result) {
      console.log('Indexed ' + result.items.length + ' documents in ' + result.took + ' ms.');
    }).catch(function(error) {
      console.trace(error);
    });
  });
}

function refreshToken(c8, conf) {
  moves.refreshToken(conf.refreshToken, function(error, authData) {
    if (error) {
      console.warn('Refresh got error: ' + error);
      return;
    }
    conf.accessToken = authData.access_token;
    c8.conf(conf).then(function() {
      console.log('Token refreshed. Try again!')
    });
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
  if (document.summary && document.summary.length) {
    for (var i=0; i<document.summary.length; i++) {
/*
      var doc = document.summary[i];
      doc.timestamp = d;
      doc.caloriesIdle = document.caloriesIdle,
      doc.lastUpdate = document.lastUpdate
      bulk.push({index: {_index: c8.type(summaryType)._index, _type: c8._type, _id: d}});
      bulk.push(doc);
*/
      var id = d.getTime() + '-' + i;
      bulk.push({index: {_index: c8.type(summaryType)._index, _type: c8._type, _id: id}});
	bulk.push({timestamp: d, summary: document.summary[i], caloriesIdle: document.caloriesIdle, lastUpdate: dates.parseISODate(document.lastUpdate)});
    }
  }
  if (document.segments && document.segments.length) {
    for (var i=0; i<document.segments.length; i++) {
      var seg = document.segments[i];
      seg.timestamp = seg.startTime;
      seg.lastUpdate = dates.parseISODate(seg.lastUpdate)
      if (seg.type == 'place') {
        // seg.position = seg.place.location.position;
        seg.duration = dates.parseISODate(seg.endTime) - dates.parseISODate(seg.startTime);
        seg.id = seg.timestamp + '-' + seg.place.id;
        c8.type(placeType);
      }
      else {
        c8.type(moveType);
        seg.id = seg.timestamp + '-' + seg.activity;
      }
      // console.log(JSON.stringify(seg));
      bulk.push({index: {_index: c8._index, _type: c8._type, _id: seg.id}});
      bulk.push(seg);
    }
  }
  return bulk;
}

module.exports = adapter;
