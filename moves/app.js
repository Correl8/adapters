var request = require('request');
var prompt = require('prompt');
var express = require('express');
var correl8 = require('correl8');
var nopt = require('nopt');
var noptUsage = require('nopt-usage');
var dates = require('moves-date');
var Moves = require('moves');
var moves;

var c8 = correl8('moves');
var defaultPort = 3000;
var defaultUrl = 'http://localhost:' + defaultPort + '/';
var MAX_DAYS = 50;
var summaryIndex = 'moves-summary';
var summaryType = 'moves-summary';
var moveIndex = 'moves-move';
var moveType = 'moves-move';
var placeIndex = 'moves-place';
var placeType = 'moves-place';

var placeFields = {
  type: 'string',
  startTime: 'date',
  endTime: 'date',
  place: {
    id: 'integer',
    type: 'string',
    location: {
      lat: 'float',
      lon: 'float',
      position: 'geo_point'
    }
  },
  'lastUpdate': 'date'
};
var moveFields = {
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
};
var summaryFields = {
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
};

var knownOpts = {
  'authenticate': Boolean,
  'help': Boolean,
  'init': Boolean,
  'clear': Boolean,
  'start': Date,
  'end': Date
};
var shortHands = {
  'h': ['--help'],
  'i': ['--init'],
  'c': ['--clear'],
  'k': ['--key'],
  'from': ['--start'],
  's': ['--start'],
  'to': ['--end'],
  'e': ['--end']
};
var description = {
  'authenticate': ' Store your Moves API credentials and exit',
  'help': ' Display this usage text and exit',
  'init': ' Create the index and exit',
  'clear': ' Clear all data in the index',
  'start': ' Start date as YYYY-MM-DD',
  'end': ' End date as YYYY-MM-DD'
};
var options = nopt(knownOpts, shortHands, process.argv, 1);
var firstDate = options['start'] || null;
var lastDate = options['end'] || new Date();
var conf;

// console.log(options);
if (options['help']) {
  console.log('Usage: ');
  console.log(noptUsage(knownOpts, shortHands, description));
}
else if (options['authenticate']) {
  var app = express();
  app.all('/', function (req, res) {
    if (req.query && req.query.code) {
      c8.config().then(function(result) {
        if (result.hits && result.hits.hits && result.hits.hits[0] && result.hits.hits[0]._source['client_id']) {
          moves = new Moves(result.hits.hits[0]._source);
          moves.token(req.query.code, function(error, response, body) {
            if (error) {
              console.trace(error);
              res.json(error);
              return;
            }
            var rb = JSON.parse(response.body);
            if (rb) {
              if (rb.error) {
                console.trace(rb.error);
                res.json(rb.error);
                return;
              }
              c8.config(rb).then(function(){
                res.send('Access token saved.');
                console.log('Access token saved.');
                c8.release();
                process.exit;
              });
            }
          });
        }
      });
    }
    else {
      res.send('Waiting for code in query string...');
    }
  });
  var server = app.listen(defaultPort, function () {
    var host = server.address().address;
    var port = server.address().port;
    // console.log('Temporary Moves auth server listening at http://%s:%s', host, port);
  });
  var config = {};
  prompt.start();
  prompt.message = '';
  var promptProps = {
    properties: {
      client_id: {
        description: 'Enter your Moves client ID'.magenta
      },
      client_secret: {
        description: 'Enter your Moves client secret'.magenta
      },
      redirect_uri: {
        description: 'Enter your redirect URL (' + defaultUrl+ ')'.magenta
      },
    }
  }
  prompt.get(promptProps, function (err, result) {
    if (err) {
      console.trace(err);
    }
    else {
      config = result;
      console.log(config);
      c8.config(config).then(function(){
        console.log('Configuration stored.');
      }).catch(function(error) {
        console.trace(error);
      });
      moves = new Moves(config);
      var auth_url = moves.authorize({
        scope: ['activity', 'location']
      });
      console.log('Thanks! Now open ' + auth_url + ' in your browser!');
    }
  });
}
else if (options['clear']) {
  c8.type(summaryType).clear().then(function(res) {
    c8.type(placeType).clear().then(function(res) {
      c8.type(moveType).clear().then(function(res) {
        console.log('Index cleared.');
        c8.release();
      });
    });
  }).catch(function(error) {
    console.trace(error);
    c8.release();
  });
}
else if (options['init']) {
  c8.type(summaryType).init(summaryFields).then(function() {
    return c8.type(placeType).init(placeFields);
  }).then(function() {
    return c8.type(moveType).init(moveFields);
  }).then(function(res) {
    console.log('Index initialized.');
    // c8.release();
  }).catch(function(error) {
    console.trace(error);
    c8.release();
  });
}
else {
  c8.config().then(function(res) {
    if (res.hits && res.hits.hits && res.hits.hits[0] && res.hits.hits[0]._source['client_id']) {
      conf = res.hits.hits[0]._source;
      moves = new Moves({
        client_id: conf.client_id,
        client_secret: conf.client_secret,
        redirect_uri: conf.redirect_uri
      });
      c8.type(summaryType).search({
        fields: ['timestamp'],
        size: 1,
        sort: [{'timestamp': 'desc'}],
      }).then(function(response) {
        if (firstDate) {
          console.log('Setting first time to ' + firstDate);
          importData(firstDate);
        }
        else if (response && response.hits && response.hits.hits && response.hits.hits[0] && response.hits.hits[0].fields && response.hits.hits[0].fields.timestamp) {
          console.log('Setting first time to ' + response.hits.hits[0].fields.timestamp);
          firstDate = new Date(response.hits.hits[0].fields.timestamp);
          importData(firstDate);
        }
        else {
          moves.get('/user/profile', conf.access_token, function(error, response, body) {
            if (error) {
              console.trace(error);
              return;
            }
            else if (response.body === 'expired_access_token') {
              refreshToken(conf);
            }
            else {
              var rb = JSON.parse(response.body);
              if (!rb || rb.error) {
                console.trace(rb.error);
                refreshToken(conf);
                return;
              }
              else {
                var user = JSON.parse(body);
                // console.log(user);
                // console.log('Setting first time to Moves date ' + user.profile.firstDate);
                firstDate = dates.parseISODate(user.profile.firstDate);
                importData(firstDate);
              }
            }
          });
        }
      }).catch(function(error) {
        if (error.body && error.body.error && error.body.error.type &&
            error.body.error.type === 'index_not_found_exception') {
          console.log('Init first with --init');
          console.trace(error);
        }
        else {
          console.trace(error);
          var msg = 'Configure first. Go to https://dev.moves-app.com/apps\n' +
          'Create a new app for Correl8. Then run:\nnode ' + process.argv[1] + ' --authenticate';
          console.log(msg);
        }
        c8.release();
      });
    }
    else {
      var msg = 'Configure first. Go to https://dev.moves-app.com/apps\n' +
      'Create a new app for Correl8. Then run:\nnode ' + process.argv[1] + ' --authenticate';
      console.log(msg);
      c8.release();
    }
  });
}

function importData(firstDate) {
  if (!firstDate) {
    console.warn('No starting date...');
    return;
  }
  var startTime = firstDate;
  // startTime.setDate(startTime.getDate() + 1);
  var now = dates.day(new Date());
  var fromDate = dates.day(startTime);
  if (fromDate === now) {
    console.log('Todays data already exists! Try again tomorrow...');
  }
  var dayCount = 0;
  var doneCount = 0;
  while (fromDate < now) {
    toDate = fromDate;
    moves.get('/user/storyline/daily?from=' + fromDate + '&to=' + toDate + '&trackPoints=true', conf.access_token, function(error, response, body) {
      if (error) {
        console.warn(error);
        return;
      }
      else if (response.body === 'expired_access_token') {
        refreshToken();
        return;
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
          refreshToken(getHistory);
        }
        else {
          var document = JSON.parse(body)[0];
          console.log(document.date);
          var bulk = splitToBulk(prepareForElastic(document));
          c8.bulk(bulk).then(function(result) {
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

function refreshToken(next) {
  console.log('Refreshing token...');
  moves.refresh_token(refresh_token, function(error, response, body) {
    if (error) {
      console.warn('Refresh got error: ' + error);
      return;
    }
    // console.log(body); // should store!
    var rb = JSON.parse(response.body);
    // console.log(rb);
    if (!rb || rb.error) {
      if (rb.error === 'invalid_grant') {
        console.trace(rb.error);
        // getToken(next);
      }
      console.trace(rb.error);
      // reauthorize();
    }
    else {
      console.log('Token refreshed. Try again!')
      // next(); // possible infinite loop!
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

function splitToBulk(document) {
  var d = document.date;
  var bulk = [];
  bulk.push({index: {_index: c8.type(summaryType)._index, _type: c8._type}});
  bulk.push({timestamp: d, summary: document.summary, caloriesIdle: document.caloriesIdle, lastUpdate: document.lastUpdate});
  if (document.segments && document.segments.length) {
    for (var i=0; i<document.segments.length; i++) {
      var seg = document.segments[i];
      seg.timestamp = seg.startTime;
      if (seg.type === 'place') {
        seg.position = seg.place.location.position;
      }
      c8.type(seg.type == 'place' ? placeType : moveType);
      bulk.push({index: {_index: c8._index, _type: c8._type}});
      bulk.push(seg);
    }
  }
  return bulk;
}
