var elasticsearch = require('elasticsearch');
var client = new elasticsearch.Client({
  host: 'localhost:9200',
  log: 'warning'
});
var dates = require('moves-date');

var INDEX_NAME = 'correl8';
var MAX_DAYS = 50;

var Moves = require('moves')
    , moves = new Moves({
          client_id: 'HpYChgFUH73KMV7JH9G56uxmIID9BYgw'
        , client_secret: 'oezIhI8UDolYvN4wHkqSGks32mKCWoE9812MJqE6ASTd6PW6Uf_XCHSKtI96jLaE'
        , redirect_uri: 'http://correl8.me/'
      });

var code = '4FgA0nS7S55o8cEu29JcaFQYnKKO6nfZfe62TRz8Hhn1HsCxIsQD6a70TeJs0D22';

var access_token = '0vgxjY7OGNA781UQyIfDvfdtblYHW0i55781x97N22n6AJCLBO4u6tnJ0ZzSb4L4';
var refresh_token = 'lIS__2UBrynJENyWRo_Bx44QfzP369rpRF1nclW5TK07wIEKZR47TK6F8SOridho';
var expires_in;
var firstDate;

if (!code) {
  reauthorize();
}

if (access_token) {
  getFirstDate(getHistory);
}
else {
  getToken(function() {getFirstDate(history); });
}
// process.exit();

function reauthorize() {
  console.log('Authorizing...');
  var auth_url = moves.authorize({
       scope: ['activity', 'location'] //can contain either activity, location or both
     // , state: 'my_state' //optional state as per oauth
   });
  console.log('Go to ' + auth_url);
  process.exit();
}

function getToken(next) {
  console.log('Getting token...');
  moves.token(code, function(error, response, body) {
    if (error) {
      console.warn("getToken got error: " + error);
      return;
    }
    else {
      // console.log("getToken got " + response.body);
    }
    var rb = JSON.parse(response.body);
    // console.log(rb);
    if (!rb || rb.error) {
      console.warn("getToken got error: " + rb.error);
      reauthorize();
      return;
    }
    access_token = rb.access_token
      , refresh_token = rb.refresh_token
      , expires_in = rb.expires_in;
      console.log('NEW ACCESS TOKEN: ' + access_token);
      console.log('NEW REFRESH TOKEN: ' + refresh_token);
    next();
  });
}

function refreshToken(next) {
  console.log('Refreshing token...');
  moves.refresh_token(refresh_token, function(error, response, body) {
    if (error) {
      console.warn("Refresh got error: " + error);
      return;
    }
    // console.log(body); // should store!
    var rb = JSON.parse(response.body);
    // console.log(rb);
    if (!rb || rb.error) {
      if (rb.error === 'invalid_grant') {
        getToken(next);
      }
      console.warn("Refresh got error: " + rb.error);
      reauthorize();
    }
    else {
      next(); // possible infinite loop!
    }
  });
}

function getFirstDate(next) {
  // console.log('Getting first date...');
  var query = {
    index: INDEX_NAME + '-moves-summary',
    type: 'moves-summary',
    body: {
      fields: ['timestamp'],
      size: 1,
      sort: [{'timestamp': 'desc'}],
    }
  };
  client.search(query, function (error, response) {
    if (error) {
      console.warn("search got error: " + JSON.stringify(error));
      return;
    }
    if (response && response.hits && response.hits.hits && response.hits.hits[0] && response.hits.hits[0].fields && response.hits.hits[0].fields.timestamp) {
      console.log("Setting first time to " + response.hits.hits[0].fields.timestamp);
      firstDate = new Date(response.hits.hits[0].fields.timestamp);
      next();
    }
    else {
      console.log("No timestamp found in index! Getting first date from Moves...");
      moves.get('/user/profile', access_token, function(error, response, body) {
        if (error) {
          console.warn("profile got error: " + error);
          reuturn;
        }
        else if (response.body === 'expired_access_token') {
          refreshToken(getFirstDate);
        }
        else {
          var rb = JSON.parse(response.body);
          if (!rb || rb.error) {
            console.warn("profile got error: " + rb.error);
            refreshToken(getFirstDate);
            return;
          }
          else {
            var user = JSON.parse(body);
            // console.log(user);
            // console.log("Setting first time to Moves date " + user.profile.firstDate);
            firstDate = dates.parseISODate(user.profile.firstDate);
            next();
          }
        }
      });
    }
  });
}

function getHistory() {
  if (!firstDate) {
    console.warn("No starting date...");
    return;
  }
  var startTime = firstDate;
  // startTime.setDate(startTime.getDate() + 1);
  var now = dates.day(new Date());
  var fromDate = dates.day(startTime);
  if (fromDate === now) {
    console.log("Todays data already exists! Try again tomorrow...");
  }
  var dayCount = 0;
  var doneCount = 0;
  while (fromDate < now) {
    toDate = fromDate;
    // console.log('Retrieving history from ' + fromDate + ' to ' + toDate + '...');
    // moves.get('/user/storyline/daily?pastDays=1&trackPoints=true', access_token, function(error, response, body) {
    moves.get('/user/storyline/daily?from=' + fromDate + '&to=' + toDate + '&trackPoints=true', access_token, function(error, response, body) {
      if (error) {
        console.warn(error);
        return;
      }
      else if (response.body === 'expired_access_token') {
        refreshToken(getHistory);
        return;
      }
      else if (!response.body) {
        console.warn('No response body in history!');
        // console.log(response.headers);
        return;
      }
      else if (response.body.substr(0, 1) != '[') {
        console.warn('Invalid response body in history: ' + response.body)
        return;
      }
      else {
        // console.log(response.body);
        var rb = JSON.parse(response.body);
        // console.log(rb);
        // console.log(JSON.stringify(body));
        if (!rb || rb.error) {
          refreshToken(getHistory);
        }
        else {
          var document = JSON.parse(body)[0];
          console.log(document.date);
          var bulk = splitToBulk(prepareForElastic(document));
          // console.log(JSON.stringify(bulk));
          client.bulk(
            {
              index: INDEX_NAME + '-moves',
              type: "moves",
              body: bulk
            },
            function (error, response) {
              if (error) {
                console.warn("ES Error: " + error);
              }
              // console.log(response);
              // console.log('Done ' + doneCount++);
            }
          );
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
  bulk.push({index: {_index: INDEX_NAME + '-moves-summary', _type: 'moves-summary'}});
  bulk.push({timestamp: d, summary: document.summary, caloriesIdle: document.caloriesIdle, lastUpdate: document.lastUpdate});
  if (document.segments && document.segments.length) {
    for (var i=0; i<document.segments.length; i++) {
      var seg = document.segments[i];
      seg.timestamp = seg.startTime;
      if (seg.type === 'place') {
        seg.position = seg.place.location.position;
      }
      seg.type = 'moves-' + seg.type;
      bulk.push({index: {_index: INDEX_NAME + '-' + seg.type, _type: seg.type}});
      bulk.push(seg);
    }
  }
  return bulk;
}
