var request = require('request');
var prompt = require('prompt');
var correl8 = require('correl8');
var nopt = require('nopt');
var noptUsage = require('nopt-usage');
var neurio = require('neurio');
var moment = require('moment');

var c8 = correl8('neurio');
var defaultPort = 3000;
var defaultUrl = 'http://localhost:' + defaultPort + '/';
var MAX_DAYS = 10;
// var MAX_PAGES = 100000;
// var MAX_PAGES = 470;
var MAX_PAGES = 2;
var EVENTS_PER_PAGE = 100;

var fields = {
  appliance: {
    label: 'string',
    name: 'string',
    locationId: 'string',
    tags: 'text',
    createdAt: 'date',
    updatedAt: 'date',
    id: 'string'
  },
  status: 'string',
  start: 'date',
  end: 'date',
  duration: 'integer',
  energy: 'integer',
  averagePower: 'integer',
  guesses: {
    heater: 'float',
    air_conditioner: 'float',
    toaster: 'float'
  },
  groupIds: 'string',
  lastCycle: {
    groupId: 'string',
    start: 'date',
    end: 'date',
    energy: 'integer',
    averagePower: 'integer',
    createdAt: 'date',
    updatedAt: 'date',
    sensorId: 'string',
    id: 'string'
  },
  cycleCount: 'integer',
  isConfirmed: 'boolean',
  id: 'string'
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
  'authenticate': ' Store your Neurio API credentials and exit',
  'help': ' Display this usage text and exit',
  'init': ' Create the index and exit',
  'clear': ' Clear all data in the index',
  'start': ' Start date as YYYY-MM-DD',
  'end': ' End date as YYYY-MM-DD'
};
var options = nopt(knownOpts, shortHands, process.argv, 2);
var firstDate = options['start'] || null;
var lastDate = options['end'] || new Date();
var conf;

if (options['help']) {
  console.log('Usage: ');
  console.log(noptUsage(knownOpts, shortHands, description));
}
else if (options['authenticate']) {
  var config = {};
  prompt.start();
  prompt.message = '';
  var promptProps = {
    properties: {
      clientId: {
        description: 'Enter your Neurio client ID'.magenta
      },
      clientSecret: {
        description: 'Enter your Neurio client secret'.magenta
      }
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
    }
  });
}
else if (options['clear']) {
  c8.clear().then(function(res) {
    console.log('Index cleared.');
    c8.release();
  }).catch(function(error) {
    console.trace(error);
    c8.release();
  });
}
else if (options['init']) {
  c8.init(fields).then(function() {
    console.log('Index initialized.');
  }).catch(function(error) {
    console.trace(error);
    c8.release();
  });
}
else {
  importData();
}

function importData() {

  c8.config().then(function(res) {
    if (res.hits && res.hits.hits && res.hits.hits[0] && res.hits.hits[0]._source['clientId']) {
      conf = res.hits.hits[0]._source;
      var auth = neurio.Auth;
      auth.simple(conf.clientId, conf.clientSecret).then(function (client) {
        client.user().then(function(user) {
          if (user && user.locations && user.locations.length) {
            c8.search({
              fields: ['timestamp'],
              size: 1,
              sort: [{'timestamp': 'desc'}],
            }).then(function(response) {
              if (firstDate) {
                console.log('Setting first time to ' + firstDate);
              }
              else if (response && response.hits && response.hits.hits && response.hits.hits[0] && response.hits.hits[0].fields && response.hits.hits[0].fields.timestamp) {
                var d = new Date(response.hits.hits[0].fields.timestamp);
                firstDate = new Date(d.getTime() + 1);
                console.log('Setting first time to ' + firstDate);
              }
              else {
                firstDate = new Date(user.createdAt);
                console.warn('No previously indexed data, setting first time to ' + firstDate);
              }
              if (lastDate.getTime() >= (firstDate.getTime() + (24 * 60 * 60 * 1000))) {
                lastDate = new Date(
                  firstDate.getFullYear(),
                  firstDate.getMonth(),
                  firstDate.getDate() + 1,
                  firstDate.getHours(),
                  firstDate.getMinutes(),
                  firstDate.getSeconds(),
                  firstDate.getMilliseconds()-1
                );
                console.warn('Max time range 24 hours, setting end time to ' + lastDate);
              }
              for (var i=0; i<user.locations.length; i++) {
                var locId = user.locations[i].id;
                getPage(client, locId, firstDate, lastDate, null, EVENTS_PER_PAGE, 1);
              }
            }).catch(function(error) {
              console.trace(error);
            });
          }
        }).catch(function(error) {
          console.trace(error);
        });
      }).catch(function(error) {
        console.trace(error);
      });
    }
  });
}

function getPage(client, locId, firstDate, lastDate, minPower, perPage, page) {
  client.applianceEvents(locId, firstDate, lastDate, minPower, perPage, page).then(function(events) {
    // console.log(events);
    if (!events || !events.length) {
      return;
    }
    var bulk = [];
    for (var j=0; j<events.length; j++) {
      var values = events[j];
      values.timestamp = values.start;
      values.duration = values.end - values.start;
      var logStr = values.timestamp;
      if (values.appliance.label) {
        logStr += ' ' + values.appliance.label;
      }
      console.log(logStr);
      var id = values.id;
      bulk.push({index: {_index: c8._index, _type: c8._type, _id: id}});
      bulk.push(values);
    }
    c8.bulk(bulk).then(function(result) {
      console.log('Indexed ' + result.items.length + ' documents in ' + result.took + ' ms.');
      bulk = null;
      if (events.length == perPage) {
        // page was full, request the next page (recursion!)
        page++;
        if (page <= MAX_PAGES) {
          getPage(client, locId, firstDate, lastDate, minPower, perPage, page);
        }
      }
    }).catch(function(error) {
      console.trace(error);
      bulk = null;
    });
  }).catch(function(error) {
    console.trace(error);
  });
}
