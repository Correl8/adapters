var request = require("request");
var nopt = require('nopt'),
 noptUsage = require("nopt-usage"),
 knownOpts = {
    'key': [String, null],
    'help': Boolean,
    'start': Date,
    'end': Date
  },
  shortHands = {
    'h': ['--help'],
    'k': ['--key'],
    's': ['--start'],
    'e': ['--end']
  },
  description = {
    'key': ' Store your RescueTime API key and exit',
    'help': ' Display this usage text and exit',
    'start': ' Start date as YYYY-MM-DD',
    'end': ' End date as YYYY-MM-DD'
  },
  options = nopt(knownOpts, shortHands, process.argv, 2);

// console.log(options);
if (options['help']) {
  console.log('Usage: ');
  console.log(noptUsage(knownOpts, shortHands, description));
  process.exit();
}

var elasticsearch = require('elasticsearch');
var client = new elasticsearch.Client({
  host: 'localhost:9200',
  log: 'warning'
});

var INDEX_NAME = 'correl8';

var firstDate = options['start'] || null;
var lastDate = options['end'] || new Date();
var apiKey;

// var withValues = ['', 'alone', 'partner', 'parent', 'kids', 'family', 'others'];
var configIndex = {index: 'config', type: 'config-adapter'};

if (options['key']) {
  var params = configIndex;
  params.id = 'config-adapter-rescuetime';
  params.body = {id: params.id, apiKey: options['key']};
  client.index(params, function (error, response) {
    if (error) {
      console.warn(error);
      res.json(error);
      return;
    }
    console.log('Configuration saved.');
    process.exit();
  });
}
else {
  client.indices.exists({index: 'config'}, function(error, response) {
    if (!response) {
      console.log('Usage: ');
      console.log(noptUsage(knownOpts, shortHands, description));
      // console.log('Configure by ' + process.argv[0] + ' ' + process.argv[1] + ' <api key>');
    }
    else {
      getConfig(importData);
    }
  });
}

function getConfig(next) {
  var params = configIndex;
  params.q = 'id:config-adapter-rescuetime',
  params.body = {
    fields: ['apiKey'],
    size: 1
  }
  client.search(params, function (error, response) {
    if (error) {
      console.warn("Config search got error: " + JSON.stringify(error));
      return;
    }
    if (response && response.hits && response.hits.hits[0] && response.hits.hits[0].fields && response.hits.hits[0].fields.apiKey) {
      apiKey = response.hits.hits[0].fields.apiKey;
      next();
    }
    else {
      // console.log(response.hits);
      console.log('Usage: ');
      console.log(noptUsage(knownOpts, shortHands, description));
      // console.log('Configure by ' + process.argv[0] + ' ' + process.argv[1] + ' <api key>');
      return;
    }
  });
}

function importData(next) {
  // console.log('Getting first date...');
  var query = {
    index: INDEX_NAME + '-rescuetime',
    type: 'rescuetime',
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
    if (firstDate) {
      console.log("Setting first time to " + firstDate);
    }
    else if (response && response.hits && response.hits.hits && response.hits.hits[0] && response.hits.hits[0].fields && response.hits.hits[0].fields.timestamp) {
      console.log("Setting first time to " + response.hits.hits[0].fields.timestamp);
      firstDate = new Date(response.hits.hits[0].fields.timestamp);
    }
    else {
      console.warn("No previously indexed data, setting first time to 1!");
      firstDate = new Date(0);
    }
    var url = 'https://www.rescuetime.com/anapi/data?key=' + apiKey +
      '&format=json&op=select&pv=interval&rs=minute' +
      '&restrict_begin=' + firstDate.toISOString().substring(0, 10) +
      '&restrict_end=' + lastDate.toISOString().substring(0, 10);
    console.log(url);
    request(url, function(error, response, body) {
      if (error || !response || !body) {
        // console.warn('Error getting data: ' + JSON.stringify(response.body));
      }
      var obj = JSON.parse(body);
      if (!obj) {
        return;
      }
      var data = obj.rows;
      if (data && data.length) {
        var bulk = [];
        for (var i=0; i<data.length; i++) {
          bulk.push({index: {_index: INDEX_NAME + '-rescuetime', _type: 'rescuetime'}});
          bulk.push({
            timestamp: data[i][0],
            spent: data[i][1],
            people: data[i][2],
            activity: data[i][3],
            category: data[i][4],
            productivity: data[i][5]
          });
          console.log(data[i][0]);
        }
        // console.log(bulk);
        client.bulk(
          {
            index: INDEX_NAME + '-rescuetime',
            type: 'rescuetime',
            body: bulk
          },
          function (error, response) {
            if (error) {
              console.warn('ES Error: ' + error);
            }
            // console.log(response);
            // console.log('Done ' + doneCount++);
          }
        );
      }
    });
  });
  if (next) {
    next();
  }
}
