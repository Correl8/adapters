var request = require("request");
var nopt = require('nopt'),
 noptUsage = require("nopt-usage"),
 knownOpts = {
    'url': [String, null],
    'help': Boolean,
    'init': Boolean,
    'clear': Boolean,
    'start': Date,
    'end': Date
  },
  shortHands = {
    'h': ['--help'],
    'i': ['--init'],
    'c': ['--clear'],
    'u': ['--url'],
    's': ['--start'],
    'e': ['--end']
  },
  description = {
    'url': ' Store the URI of your Expense instance and exit',
    'help': ' Display this usage text and exit',
    'init': ' Create the index and exit',
    'clear': ' Clear all data in the index',
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

var INDEX_BASE = 'correl8';
var sensor = 'expense';
var CONFIG_BASE = 'config-adapter';
var CONFIG_INDEX = 'config';

var firstDate = options['start'] || null;
var lastDate = options['end'] || null;
var apiUrl;
var configIndex = {index: CONFIG_INDEX, type: CONFIG_BASE};

if (options['url']) {
  var params = configIndex;
  params.id = CONFIG_BASE + '-' + sensor;
  params.body = {id: params.id, url: options['url']};
  client.index(params).then(function (response) {
    console.log('Configuration saved.');
  }).catch(function(error) {
    console.warn(error);
  });
  process.exit();
}
else if (options['clear']) {
}
else {
  client.indices.exists({index: CONFIG_INDEX}, function(error, response) {
    if (!response) {
        console.log('Usage: ');
        console.log(noptUsage(knownOpts, shortHands, description));
        // console.log('Configure by ' + process.argv[0] + ' ' + process.argv[1] + ' <expense_url>');
    }
    else {
      getConfig(importData);
    }
  });
}

function getConfig(next) {
  var params = configIndex;
  params.q = 'id:' + CONFIG_BASE + '-' + sensor,
  params.body = {
    fields: ['url'],
    size: 1
  }
  client.search(params, function (error, response) {
    if (error) {
      console.warn("Config search got error: " + JSON.stringify(error));
      return;
    }
    if (response && response.hits && response.hits.hits[0]) {
      apiUrl = response.hits.hits[0].fields.url;
      next();
    }
    else {
      console.log('Usage: ');
      console.log(noptUsage(knownOpts, shortHands, description));
      // console.log('Configure by ' + process.argv[0] + ' ' + process.argv[1] + ' <expense_url>');
    }
  });
}

function importData(next) {
  // console.log('Getting first date...');
  var query = {
    index: INDEX_BASE + '-' + sensor,
    type: sensor,
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
    var url = apiUrl + '&from=' + firstDate.getDay() + '.' + (firstDate.getMonth() + 1) + '.' + firstDate.getFullYear();
    if (lastDate) {
      url += '&to=' + lastDate.getDay() + '.' + (lastDate.getMonth() + 1) + '.' + lastDate.getFullYear();
    }
    var cookieJar = request.jar();
    // console.log(url);
    request({url: url, jar: cookieJar}, function(error, response, body) {
      if (error || !response || !body) {
        // console.warn('Error getting data: ' + JSON.stringify(response.body));
      }
      // console.log(body);
      var data = JSON.parse(body);
      if (data && data.length) {
        var bulk = [];
        for (var i=0; i<data.length; i++) {
          var dayData = data[i];
          for (var j=0; j<dayData.length; j++) {
            var id = dayData[j].date + '-' + dayData[j].t;
            bulk.push({index: {_index: INDEX_BASE + '-' + sensor, _type: sensor, _id: id}});
            dayData[j].id = id;
            dayData[j].timestamp = dayData[j].date;
            bulk.push(dayData[j]);
            // console.log(dayData[j]);
          }
        }
        // console.log(bulk);
        client.bulk(
          {
            index: INDEX_BASE + '-' + sensor,
            type: sensor,
            body: bulk
          },
          function (error, response) {
            if (error) {
              console.warn('ES Error: ' + error);
            }
          }
        );
      }
    });
  });
  if (next) {
    next();
  }
}
