var request = require("request");
var correl8 = require("correl8");
var nopt = require('nopt');
var noptUsage = require("nopt-usage");

var c8 = correl8('rescuetime');
var fields = {
  date: 'date',
  spent: 'integer',
  people: 'integer',
  activity: 'string',
  category: 'string',
  productivity: 'integer'
};
var apiKey;

var knownOpts = {
    'key': [String, null],
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
    'k': ['--key'],
    'from': ['--start'],
    's': ['--start'],
    'to': ['--end'],
    'e': ['--end']
  },
  description = {
    'key': ' Store your RescueTime API key and exit',
    'help': ' Display this usage text and exit',
    'init': ' Create the index and exit',
    'clear': ' Clear all data in the index',
    'start': ' Start date as YYYY-MM-DD',
    'end': ' End date as YYYY-MM-DD'
  },
  options = nopt(knownOpts, shortHands, process.argv, 2);
var firstDate = options['start'] || null;
var lastDate = options['end'] || new Date();

// console.log(options);
if (options['help']) {
  console.log('Usage: ');
  console.log(noptUsage(knownOpts, shortHands, description));
}
else if (options['key']) {
  c8.config({key: options['key']}).then(function(){
    console.log('Configuration stored.');
    c8.release();
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
  c8.init(fields).then(function(res) {
    console.log('Index initialized.');
    c8.release();
  }).catch(function(error) {
    console.trace(error);
    c8.release();
  });
}
else {
  c8.config().then(function(res) {
    if (res.hits && res.hits.hits && res.hits.hits[0] && res.hits.hits[0]._source['key']) {
      apiKey = res.hits.hits[0]._source['key'];
      importData();
    }
    else {
      console.log('Configure first using --key. Usage: ');
      console.log(noptUsage(knownOpts, shortHands, description));
      c8.release();
    }
  });
}

function importData() {
  // console.log('Getting first date...');
  c8.search({
    fields: ['timestamp'],
    size: 1,
    sort: [{'timestamp': 'desc'}],
  }).then(function(response) {
    if (firstDate) {
      console.log("Setting first time to " + firstDate);
    }
    else if (response && response.hits && response.hits.hits && response.hits.hits[0] && response.hits.hits[0].fields && response.hits.hits[0].fields.timestamp) {
      console.log("Setting first time to " + response.hits.hits[0].fields.timestamp);
      firstDate = new Date(response.hits.hits[0].fields.timestamp);
    }
    else {
      console.warn("No previously indexed data, setting first time to 0!");
      firstDate = new Date(0);
    }
    var url = 'https://www.rescuetime.com/anapi/data?key=' + apiKey +
      '&format=json&op=select&pv=interval&rs=minute' +
      '&restrict_begin=' + firstDate.toISOString().substring(0, 10) +
      '&restrict_end=' + lastDate.toISOString().substring(0, 10);
    var cookieJar = request.jar();
    // console.log(url);
    request({url: url, jar: cookieJar}, function(error, response, body) {
      if (error || !response || !body) {
        // console.warn('Error getting data: ' + JSON.stringify(response.body));
      }
      // console.log(body);
      var obj = JSON.parse(body);
      var data = obj.rows;
      if (data && data.length) {
        var bulk = [];
        for (var i=0; i<data.length; i++) {
          var id = data[i][0] + '-' + data[i][3]; // unique enough?
          bulk.push({index: {_index: c8.index(), _type: c8.type(), _id: id}});
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
        c8.bulk(bulk).then(function(result) {
          console.log('Indexed ' + result.items.length + ' documents in ' + result.took + ' ms.');
          c8.release();
        }).catch(function(error) {
          console.trace(error);
          c8.release();
        });
      }
    });
  });
}
