var request = require("request");
var correl8 = require("correl8");
var nopt = require('nopt');
var noptUsage = require("nopt-usage");

var c8 = correl8('expense');
var fields = {
  date: 'date',
  c: 'integer',
  category: 'string',
  t: 'float',
  type: 'string',
  cost: 'float'
};
var apiUrl;

var knownOpts = {
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
    'from': ['--start'],
    's': ['--start'],
    'to': ['--end'],
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
var firstDate = options['start'] || null;
var lastDate = options['end'] || null;

// console.log(options);
if (options['help']) {
  console.log('Usage: ');
  console.log(noptUsage(knownOpts, shortHands, description));
}
else if (options['url']) {
  c8.config({url: options['url']}).then(function(){
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
    if (res.hits && res.hits.hits && res.hits.hits[0] && res.hits.hits[0]._source['url']) {
      // console.log(res.hits.hits[0]);
      apiUrl = res.hits.hits[0]._source['url'];
      // console.log('Url set to ' + apiUrl);
      importData();
    }
    else {
      console.log('Configure first using --url. Usage: ');
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
    var url = apiUrl + '&from=' + firstDate.getDate() + '.' + (firstDate.getMonth() + 1) + '.' + firstDate.getFullYear();
    if (lastDate) {
      url += '&to=' + lastDate.getDate() + '.' + (lastDate.getMonth() + 1) + '.' + lastDate.getFullYear();
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
            bulk.push({index: {_index: c8._index, _type: c8._type, _id: id}});
            dayData[j].id = id;
            dayData[j].timestamp = dayData[j].date;
            bulk.push(dayData[j]);
            console.log(dayData[j]);
          }
        }
        // console.log(bulk);
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
