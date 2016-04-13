var request = require("request");
var correl8 = require('correl8');
var nopt = require('nopt');
var nordpool = require('nordpool');
var noptUsage = require('nopt-usage');

var prices = new nordpool.Prices()

var type = 'nordpool';
var c8 = correl8(type);

var MAX_DAYS = 7;
var MS_IN_DAY = 24 * 60 * 60 * 1000;

// move to a separate module!
var apiUrl = 'http://www.nordpoolspot.com/api/marketdata/page/35?currency=,EUR,EUR,EUR';

var fields = {
  timestamp: 'date',
  price: 'float'
};

var knownOpts = {
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
  'help': ' Display this usage text and exit',
  'init': ' Create the index and exit',
  'clear': ' Clear all data in the index',
  'start': ' Start date as YYYY-MM-DD',
  'end': ' End date as YYYY-MM-DD'
};
var options = nopt(knownOpts, shortHands, process.argv, 2);
var firstDate = options['start'] || null;
var lastDate = options['end'] || null;
var conf;

if (options['help']) {
  console.log('Usage: ');
  console.log(noptUsage(knownOpts, shortHands, description));
}
else if (options['clear']) {
  c8.clear().then(function() {
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
  c8.search({
    fields: ['timestamp'],
    size: 1,
    sort: [{'timestamp': 'desc'}],
  }).then(function(response) {
    if (firstDate) {
      console.log("Setting first time to " + firstDate);
    }
    else if (response && response.hits && response.hits.hits && response.hits.hits[0] && response.hits.hits[0].fields && response.hits.hits[0].fields.timestamp) {
      firstDate = new Date(response.hits.hits[0].fields.timestamp);
      console.log("Setting first time to " + firstDate.toISOString());
    }
    else {
      console.warn("No previously indexed data, setting first time to today!");
      firstDate = new Date();
    }
    var url = apiUrl + '&startDate=' + fmtDate(firstDate);
    if (lastDate) {
      if (typeof(lastDate) != 'Date') {
        lastDate = new Date(lastDate);
      }
      if (lastDate.getTime() > firstDate.getTime() + (MAX_DAYS * MS_IN_DAY)) {
        lastDate.setTime(firstDate.getTime() + (MAX_DAYS * MS_IN_DAY));
      }
    }
    else {
      lastDate = new Date(firstDate.getTime() + (MAX_DAYS * MS_IN_DAY));
    }
    prices.weekly({date: lastDate}, function(error, data) {
      if (error || !data) {
        console.warn('Error getting data: ' + JSON.stringify(error));
      }
      // console.log(JSON.stringify(data, null, 2));
      if (data && data.length) {
        var bulk = [];
        for (var i=0; i<data.length; i++) {
          var row = data[i];
          var id = 'price-hourly-' + row.date + '-' + row.area;
          var values = {timestamp: row.date, price: row.value};
          bulk.push({index: {_index: c8._index, _type: c8._type, _id: id}});
          bulk.push(values);
          // console.log(id + ': ' + row.value);
        }
        c8.bulk(bulk).then(function(result) {
          console.log('Indexed ' + result.items.length + ' documents in ' + result.took + ' ms.');
          bulk = null;
        });
      }
      else {
        console.log(JSON.stringify(data.Rows, null, 2));
      }
    });
  });
}

function fmtDate(d) {
  var date = d.getDate();
  var month = d.getMonth() + 1;
  var year = d.getFullYear();
  if (date < 10) {
    date = '0' + '' + date.toString();
  }
  if (month < 10) {
    month = '0' + '' + month.toString();
  }
  return date + '-' + month + '-' + year;
}

function isValidDate(d) {
  if (Object.prototype.toString.call(d) !== "[object Date]" ) {
    return false;
  }
  return !isNaN(d.getTime());
}
