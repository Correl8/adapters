var nordpool = require("nordpool");
var prices = new nordpool.Prices()

var MS_IN_DAY = 24 * 60 * 60 * 1000;

var adapter = {};

adapter.sensorName = 'nordpool-price-hourly';

adapter.types = [
  {
    name: 'nordpool-price-hourly',
    fields: {
      timestamp: 'date',
      area: 'string',
      value: 'float'
    }
  }
];

adapter.promptProps = {
  properties: {
    area: {
      description: 'Area'.magenta,
      default: 'ALL'
    },
    currency: {
      description: 'Currency'.magenta,
      default: 'EUR'
    }
  }
};

adapter.storeConfig = function(c8, result) {
  return c8.config(result).then(function(){
    console.log('Configuration stored.');
    c8.release();
  });
}

adapter.importData = function(c8, conf, opts) {
  return new Promise(function (fulfill, reject){
    var firstDate, lastDate;
    c8.search({
      _source: ['timestamp'],
      size: 1,
      sort: [{'timestamp': 'desc'}],
    }).then(function(response) {
      var resp = c8.trimResults(response);
      console.log('Getting last date...');
      if (opts.firstDate) {
        firstDate = opts.firstDate;
        if (typeof(firstDate) != 'Date') {
          firstDate = new Date(firstDate);
          console.log("Setting firstDate to " + firstDate.toISOString());
        }
      }
      else if (opts.lastDate) {
        lastDate = opts.lastDate;
        if (typeof(lastDate) != 'Date') {
          lastDate = new Date(lastDate);
          console.log("Setting lastDate to " + lastDate.toISOString());
        }
      }
      else if (resp && resp.timestamp) {
          firstDate = new Date(resp.timestamp);
        console.log("Setting firstDate to last found " + firstDate.toISOString());
      }
      else {
        console.warn("No previously indexed data, setting lastDate to today!");
        lastDate = new Date();
      }
      var params = {area: conf.area, currency: conf.currency, to: lastDate};
      if (firstDate) {
        params.from = firstDate;
      }
      // console.log(params);
      prices.hourly(params, function(error, data) {
        if (error) {
          reject(error);
          return;
        }
        // console.log(JSON.stringify(data, null, 2));
        if (data && data.length) {
          var bulk = [];
          for (var i=0; i<data.length; i++) {
            var row = data[i];
            var ts = row.date.format();
            var id = 'price-hourly-' + ts + '-' + row.area;
            var values = {area: row.area, value: row.value, timestamp: ts};
            bulk.push({index: {_index: c8._index, _type: c8._type, _id: id}});
            bulk.push(values);
            console.log(id + ': ' + row.value);
          }
          if (bulk.length > 0) {
            c8.bulk(bulk).then(function(response) {
              let result = c8.trimBulkResults(response);
              if (result.errors) {
                var messages = [];
                for (var i=0; i<result.items.length; i++) {
                  if (result.items[i].index.error) {
                    messages.push(i + ': ' + result.items[i].index.error.reason);
                  }
                }
                reject(new Error(messages.length + ' errors in bulk insert:\n ' + messages.join('\n ')));
              }
              fulfill('Indexed ' + result.items.length + ' documents in ' + result.took + ' ms.');
            }).catch(function(error) {
              reject(error);
              bulk = null;
            });
          }
          else {
            fulfill('No data available');
          }
        }
        else {
          fulfill('No data available');
          // fulfill(JSON.stringify(data.Rows, null, 2));
        }
      });
    }).catch(function(error) {
      reject(error);
    });
  });
}
module.exports = adapter;
