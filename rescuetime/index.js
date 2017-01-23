var request = require('request');

var MAX_DAYS = 30;
var MS_IN_DAY = 24 * 60 * 60 * 1000;

var adapter = {};

adapter.sensorName = 'rescuetime';

adapter.types = [
  {
    name: 'rescuetime',
    fields: {
      timestamp: 'date',
      spent: 'integer',
      people: 'integer',
      activity: 'string',
      category: 'string',
      productivity: 'integer'
    }
  }
];

adapter.promptProps = {
  properties: {
    key: {
      description: 'Enter your RescueTime API Key'.magenta
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
    c8.search({
      _source: ['timestamp'],
      size: 1,
      sort: [{'timestamp': 'desc'}],
    }).then(function(response) {
      var resp = c8.trimResults(response);
      if (opts.firstDate) {
        firstDate = new Date(opts.firstDate);
        console.log('Setting first time to ' + firstDate);
      }
      else if (resp && resp.timestamp) {
        var d = new Date(resp.timestamp);
        lastConsumptionEnery = resp.cumulativeConsumptionEnergy;
        firstDate = new Date(d.getTime() + 1);
        console.log('Setting first time to ' + firstDate);
      }
      else {
        firstDate = new Date(user.createdAt);
        console.warn('No previously indexed data, setting first time to ' + firstDate);
      }
      if (opts.lastDate) {
        lastDate = new Date(opts.lastDate);
      }
      else {
        lastDate = new Date();
      }
      if (lastDate.getTime() >= (firstDate.getTime() + (MAX_DAYS * MS_IN_DAY))) {
        lastDate.setTime(firstDate.getTime() + (MAX_DAYS * MS_IN_DAY));
        console.warn('Max date range %d days, setting lastDate to %s', MAX_DAYS, lastDate);
      }
      var url = 'https://www.rescuetime.com/anapi/data?key=' + conf.key +
        '&format=json&op=select&pv=interval&rs=minute' +
        '&restrict_begin=' + firstDate.toISOString().substring(0, 10) +
        '&restrict_end=' + lastDate.toISOString().substring(0, 10);
      var cookieJar = request.jar();
      console.log(url);
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
            var tz = new Date().getTimezoneOffset();
            var d = new Date(data[i][0]).getTime() + (tz * 60 * 1000);
            var dString = new Date(d).toISOString();
            bulk.push({index: {_index: c8._index, _type: c8._type, _id: id}});
            bulk.push({
              timestamp: dString,
              spent: data[i][1],
              people: data[i][2],
              activity: data[i][3],
              category: data[i][4],
              productivity: data[i][5]
            });
            console.log(dString);
          }
          if (bulk.length > 0) {
            c8.bulk(bulk).then(function(result) {
              fulfill('Indexed ' + result.items.length + ' documents in ' + result.took + ' ms.');
              c8.release();
            }).catch(function(error) {
              reject(error);
              c8.release();
            });
          }
          else {
            fulfill('No data to import.');
          }
        }
      });
    }).catch(function(error) {
      reject(error);
      c8.release();
    });
  });
};

module.exports = adapter;
