var request = require("request");

var adapter = {};

adapter.sensorName = 'expense';

adapter.types = [
  {
    name: 'expense',
    fields: {
      date: 'date',
      c: 'integer',
      category: 'string',
      t: 'float',
      type: 'string',
      cost: 'float'
    }
  }
];

adapter.promptProps = {
  properties: {
    url: {
      description: 'Your expense url'.magenta
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
    return c8.search({
      _source: ['timestamp'],
      size: 1,
      sort: [{'timestamp': 'desc'}],
    }).then(function(response) {
      var resp = c8.trimResults(response);
      var firstDate, lastDate;
      if (opts.firstDate) {
        firstDate = new Date(opts.firstDate);
        console.log('Setting first time to ' + firstDate);
      }
      else if (resp && resp.timestamp) {
        var d = new Date(resp.timestamp);
        firstDate = new Date(d.getTime() + 1);
        console.log('Setting first time to ' + firstDate);
      }
      else {
        console.warn("No previously indexed data, setting first time to 0!");
        firstDate = new Date(0);
      }
      var url = conf.url + '&from=' + firstDate.getDate() + '.' + (firstDate.getMonth() + 1) + '.' + firstDate.getFullYear();
      if (opts.lastDate) {
        var lastDate = opts.lastDate;
        url += '&to=' + lastDate.getDate() + '.' + (lastDate.getMonth() + 1) + '.' + lastDate.getFullYear();
      }
      var cookieJar = request.jar();
      return request({url: url, jar: cookieJar}, function(error, response, body) {
        if (error || !response || !body) {
          reject(new Error('Error getting data: ' + JSON.stringify(response.body)));
        }
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
              console.log(dayData[j].date);
            }
          }
          if (bulk.length > 0) {
            c8.bulk(bulk).then(function(result) {
              if (result.errors) {
                var messages = [];
                for (var i=0; i<result.items.length; i++) {
                  if (result.items[i].index.error) {
                    messages.push(i + ': ' + result.items[i].index.error.reason);
                  }
                }
                reject(new Error(messages.length + ' errors in bulk insert:\n ' + messages.join('\n ')));
                c8.release();
                return;
              }
              fulfill('Indexed ' + result.items.length + ' documents in ' + result.took + ' ms.');
              c8.release();
            }).catch(function(error) {
              reject(error);
              c8.release();
            });
          }
          else {
            fulfill('No data available');
            c8.release();
          }
        }
        else {
          fulfill('No data available');
        }
      });
    }).catch(function(error) {
      reject(error);
      c8.release();
    });
  });
}
module.exports = adapter;
