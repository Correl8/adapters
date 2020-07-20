var request = require("request");

// if no start date is given as an argument,
// fetch data from last DEFAULT_DAYS days
var DEFAULT_DAYS = 30;

var adapter = {};

adapter.sensorName = 'expense';

adapter.types = [
  {
    name: 'expense',
    fields: {
      "@timestamp": 'date',
      "ecs": {
        "version": 'keyword'
      },
      "event": {
        "created": "date",
        "module": "keyword",
        "original": "keyword",
        "start": "date",
      },
      "expense": {
        "category": {
          "id": 'long',
          "name": 'keyword'
        },
        "type": {
          "id": 'float',
          "name": 'keyword'
        },
        "cost": 'float'
      }
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
    if (opts.firstDate) {
      firstDate = new Date(opts.firstDate);
      console.log('Setting first time to ' + firstDate);
    }
    else {
      var d = new Date();
      firstDate = new Date(d.getTime() - DEFAULT_DAYS * 24 * 60 * 60 * 1000);
      console.log('Setting first time to ' + firstDate);
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
      var dates = JSON.parse(body);
      if (dates && dates.length) {
        var bulk = [];
        for (var i=0; i<dates.length; i++) {
          var dayData = dates[i];
          var dayCost = 0;
          for (var j=0; j<dayData.length; j++) {
            var id = dayData[j].date + '-' + dayData[j].t;
            let data = {
              "@timestamp": dayData[j].date,
              "event": {
                "created": new Date(),
                "module": "expense",
                "original": JSON.stringify(dayData[j]),
                "start": dayData[j].date,
              },
              "expense": {
                "category": {
                  "id": dayData[j].c,
                  "name": dayData[j].category,
                },
                "type": {
                  "id": dayData[j].t,
                  "name": dayData[j].type,
                },
                "cost": dayData[j].cost
              }
            };
            bulk.push({index: {_index: c8._index, _type: c8._type, _id: id}});
            bulk.push(data);
            dayCost += dayData[j].cost;
          }
          console.log(dayData[0].date + ': ' + dayCost);
        }
        if (bulk.length > 0) {
          c8.bulk(bulk).then(function(res) {
            let result = c8.trimResults(res);
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
  });
}
module.exports = adapter;
