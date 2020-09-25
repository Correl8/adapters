var request = require('request');

var MAX_DAYS = 31;
var MS_IN_DAY = 24 * 60 * 60 * 1000;

let productivityDescription = {
  "-2": "Very distracting",
  "-1": "Distracting",
  "0": "Neutral",
  "1": "Productive",
  "2": "Very productive"
};

var adapter = {};

adapter.sensorName = 'rescuetime';

adapter.types = [
  {
    name: 'rescuetime',
    fields: {
      "@timestamp": "date",
      "ecs": {
        "version": 'keyword'
      },
      "event": {
        "created": "date",
        "dataset": "keyword",
        "duration": "long",
        "end": "date",
        "module": "keyword",
        "original": "keyword",
        "start": "date",
        // "timezone": "keyword"
      },
      "date_details": {
        "year": 'long',
        "month": 'long',
        "month_name": 'keyword',
        "week_number": 'long',
        "day_of_year": 'long',
        "day_of_month": 'long',
        "day_of_week": 'keyword',
      },
      "time_slice": {
        "start_hour": 'long',
        "id": 'long',
        "name": 'keyword',
      },
      "rescuetime": {
        "spent": 'long',
        "people": 'long',
        "activity": 'keyword',
        "category": 'keyword',
        "productivity": {
          "id": 'integer',
          "description": 'keyword'
        }
      }
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
      _source: ['@timestamp'],
      size: 1,
      sort: [{'@timestamp': 'desc'}],
    }).then(function(response) {
      let resp = c8.trimResults(response);
      if (opts.firstDate) {
        firstDate = new Date(opts.firstDate);
        console.log('Setting first time to ' + firstDate);
      }
      else if (resp && resp['@timestamp']) {
        let d = new Date(resp['@timestamp']);
        firstDate = new Date(d.getTime() + 1);
        console.log('Setting first time to ' + firstDate);
      }
      else {
        let now = new Date();
        firstDate = new Date(now.getTime() - (MAX_DAYS * MS_IN_DAY));
        console.warn('No previously indexed data, setting first time to ' + firstDate);
      }
      if (opts.lastDate) {
        lastDate = new Date(opts.lastDate);
      }
      else {
        lastDate = new Date();
      }
      if (lastDate.getTime() >= (firstDate.getTime() + (MAX_DAYS * MS_IN_DAY))) {
        lastDate.setTime(firstDate.getTime() + ((MAX_DAYS - 1) * MS_IN_DAY));
        console.warn('Max date range %d days, setting lastDate to %s', MAX_DAYS, lastDate);
      }
      let url = 'https://www.rescuetime.com/anapi/data?key=' + conf.key +
        '&format=json&op=select&pv=interval&rs=minute' +
        '&restrict_begin=' + firstDate.toISOString().substring(0, 10) +
        '&restrict_end=' + lastDate.toISOString().substring(0, 10);
      let cookieJar = request.jar();
      // console.log(url);
      request({url: url, jar: cookieJar}, function(error, response, body) {
        if (error || !response || !body) {
          // console.warn('Error getting data: ' + JSON.stringify(response.body));
        }
        // console.log(body);
        let obj = JSON.parse(body);
        // console.log(obj);
        let data = obj.rows;
        if (data && data.length) {
          let bulk = [];
          for (var i=0; i<data.length; i++) {
            let id = data[i][0] + '-' + data[i][3]; // unique enough?
            let d = new Date(data[i][0]);
            // let tz = new Date(data[i][0]).getTimezoneOffset();
            // console.log(tz);
            // d.setTime(d.getTime() + (tz * 60 * 1000));
            let dString = new Date(d).toISOString();
            let startHour = d.getHours();
            let startMinute = d.getMinutes();
            let sliceName = [startHour, startMinute].join(':');
            if (startMinute == 5) {
              sliceName = [startHour, '0' + startMinute].join(':');
            }
            else if (startMinute == 0) {
              sliceName += '0';
            }
            let idTime = startHour + startMinute/60;
            let sliceId = Math.round((idTime + (idTime >= 4 ? -4 : 20)) * 12);
            bulk.push({index: {_index: c8._index, _id: id}});
            bulk.push({
              "@timestamp": dString,
              "ecs": {
                "version": '1.0.1'
              },
              "event": {
                "created": new Date(),
                "dataset": "rescuetime.anapi",
                "duration": data[i][1] * 1E9, // seconds to nanos
                "end": new Date(d.getTime() + data[i][1] * 1000),
                "module": "rescuetime",
                "original": JSON.stringify(data[i]),
                "start": d,
              },
              "time_slice": {
                "start_hour": startHour,
                "id": sliceId,
                "name": sliceName,
              },
              "rescuetime": {
                "spent": data[i][1],
                "people": data[i][2],
                "activity": data[i][3],
                "category": data[i][4],
                "productivity": {
                  "id": data[i][5],
                  "description": productivityDescription[data[i][5]]
                }
              }
            });
            console.log(dString);
          }
          if (bulk.length > 0) {
            // console.log(bulk);
            c8.bulk(bulk).then(function(response) {
              let result = c8.trimBulkResults(response);
              if (result.errors) {
                let messages = [];
                for (var i=0; i<result.items.length; i++) {
                  if (result.items[i].index.error) {
                    messages.push(i + ': ' + result.items[i].index.error.reason);
                  }
                }
                reject(new Error(messages.length + ' errors in bulk insert:\n ' + messages.join('\n ')));
              }
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
