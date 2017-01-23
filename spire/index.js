var Spire = require('spire-tracker');

var MAX_DAYS = 7;
var MS_IN_DAY = 24 * 60 * 60 * 1000;

var adapter = {};

adapter.sensorName = 'spire';

var streakIndex = 'spire-streaks';
var eventIndex = 'spire-events';

adapter.types = [
  {
    name: streakIndex,
    fields: {
      timestamp: "date",
      type: "string",
      original_type: "string",
      modified_type: "string",
      modified: "boolean",
      comment: "text",
      start_at: "date",
      stop_at: "date",
      value: "date",
      sub_value: "date",
      calm_duration: "integer",
      focus_duration: "integer",
      tense_duration: "integer",
      activity_duration: "integer",
      sedentary_duration: "integer",
      disconnected_duration: "integer",
      charging_duration: "integer",
      not_worn_duration: "integer",
      neutral_duration: "integer",
      duration: "integer"
    }
  },
  {
    name: eventIndex,
    fields: {
      timestamp: "date",
      br: "float",
      steps: "integer",
      calories: "float"
    }
  }
];

adapter.promptProps = {
  properties: {
    access_token: {
      description: 'Enter your access token'.magenta
    }
  }
};

adapter.storeConfig = function(c8, result) {
  var conf = result;
  return c8.config(conf).then(function(){
    console.log('Configuration stored.');
  }).catch(function(error) {
    console.trace(error);
  });
};

adapter.importData = function(c8, conf, opts) {

/*
 // ignore dates, just fetch latest day
    return new Promise(function (fulfill, reject){
    var client = new Spire(conf.access_token);

    client.events().then(function(data) {
      var bulk = [];
      for (var i=0; i<data.length; i++) {
        var obj = data[i];
        var id = obj.timestamp + '-' + 'events';
        obj.timestamp = new Date(obj.timestamp * 1000);
        bulk.push({index: {_index: c8.type(eventIndex)._index, _type: c8._type, _id: id}});
        bulk.push(obj);
      }
      if (bulk.length > 0) {
        return c8.bulk(bulk).then(function(result) {
          console.log('Indexed ' + result.items.length + ' events in ' + result.took + ' ms.');
        }).catch(function(error) {
          console.trace(error);
        });
      }
    });

    client.streaks().then(function(data) {
      var streakBulk = [];
      for (var i=0; i<data.length; i++) {
        var obj = data[i];
        var id = obj.start_at + '-' + 'streak';
        obj.duration = obj.stop_at - obj.start_at;
        obj[obj.type + '_duration'] = obj.value;
        if (obj.type == 'activity') {
          obj.steps = obj.sub_value;
        }
        else if (obj.sub_value) {
          obj.br = obj.sub_value;
        }
        obj.start_at = new Date(obj.start_at * 1000);
        obj.stop_at = new Date(obj.stop_at * 1000);
        obj.timestamp = obj.start_at;
        streakBulk.push({index: {_index: c8.type(streakIndex)._index, _type: c8._type, _id: id}});
        streakBulk.push(obj);
      }
      if (streakBulk.length > 0) {
        c8.bulk(streakBulk).then(function(result) {
          console.log('Indexed ' + result.items.length + ' streak documents in ' + result.took + ' ms.');
          fulfill(result);
        }).catch(function(error) {
          reject(error);
        });
      }
    });
  });

*/

  return new Promise(function (fulfill, reject){
    c8.type(eventIndex).search({
      _source: ['timestamp'],
      size: 1,
      sort: [{'timestamp': 'desc'}],
    }).then(function(response) {
      c8.search({
        _source: ['timestamp'],
        size: 1,
        sort: [{'timestamp': 'desc'}],
      }).then(function(response) {
        var resp = c8.trimResults(response);
        var firstDate;
        if (opts.firstDate) {
          firstDate = new Date(opts.firstDate);
          console.log('Setting date to given: ' + firstDate);
        }
        else if (resp && resp.timestamp) {
          var d = new Date(resp.timestamp);
          firstDate = new Date();
          firstDate.setTime(d.getTime());
          console.log('Setting date to last known: ' + firstDate);
        }
        var lastDate = new Date();

        var client = new Spire(conf.access_token);
        var firstStreak = new Date();


        client.streaks().then(function(data) {
          var streakBulk = [];
          for (var i=0; i<data.length; i++) {
            var obj = data[i];
            var id = obj.start_at + '-' + 'streak';
            obj.duration = obj.stop_at - obj.start_at;
            obj[obj.type + '_duration'] = obj.value;
            if (obj.type == 'activity') {
              obj.steps = obj.sub_value;
            }
            else if (obj.sub_value) {
              obj.br = obj.sub_value;
            }
            obj.start_at = new Date(obj.start_at * 1000);
            obj.stop_at = new Date(obj.stop_at * 1000);
            obj.timestamp = obj.start_at;
            if (obj.timestamp.getTime() < firstStreak.getTime()) {
              firstStreak = obj.timestamp;
            }
            else {
              // console.log(i + ': ' + obj.timestamp + '>' + firstStreak);
            }
            streakBulk.push({index: {_index: c8.type(streakIndex)._index, _type: c8._type, _id: id}});
            streakBulk.push(obj);
          }

          if (!firstDate) {
            firstDate = firstStreak;
            console.log('Setting date to first available: ' + firstDate);
          }

          if (opts.lastDate) {
            lastDate = new Date(opts.lastDate);
          }
          if (lastDate - firstDate > MAX_DAYS * MS_IN_DAY) {
            lastDate.setTime(firstDate.getTime() + MAX_DAYS * MS_IN_DAY);
          }
          console.log('Setting end date to ' + lastDate);

          var date = firstDate;
          while (date < lastDate) {
            console.log(date);
            client.events(date).then(function(data) {
              var bulk = [];
              for (var i=0; i<data.length; i++) {
                var obj = data[i];
                var id = obj.timestamp + '-' + 'events';
                obj.timestamp = new Date(obj.timestamp * 1000);
                bulk.push({index: {_index: c8.type(eventIndex)._index, _type: c8._type, _id: id}});
                bulk.push(obj);
              }
              if (bulk.length > 0) {
                c8.bulk(bulk).then(function(result) {
                  console.log('Indexed ' + result.items.length + ' events in ' + result.took + ' ms.');
                }).catch(function(error) {
                  reject(error);
                });
              }
              else {
                fulfill('No events');
              }
            });
            date.setTime(date.getTime() + MS_IN_DAY);
          }


          if (streakBulk.length > 0) {
            c8.bulk(streakBulk).then(function(result) {
              fulfill('Indexed ' + result.items.length + ' streak documents in ' + result.took + ' ms.');
            }).catch(function(error) {
              reject(error);
            });
          }
        });

      });
    }).catch(function(error) {
      console.trace(error);
    });
  });
};

module.exports = adapter;
