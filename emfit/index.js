var QS = require('emfit-qs');

var MAX_DAYS = 30;
var MS_IN_DAY = 24 * 60 * 60 * 1000;

var adapter = {};

adapter.sensorName = 'emfit-qs';

var precenseIndex = 'emfit-qs';

adapter.types = [
  {
    name: precenseIndex,
    fields: {
      "timestamp": "date",
    }
  }
];

adapter.promptProps = {
  properties: {
    username: {
      description: 'Enter your username'.magenta
    },
    password: {
      description: 'Enter your password'.magenta
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
  c8.type(precenseIndex).search({
    _source: ['timestamp'],
    size: 1,
    sort: [{'timestamp': 'desc'}],
  }).then(function(response) {
    // console.log('Getting first date...');
    return c8.search({
      _source: ['timestamp'],
      size: 1,
      sort: [{'timestamp': 'desc'}],
    }).then(function(response) {
      var resp = c8.trimResults(response);
      var firstDate = new Date();
      var lastDate = opts.lastDate || new Date();
      firstDate.setTime(lastDate.getTime() - (MAX_DAYS * MS_IN_DAY));
      if (opts.firstDate) {
        firstDate = new Date(opts.firstDate);
        console.log('Setting first time to ' + firstDate);
      }
      else if (resp && resp.timestamp) {
        var d = new Date(resp.timestamp);
        firstDate.setTime(d.getTime() + 1);
        console.log('Setting first time to ' + firstDate);
      }
      if (lastDate.getTime() > (firstDate.getTime() + MAX_DAYS * MS_IN_DAY)) {
        lastDate.setTime(firstDate.getTime() + MAX_DAYS * MS_IN_DAY);
        console.warn('Setting last date to ' + lastDate);
      }
      importData(c8, conf, firstDate, lastDate);
    });
  }).catch(function(error) {
    console.trace(error);
  });
}

function importData(c8, conf, firstDate, lastDate) {
  var qs = new QS()
  qs.login(conf.username, conf.password).then(function(data) {
    var deviceId = data.device_settings[0].device_id
    qs.latest(deviceId).then(function(latest) {
      console.log(JSON.stringify(latest, null, 2))
      var bulk = [];
      // bulk.push({index: {_index: c8._index, _type: c8._type, _id: id}});
      // bulk.push(obj[i]);
      if (bulk.length > 0) {
        return c8.bulk(bulk).then(function(result) {
          console.log('Indexed ' + result.items.length + ' documents in ' + result.took + ' ms.');
        }).catch(function(error) {
          console.trace(error);
        });
      }
    })
  })
}

module.exports = adapter;
