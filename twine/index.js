var Bowline = require('bowline');

var adapter = {};

adapter.sensorName = 'twine';

adapter.types = [
  {
    id: 'string',
    name: 'twine',
    fields: {
      timestamp: 'date',
      meta: {
        sensor: 'string',
        battery: 'string',
        wifiSignal: 'string'
      },
      measureTime: {
        age: 'float',
        timestamp: 'integer'
      },
      values: {
        batteryVoltage: 'float',
        firmwareVersion: 'string',
        isVibrating: 'boolean',
        orientation: 'string',
        temperature: 'integer',
        updateMode: 'string',
        vibration: 'integer'
      }
    }
  }
];

adapter.promptProps = {
  properties: {
    email: {
      description: 'Enter your Twine account (e-mail address)'.magenta
    },
    password: {
      description: 'Enter your password'.magenta
    },
    deviceId: {
      description: 'Enter your Twine device ID'.magenta
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
    if (conf.deviceId) {
      var client = new Bowline(conf);
      return client.fetch(function(err, response){
        if (err) {
          console.trace(err);
          return;
        }
        response.id = response.time.timestamp + '-' + conf.deviceId;
        response.meta.sensor = conf.deviceId;
        response.timestamp = new Date(response.time.timestamp * 1000);
        response.measureTime = response.time;
        response.isVibrating = response.isVibrating ? true : false;
        delete(response.time);
        c8.insert(response).then(function(result) {
          // console.log(result);
          console.log(response.timestamp + ': ' + result.result);
          // console.log('Indexed ' + result.items.length + ' documents in ' + result.took + ' ms.');
        }).catch(function(error) {
          reject(error);
        });
      });
    }
    else {
      reject('Configure first.');
    }
  });
};

module.exports = adapter;
