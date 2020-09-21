var Bowline = require('bowline');

var adapter = {};

adapter.sensorName = 'twine';

adapter.types = [
  {
    id: 'string',
    name: 'twine',
    fields: {
      timestamp: 'date',
      celcius: 'float',
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
  // console.log("1");
  return new Promise(function (fulfill, reject){
    // console.log("2");
    if (conf.deviceId) {
      // console.log("3");
      let twine = new Bowline(conf);
      twine.fetch(function(err, response){
        // console.log("4");
        if (err) {
          reject(err);
          return;
        }
        response.id = response.time.timestamp + '-' + conf.deviceId;
        response.meta.sensor = conf.deviceId;
        response.timestamp = new Date(response.time.timestamp * 1000);
        response.measureTime = response.time;
        response.values.isVibrating = response.values.isVibrating ? true : false;
        response.celcius = (response.values.temperature - 32) * 5 / 9;
        delete(response.time);
        // console.log("5");
        c8.insert(response).then(function(result) {
          // console.log("6");
          // console.log(result);
          // console.log(response.timestamp + ': ' + result.result);
          // console.log('Indexed ' + result.items.length + ' documents in ' + result.took + ' ms.');
          fulfill(response.timestamp + ': ' + result.result)
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
