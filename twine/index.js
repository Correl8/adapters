var Bowline = require('bowline');

var adapter = {};

adapter.sensorName = 'twine';

adapter.types = [
  {
    name: 'twine',
    fields: {
      meta: {
        sensor: 'string',
        battery: 'string',
        wifiSignal: 'string'
      },
      time: {
        age: 'float',
        timestamp: 'integer'
      },
      values: {
        batteryVoltage: 'integer',
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
  if (conf.deviceId) {
    var client = new Bowline(conf);
    client.fetch(function(err, response){
      if (err) {
        console.trace(err);
        return;
      }
      response._id = response.time.timestamp;
      response.meta.sensor = conf.deviceId;
      response.timestamp = new Date(response.time.timestamp * 1000);
      console.log(response.timestamp);
      c8.insert(response).then(function(result) {
        // console.log(result);
        // console.log('Indexed ' + result.items.length + ' documents in ' + result.took + ' ms.');
      }).catch(function(error) {
        console.trace(error);
      });
    });
  }
  else {
    console.log('Configure first.');
  }
};

module.exports = adapter;
