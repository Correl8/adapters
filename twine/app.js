var prompt = require('prompt');
var correl8 = require('correl8');
var nopt = require('nopt');
var noptUsage = require('nopt-usage');
var moment = require('moment');
var Bowline = require('bowline');

var c8 = correl8('twine');

var fields = {
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
};

var knownOpts = {
  'authenticate': Boolean,
  'help': Boolean,
  'init': Boolean,
  'clear': Boolean,
  'start': Date,
  'end': Date
};
var shortHands = {
  'h': ['--help'],
  'i': ['--init'],
  'c': ['--clear'],
  'k': ['--key'],
  'from': ['--start'],
  's': ['--start'],
  'to': ['--end'],
  'e': ['--end']
};
var description = {
  'authenticate': ' Store your Device ID and secret (and exit)',
  'help': ' Display this usage text and exit',
  'init': ' Create the index and exit',
  'clear': ' Clear all data in the index',
  'start': ' Start date as YYYY-MM-DD',
  'end': ' End date as YYYY-MM-DD'
};
var options = nopt(knownOpts, shortHands, process.argv, 2);
var firstDate = options['start'] || null;
var lastDate = options['end'] || new Date();
var conf;

if (options['help']) {
  console.log('Usage: ');
  console.log(noptUsage(knownOpts, shortHands, description));
}
else if (options['authenticate']) {
  var config = {};
  prompt.start();
  prompt.message = '';
  var promptProps = {
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
  }
  prompt.get(promptProps, function (err, result) {
    if (err) {
      console.trace(err);
    }
    else {
      config = result;
      console.log(config);
      c8.config(config).then(function(){
        console.log('Configuration stored.');
      }).catch(function(error) {
        console.trace(error);
      });
    }
  });
}
else if (options['clear']) {
  c8.clear().then(function() {
    console.log('Index cleared.');
    c8.release();
  }).catch(function(error) {
    console.trace(error);
    c8.release();
  });
}
else if (options['init']) {
  c8.init(fields).then(function() {
    console.log('Index initialized.');
  }).catch(function(error) {
    console.trace(error);
    c8.release();
  });
}
else {
  importData();
}

function importData() {
  c8.config().then(function(res) {
    if (res.hits && res.hits.hits && res.hits.hits[0] && res.hits.hits[0]._source['deviceId']) {
      conf = res.hits.hits[0]._source;
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
      var msg = 'Configure first. Run:\nnode ' + process.argv[1] + ' --authenticate\n';
      console.log(msg);
    }
  });
}
