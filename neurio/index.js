var neurio = require('neurio');

var adapter = {};

var MS_IN_DAY = 24 * 60 * 60 * 1000;
var MAX_DAYS = 10;
var MAX_PAGES = 5;
var EVENTS_PER_PAGE = 100;
var MIN_POWER = 400;
// maximum granularity is 5 minutes
var GRANULARITY = 'minutes';
var FREQUENCY = 5;
var lastConsumptionEnergy;

adapter.sensorName = 'neurio';

adapter.types = [
  {
    name: 'neurio-appliances',
    fields: {
      appliance: {
        label: 'string',
        name: 'string',
        locationId: 'string',
        tags: 'string',
        createdAt: 'date',
        updatedAt: 'date',
        id: 'string'
      },
      status: 'string',
      start: 'date',
      end: 'date',
      duration: 'integer',
      energy: 'integer',
      averagePower: 'integer',
      guesses: {
        heater: 'float',
        air_conditioner: 'float',
        toaster: 'float'
      },
      groupIds: 'string',
      lastCycle: {
        groupId: 'string',
        start: 'date',
        end: 'date',
        energy: 'integer',
        averagePower: 'integer',
        createdAt: 'date',
        updatedAt: 'date',
        sensorId: 'string',
        id: 'string'
      },
      cycleCount: 'integer',
      isConfirmed: 'boolean',
      id: 'string'
    },
  },
  {
    name: 'neurio-samples',
    fields: {
      timestamp: 'date',
      cumulativeConsumptionEnergy: 'long',
      consumptionEnergy: 'long',
      consumptionPower: 'long',
      generationEnergy: 'long',
      generationPower: 'long'
    }
  },
  {
    name: 'neurio-energy',
    fields: {
      timestamp: 'date',
      start: 'date',
      end: 'date',
      duration: 'integer',
      consumptionEnergy: 'long',
      generationEnergy: 'long'
    }
  }
];

adapter.promptProps = {
  properties: {
    clientId: {
      description: 'Enter your Neurio client ID'.magenta
    },
    clientSecret: {
      description: 'Enter your Neurio client secret'.magenta
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
    var auth = neurio.Auth;
    var lastDate = new Date();
    var firstDate = new Date(lastDate.getTime() - (MS_IN_DAY));
    auth.simple(conf.clientId, conf.clientSecret).then(function (client) {
      client.user().then(function(user) {
        if (user && user.locations && user.locations.length) {
          c8.type(adapter.types[2].name).search({
            _source: ['timestamp', 'cumulativeConsumptionEnergy'],
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
              lastConsumptionEnergy = resp.cumulativeConsumptionEnergy;
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
            if (lastDate.getTime() >= (firstDate.getTime() + MS_IN_DAY)) {
              lastDate = new Date(
                firstDate.getFullYear(),
                firstDate.getMonth(),
                firstDate.getDate() + 1,
                firstDate.getHours(),
                firstDate.getMinutes(),
                firstDate.getSeconds(),
                firstDate.getMilliseconds()-1
              );
              console.warn('Max time range 24 hours, setting end time to ' + lastDate);
            }
            for (var i=0; i<user.locations.length; i++) {
              var locId = user.locations[i].id;
              getAppliancePage(c8, client, locId, firstDate, lastDate, MIN_POWER, EVENTS_PER_PAGE, 1);
              for (var j=0; j<user.locations[i].sensors.length; j++) {
                var sensorId = user.locations[i].sensors[j].id;
                getSamplesHistoryPage(c8, client, sensorId, firstDate, lastDate, GRANULARITY, FREQUENCY, EVENTS_PER_PAGE, 1);
                getEnergyStats(c8, client, sensorId, firstDate, lastDate, GRANULARITY, FREQUENCY);
              }
            }
          }).catch(function(error) {
            reject(error);
          });
        }
      }).catch(function(error) {
        reject(error);
      });
    }).catch(function(error) {
      reject(error);
    });
  });
};

function getAppliancePage(c8, client, locId, firstDate, lastDate, minPower, perPage, page) {
  var applianceType = adapter.types[0].name;
  client.applianceEvents(locId, firstDate, lastDate, minPower, perPage, page).then(function(events) {
  // client.applianceEventsRecent(locId, firstDate).then(function(events) {
    // console.log(events);
    if (!events || !events.length) {
      return;
    }
    var bulk = [];
    for (var j=0; j<events.length; j++) {
      var values = events[j];
      values.timestamp = new Date(values.start);
      values.duration = (new Date(values.end).getTime() - new Date(values.start).getTime())/1000;
      var logStr = values.timestamp.toISOString();
      if (values.appliance.label) {
        logStr += ' ' + values.appliance.label;
      }
      console.log(logStr);
      var id = values.id;
      bulk.push({index: {_index: c8.type(applianceType)._index, _type: c8.type(applianceType)._type, _id: id}});
      bulk.push(values);
    }
    c8.type(applianceType).bulk(bulk).then(function(result) {
      console.log('Indexed ' + result.items.length + ' appliance events in ' + result.took + ' ms.');
      bulk = null;
      if (events.length == perPage) {
        // page was full, request the next page (recursion!)
        page++;
        if (page <= MAX_PAGES) {
          getAppliancePage(c8, client, locId, firstDate, lastDate, minPower, perPage, page);
        }
      }
    }).catch(function(error) {
      console.trace(error);
      bulk = null;
    });
  }).catch(function(error) {
    console.trace(error);
  });
}

function getSamplesHistoryPage(c8, client, sensorId, start, end, granularity, frequency, perPage, page) {
  var sampleType = adapter.types[1].name;
  client.historySamples(sensorId, start, end, granularity, frequency, perPage, page).then(function (events) {
    var bulk = [];
    for (var i=0; i<events.length; i++) {
      var values = events[i];
      if (!lastConsumptionEnergy) {
        lastConsumptionEnergy = values.consumptionEnergy;
        // console.log(values.timestamp + ' - skipping');
        continue; // will lose some power readings?
        values.cumulativeConsumptionEnergy = null;
        values.consumptionEnergy = null;
      }
      values.cumulativeConsumptionEnergy = values.consumptionEnergy;
      values.consumptionEnergy = values.consumptionEnergy - lastConsumptionEnergy;
      lastConsumptionEnergy = values.cumulativeConsumptionEnergy;
      var power = values.consumptionPower || 0;
      if (values.generationPower) {
        power -= values.generationPower;
      }
      var energy = values.consumptionEnergy || 0;
      if (values.generationEnergy) {
        power -= values.generationEnergy;
      }
      var logStr = values.timestamp + ' ' + power + ' W, ' + energy + ' Ws';
      console.log(logStr);
      var id = values.timestamp;
      bulk.push({index: {_index: c8.type(sampleType)._index, _type: c8.type(sampleType)._type, _id: id}});
      bulk.push(values);
      // console.log(values);
    }
    c8.type(sampleType).bulk(bulk).then(function(result) {
      console.log('Indexed ' + result.items.length + ' power readings in ' + result.took + ' ms.');
      bulk = null;
      if (events.length == perPage) {
        // page was full, request the next page (recursion!)
        page++;
        if (page <= MAX_PAGES) {
          getSamplesHistoryPage(c8, client, sensorId, start, end, granularity, frequency, perPage, page);
        }
      }
    }).catch(function(error) {
      console.trace(error);
      bulk = null;
    });
  }).catch(function(error) {
    console.trace(error);
  });
}

function getEnergyStats(c8, client, sensorId, start, end, granularity, frequency) {
  var energyType = adapter.types[2].name;
  client.stats(sensorId, start, end, granularity, frequency).then(function (stats) {
    var bulk = [];
    for (var i=0; i<stats.length; i++) {
      var values = stats[i];
      values.timestamp = new Date(values.start);
      values.duration = (new Date(values.end).getTime() - new Date(values.start).getTime())/1000;
      var consumption = values.consumptionEnergy || 0;
      if (values.generationEnergy) {
        consumption -= values.generationEnergy;
      }
      // console.log(values);
      // console.log(consumption);
      var logStr = values.timestamp.toISOString() + ' ' + consumption + ' Ws';
      console.log(logStr);
      var id = values.timestamp;
      bulk.push({index: {_index: c8.type(energyType)._index, _type: c8.type(energyType)._type, _id: id}});
      bulk.push(values);
    }
    c8.type(energyType).bulk(bulk).then(function(result) {
      console.log('Indexed ' + result.items.length + ' energy stats in ' + result.took + ' ms.');
      bulk = null;
    }).catch(function(error) {
      console.trace(error);
      bulk = null;
    });
  }).catch(function(error) {
    console.trace(error);
  });
}

module.exports = adapter;
