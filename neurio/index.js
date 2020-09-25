const neurio = require('neurio');
const moment = require('moment');

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
    "name": 'neurio-appliances',
    "fields": {
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
        "month": {
          "number": 'long',
          "name": 'keyword',
        },
        "week_number": 'long',
        "day_of_year": 'long',
        "day_of_month": 'long',
        "day_of_week": {
          "number": 'long',
          "name": 'keyword',
        }
      },
      "time_slice": {
        "start_hour": 'long',
        "id": 'long',
        "name": 'keyword',
      },
      "neurio": {
        "appliance": {
          "label": 'keyword',
          "name": 'keyword',
          "attributes": {
            "autoDisagg": "boolean",
            "autoTagged": "boolean",
            "cycleTreshold": "long"
          },
          "locationId": 'keyword',
          "tags": 'keyword',
          "createdAt": 'date',
          "updatedAt": 'date',
          "id": 'keyword'
        },
        "status": 'keyword',
        "energy": {
          "consumption": 'long'
        },
        "averagePower": {
          "consumption": 'long'
        },
        "guesses": {
          "air_conditioner": 'float',
          "dryer": 'float',
          "electric_kettle": 'float',
          "electric_vehicle": 'float',
          "heater": 'float',
          "microwave": 'float',
          "oven": 'float',
          "pool_pump": 'float',
          "stove": 'float',
          "toaster": 'float',
          "water_heater": 'float'
        },
        "groupIds": 'keyword',
        "lastCycle": {
          "groupId": 'keyword',
          "start": 'date',
          "end": 'date',
          "energy": {
            "consumption": 'long'
          },
          "averagePower": {
            "consumption": 'long'
          },
          "createdAt": 'date',
          "updatedAt": 'date',
          "sensorId": 'keyword',
          "significant": 'boolean',
          "id": 'keyword'
        },
        "cycleCount": 'long',
        "isConfirmed": 'boolean',
        "id": 'keyword',
        "precedingEventId": "keyword"
      },
    },
  },
  {
    "name": 'neurio-samples',
    "fields": {
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
        "month": {
          "number": 'long',
          "name": 'keyword',
        },
        "week_number": 'long',
        "day_of_year": 'long',
        "day_of_month": 'long',
        "day_of_week": {
          "number": 'long',
          "name": 'keyword',
        }
      },
      "time_slice": {
        "start_hour": 'long',
        "id": 'long',
        "name": 'keyword',
      },
      "neurio": {
        "energy": {
          "consumption": 'long',
          "cumulativeConsumption": 'long',
          "generation": 'long',
          "net": 'long',
        },
        "power": {
          "consumption": 'long',
          "generation": 'long',
          "net": 'long',
        },
        "submeters": {
          "power": "long",
          "energy": "long",
          "name": "keyword",
          "channelNumber": "long"
        }
      }
    }
  },
  {
    "name": 'neurio-energy',
    "fields": {
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
        "timezone": "keyword"
      },
      "date_details": {
        "year": 'long',
        "month": {
          "number": 'long',
          "name": 'keyword',
        },
        "week_number": 'long',
        "day_of_year": 'long',
        "day_of_month": 'long',
        "day_of_week": {
          "number": 'long',
          "name": 'keyword',
        }
      },
      "time_slice": {
        "start_hour": 'long',
        "id": 'long',
        "name": 'keyword',
      },
      "neurio": {
        "energy": {
          "consumption": 'long',
          "cumulativeConsumption": 'long',
          "generation": 'long',
          "imported": 'long',
          "exported": 'long',
        },
      },
      "submeters": {
        "energy": "long",
        "name": "keyword",
        "channelNumber": "long"
      }
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
            _source: ['@timestamp', 'cumulativeConsumptionEnergy'],
            size: 1,
            sort: [{'@timestamp': 'desc'}],
          }).then(function(response) {
            var resp = c8.trimResults(response);
            if (opts.firstDate) {
              firstDate = new Date(opts.firstDate);
              console.log('Setting first time to ' + firstDate);
            }
            else if (resp && resp['@timestamp']) {
              var d = new Date(resp['@timestamp']);
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
              // console.log(JSON.stringify(user.locations[i], null, 1));
              getAppliancePage(c8, client, locId, firstDate, lastDate, MIN_POWER, EVENTS_PER_PAGE, 1);
              for (var j=0; j<user.locations[i].sensors.length; j++) {
                var sensorId = user.locations[i].sensors[j].id;
                if (!sensorId) {
                  continue;
                }
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
  // console.log(adapter.types[0].name);
  client.applianceEvents(locId, firstDate, lastDate, minPower, perPage, page).then(function(events) {
  // client.applianceEventsRecent(locId, firstDate).then(function(events) {
    // console.log(events);
    if (!events || !events.length) {
      return;
    }
    var bulk = [];
    for (var j=0; j<events.length; j++) {
      var values = events[j];
      let st = new Date(values.start);
      const start = moment(st.getTime());
      let et = new Date(values.end);
      var logStr = st.toISOString();
      if (values.appliance.label) {
        logStr += ' ' + values.appliance.label;
      }
      let startHour = st.getHours();
      let startMinute = st.getMinutes();
      let sliceName = [startHour, startMinute].join(':');
      if ((startMinute == 0) || (startMinute == 5)) {
        sliceName = [startHour, '0' + startMinute].join(':');
      }
      let idTime = startHour + startMinute/60;
      let sliceId = Math.round((idTime + (idTime >= 4 ? -4 : 20)) * 12);
      // console.log(logStr);
      var id = values.id;
      bulk.push({index: {_index: c8.type(applianceType)._index, _id: id}});
      let data = {
        "@timestamp": st,
        "ecs": {
          "version": '1.0.1'
        },
        "event": {
          "created": new Date(),
          "dataset": "neurio.appliances",
          "duration": (et.getTime() - st.getTime()) * 1E6, // ms to ns
          "end": et,
          "module": "neurio",
          "original": JSON.stringify(events[j]),
          "start": st,
        },
        "date_details": {
          "year": start.format('YYYY'),
          "month": {
            "number": start.format('M'),
            "name": start.format('MMMM'),
          },
          "week_number": start.format('W'),
          "day_of_year": start.format('DDD'),
          "day_of_month": start.format('D'),
          "day_of_week": {
            "number": start.format('d'),
            "name": start.format('dddd'),
          }
        },
        "time_slice": {
          "start_hour": startHour,
          "id": sliceId,
          "name": sliceName,
        },
        "neurio": {
          "appliance": values.appliance,
          "status": values.status,
          "energy": {
            "consumption": values.consumptionEnergy
          },
          "averagePower": {
            "consumption": values.consumptionPower
          },
          "guesses": values.guesses,
          "groupIds": values.groupIds,
          "lastCycle": {
            "groupId": values.lastCycle.groupId,
            "start": values.lastCycle.start,
            "end": values.lastCycle.end,
            "energy": {
              "consumption": values.lastCycle.consumptionEnergy
            },
            "averagePower": {
              "consumption": values.lastCycle.consumptionPower
            },
            "createdAt": values.lastCycle.createdAt,
            "updatedAt": values.lastCycle.updatedAt,
            "sensorId": values.lastCycle.sensorId,
            "significant": values.lastCycle.significant,
            "id": values.lastCycle.id
          },
          "cycleCount": values.cycleCount,
          "isConfirmed": values.isConfirmed,
          "id": values.id,
          "precedingEventId": values.precedingEventId
        }
      };
      bulk.push(data);
    }
    if (bulk.length > 0) {
      // console.log(bulk);
      // process.exit();
      c8.type(applianceType).bulk(bulk).then(function(response) {
        let result = c8.trimBulkResults(response);
        if (result.errors) {
          var messages = [];
          for (var i=0; i<result.items.length; i++) {
            if (result.items[i].index.error) {
              messages.push(i + ': ' + result.items[i].index.error.reason);
            }
          }
          throw(new Error(messages.length + ' errors in bulk insert:\n ' + messages.join('\n ')));
        }
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
    }
    else {
      console.log('No appliance events indexed.');
    }
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
      let cumulativeConsumptionEnergy = values.consumptionEnergy;
      let consumptionEnergy = values.consumptionEnergy - lastConsumptionEnergy;
      lastConsumptionEnergy = values.cumulativeConsumptionEnergy;
      let st = new Date(values.timestamp);
      const start = moment(st.getTime());
      let et = new Date(st.getTime() + 5 * 60 * 1000); // 5 minute intervals
      let startHour = st.getHours();
      let startMinute = st.getMinutes();
      let sliceName = [startHour, startMinute].join(':');
      if ((startMinute == 0) || (startMinute == 5)) {
        sliceName = [startHour, '0' + startMinute].join(':');
      }
      let idTime = startHour + startMinute/60;
      let sliceId = Math.round((idTime + (idTime >= 4 ? -4 : 20)) * 12);
      // var logStr = values.timestamp + ' ' + (values.netPower || values.consumptionPower) + ' W, ' + (values.netEnergy || values.consumptionEnergy) + ' Ws';
      // console.log(logStr);
      bulk.push({index: {_index: c8.type(sampleType)._index, _type: c8.type(sampleType)._type, _id: st}});
      let data = {
        "@timestamp": st,
        "ecs": {
          "version": '1.0.1'
        },
        "event": {
          "created": new Date(),
          "dataset": "neurio.history_samples",
          "duration": (et.getTime() - st.getTime()) * 1E6, // ms to ns
          "end": et,
          "module": "neurio",
          "original": JSON.stringify(events[i]),
          "start": st,
        },
        "date_details": {
          "year": start.format('YYYY'),
          "month": {
            "number": start.format('M'),
            "name": start.format('MMMM'),
          },
          "week_number": start.format('W'),
          "day_of_year": start.format('DDD'),
          "day_of_month": start.format('D'),
          "day_of_week": {
            "number": start.format('d'),
            "name": start.format('dddd'),
          }
        },
        "time_slice": {
          "start_hour": startHour,
          "id": sliceId,
          "name": sliceName,
        },
        "neurio": {
          "energy": {
            "consumption": values.consumptionEnergy || null,
            "generation": values.generationEnergy || null,
            "net": values.netEnergy || values.consumptionEnergy || null
          },
          "power": {
            "consumption": values.consumptionPower || null,
            "generation": values.generationPower || null,
            "net": values.netPower || values.consumptionPower || null
          },
        }
      };
      if (values.submeters) {
        data.neurio.submeters = values.submeters;
      }
      bulk.push(data);
      // console.log(values);
      // console.log(data);
    }
    if (bulk.length > 0) {
      c8.type(sampleType).bulk(bulk).then(function(response) {
        let result = c8.trimBulkResults(response);
        if (result.errors) {
          var messages = [];
          for (var i=0; i<result.items.length; i++) {
            if (result.items[i].index.error) {
              messages.push(i + ': ' + result.items[i].index.error.reason);
            }
          }
          throw(new Error(messages.length + ' errors in bulk insert:\n ' + messages.join('\n ')));
        }
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
    }
    else {
      console.log('No power readings indexed.');
    }
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
      let st = new Date(values.start);
      const start = moment(st.getTime());
      let et = new Date(values.end);
      var consumption = values.consumptionEnergy || 0;
      if (values.generationEnergy) {
        consumption -= values.generationEnergy;
      }
      // console.log(values);
      // console.log(consumption);
      let startHour = st.getHours();
      let startMinute = st.getMinutes();
      let sliceName = [startHour, startMinute].join(':');
      if ((startMinute == 0) || (startMinute == 5)) {
        sliceName = [startHour, '0' + startMinute].join(':');
      }
      let idTime = startHour + startMinute/60;
      let sliceId = Math.round((idTime + (idTime >= 4 ? -4 : 20)) * 12);
      // var logStr = values.start + ' ' + consumption + ' Ws';
      // console.log(logStr);
      bulk.push({index: {_index: c8.type(energyType)._index, _type: c8.type(energyType)._type, _id: st}});
      let data = {
        "@timestamp": st,
        "ecs": {
          "version": '1.0.1'
        },
        "event": {
          "created": new Date(),
          "dataset": "neurio.energy_stats",
          "duration": (et.getTime() - st.getTime()) * 1E6, // ms to ns
          "end": et,
          "module": "neurio",
          "original": JSON.stringify(stats[i]),
          "start": st,
        },
        "date_details": {
          "year": start.format('YYYY'),
          "month": {
            "number": start.format('M'),
            "name": start.format('MMMM'),
          },
          "week_number": start.format('W'),
          "day_of_year": start.format('DDD'),
          "day_of_month": start.format('D'),
          "day_of_week": {
            "number": start.format('d'),
            "name": start.format('dddd'),
          }
        },
        "time_slice": {
          "start_hour": startHour,
          "id": sliceId,
          "name": sliceName,
        },
        "neurio": {
          "energy": {
            "consumption": values.consumptionEnergy || null,
            "generation": values.generationEnergy || null,
            "imported": values.importedEnergy || null,
            "exported": values.exportedEnergy || null,
          },
        }
      };
      if (values.submeters) {
        data.neurio.submeters = values.submeters;
      }
      bulk.push(data);
    }
    if (bulk.length > 0) {
      c8.type(energyType).bulk(bulk).then(function(response) {
        let result = c8.trimBulkResults(response);
        if (result.errors) {
          var messages = [];
          for (var i=0; i<result.items.length; i++) {
            if (result.items[i].index.error) {
              messages.push(i + ': ' + result.items[i].index.error.reason);
            }
          }
          throw(new Error(messages.length + ' errors in bulk insert:\n ' + messages.join('\n ')));
        }
        console.log('Indexed ' + result.items.length + ' energy stats in ' + result.took + ' ms.');
        bulk = null;
      }).catch(function(error) {
        console.trace(error);
        bulk = null;
      });
    }
    else {
      console.log('No energy stats indexed.');
    }
  }).catch(function(error) {
    console.trace(error);
  });
}

function time2slice(t) {
  // creates a time_slice from a moment object
  let time_slice = {};
  let hour = t.format('H');
  let minute = (5 * Math.floor(t.format('m') / 5 )) % 60;
  time_slice.name = [hour, minute].join(':');
  if (minute == 5) {
    time_slice.name = [hour, '0' + minute].join(':');
  }
  else if (minute == 0) {
    time_slice.name += '0';
  }
  let idTime = parseInt(hour) + parseInt(minute)/60;
  time_slice.id = Math.round((idTime + (idTime >= 4 ? -4 : 20)) * 12);
  return time_slice;
}

module.exports = adapter;
