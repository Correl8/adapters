const neurio = require('neurio');
const moment = require('moment');

const adapter = {};

const MS_IN_DAY = 24 * 60 * 60 * 1000;
const MAX_DAYS = 10;
const MAX_PAGES = 5;
const EVENTS_PER_PAGE = 100;
const MIN_POWER = 400;
// maximum granularity is 5 minutes
const GRANULARITY = 'minutes';
const FREQUENCY = 5;
let cumulative = 0;

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
        "ingested": "date",
        "kind": "keyword",
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
        "ingested": "date",
        "kind": "keyword",
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
        "ingested": "date",
        "kind": "keyword",
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

adapter.storeConfig = async (c8, conf) => {
  // User information (including locations) is stored as a part of config.
  // No need to call the user endpoint every time, but if locations are
  // added, adapter has to be reconfigured.
  const auth = neurio.Auth;
  const client = await auth.simple(conf.clientId, conf.clientSecret)
  // user object is too complex to index as a part of config, store as string
  conf.user = JSON.stringify(await client.user());
  await c8.config(conf)
  console.log('Configuration stored.')
}

adapter.importData = async (c8, conf, opts) => {
  try {
    const auth = neurio.Auth;
    const client = await auth.simple(conf.clientId, conf.clientSecret)
    let firstDate, lastDate;
    let sortOrder = 'desc';
    if (!conf.user) {
      throw new Error('User information missing. Configure first!')
    }
    const user = JSON.parse(conf.user);
    if (!user || !user.locations || !user.locations.length) {
      throw new Error('User information missing or invalid. Configure first!')
    }
    if (opts.firstDate) {
      firstDate = new Date(opts.firstDate);
    }
    if (opts.lastDate) {
      lastDate = new Date(opts.lastDate);
      // sortOrder = 'asc'
      if (!firstDate) {
        firstDate = new Date(lastDate.getTime() - (MS_IN_DAY));
        console.log('Setting first time to ' + firstDate);
      }
    }
    else {
      lastDate = new Date();
    }
    const queryParams = {
      _source: ['@timestamp', 'neurio.energy.cumulativeConsumption'],
      size: 1,
      sort: [{'@timestamp': sortOrder}],
    };
    if (firstDate) {
      queryParams.query = {
        range:{
          "@timestamp":{
            // gte: firstDate,
            // lt: lastDate || 'NOW'
            lt: firstDate || 'NOW'
          }
        }
      };
    }
    // console.log(JSON.stringify(queryParams));
    const response = await c8.type(adapter.types[1].name).search(queryParams);
    const resp = c8.trimResults(response);
    // console.log(resp);
    // console.log(resp.neurio);
    cumulative = 0;
    if (resp && resp.neurio) {
      cumulative = resp.neurio.energy.cumulativeConsumption;
    }
    // console.log(cumulative)
    if (resp['@timestamp'] && !firstDate) {
      firstDate = new Date(resp['@timestamp']);
      console.warn('Adjusting first time to ' + firstDate);
    }
    else if (!firstDate) {
      firstDate = new Date(user.createdAt);
      console.warn('No previously indexed data, setting first time to ' + firstDate);
    }
    if (lastDate.getTime() > (firstDate.getTime() + MS_IN_DAY)) {
      lastDate.setTime(firstDate.getTime() + MS_IN_DAY - 1);
      console.warn('Max time range 24 hours, setting end time to ' + lastDate);
    }
    let totalResults = 0
    // console.log(firstDate, lastDate)
    for (loc of user.locations) {
      const locId = loc.id;
      // console.log(JSON.stringify(user.locations[i], null, 1));
      totalResults += await getAppliancePage(c8, client, locId, firstDate, lastDate, MIN_POWER, EVENTS_PER_PAGE, 1)
      // console.log(totalResults)
      for (const sensor of loc.sensors) {
        if (!sensor.id) {
          continue;
        }
        totalResults += await getSamplesHistoryPage(c8, client, sensor.id, firstDate, lastDate, GRANULARITY, FREQUENCY, EVENTS_PER_PAGE, 1);
        totalResults += await getEnergyStats(c8, client, sensor.id, firstDate, lastDate, GRANULARITY, FREQUENCY);
      }
    }
    return `Imported ${totalResults} items from ${user.locations.length} locations`
  }
  catch(e) {
    throw new Error(e)
  };
};

getAppliancePage = async (c8, client, locId, firstDate, lastDate, minPower, perPage, page) => {
  const applianceType = adapter.types[0].name
  let applianceResults = 0
  let bulk = [];
  try {
    const events = await client.applianceEvents(locId, firstDate, lastDate, minPower, perPage, page)
    if (!events || !events.length) {
      // console.log(`No appliance events found from location ${locId}.`);
      return 0;
    }
    for (values of events) {
      let st = new Date(values.start);
      const start = moment(st.getTime());
      let et = new Date(values.end);
      let logStr = st.toISOString();
      if (values.appliance.label) {
        logStr += ' ' + values.appliance.label;
      }
      // console.log(logStr);
      const id = values.id;
      bulk.push({index: {_index: c8.type(applianceType)._index, _id: id}});
      let data = {
        "@timestamp": st,
        "ecs": {
          "version": '1.6.0'
        },
        "event": {
          "created": st,
          "dataset": "neurio.appliances",
          "duration": (et.getTime() - st.getTime()) * 1E6, // ms to ns
          "end": et,
          "ingested": new Date(),
          "kind": "event",
          "module": "neurio",
          "original": JSON.stringify(values),
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
        "time_slice": time2slice(start),
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
      const response = await c8.type(applianceType).bulk(bulk)
      const result = c8.trimBulkResults(response);
      if (result.errors) {
        const messages = [];
        for (var i=0; i<result.items.length; i++) {
          if (result.items[i].index.error) {
            messages.push(i + ': ' + result.items[i].index.error.reason);
          }
        }
        throw(new Error(messages.length + ' errors in bulk insert:\n ' + messages.join('\n ')));
      }
      else {
        applianceResults += result.items.length;
      }
      console.log('Indexed ' + result.items.length + ' appliance events in ' + result.took + ' ms.');
      bulk = null;
      if (events.length == perPage) {
        // page was full, request the next page (recursion!)
        page++;
        if (page <= MAX_PAGES) {
          applianceResults += await getAppliancePage(c8, client, locId, firstDate, lastDate, minPower, perPage, page);
        }
      }
    }
    else {
      console.log('No appliance events indexed.');
    }
  }
  catch(e) {
    console.error('Error indexing data');
    throw new Error(e);
  }
  return applianceResults;
}

getSamplesHistoryPage = async (c8, client, sensorId, start, end, granularity, frequency, perPage, page) => {
  const sampleType = adapter.types[1].name;
  let sampleResults = 0
  let bulk = [];
  try {
    const events = await client.historySamples(sensorId, start, end, granularity, frequency, perPage, page)
    for (event of events) {
      const values = Object.assign({}, event); // create a copy - events will still be the original
      const delta = values.consumptionEnergy - cumulative // difference from last known reading
      values.consumptionEnergy = delta;
      cumulative += delta;
      values.cumulativeConsumption = cumulative
      let et = new Date(values.timestamp);
      let st = new Date(et.getTime() - 5 * 60 * 1000); // 5 minute intervals
      const start = moment(st.getTime());
      const logStr = values.timestamp + ' ' + (values.netPower || values.consumptionPower) + ' W, ' + (values.netEnergy || values.consumptionEnergy) + ' Ws';
      // console.log(logStr);
      bulk.push({index: {_index: c8.type(sampleType)._index, _id: st}});
      let data = {
        "@timestamp": et,
        "ecs": {
          "version": '1.6.0'
        },
        "event": {
          "created": et,
          "dataset": "neurio.history_samples",
          "duration": (et.getTime() - st.getTime()) * 1E6, // ms to ns
          "end": et,
          "ingested": new Date(),
          "kind": "metric",
          "module": "neurio",
          "original": JSON.stringify(event),
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
        "time_slice": time2slice(start),
        "neurio": {
          "energy": {
            "cumulativeConsumption": values.cumulativeConsumption || null,
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
      // console.log(event);
      // console.log(values);
      // console.log(data);
      // return;
    }
    // console.log(cumulative);
    if (bulk.length > 0) {
      const response = await c8.type(sampleType).bulk(bulk)
      const result = c8.trimBulkResults(response);
      if (result.errors) {
        const messages = [];
        for (var i=0; i<result.items.length; i++) {
          if (result.items[i].index.error) {
            messages.push(i + ': ' + result.items[i].index.error.reason);
          }
        }
        throw(new Error(messages.length + ' errors in bulk insert:\n ' + messages.join('\n ')));
      }
      else {
        sampleResults += result.items.length;
      }
      console.log('Indexed ' + result.items.length + ' power readings in ' + result.took + ' ms.');
      bulk = null;
      if (events.length == perPage) {
        // page was full, request the next page (recursion!)
        page++;
        if (page <= MAX_PAGES) {
          sampleResults += await getSamplesHistoryPage(c8, client, sensorId, start, end, granularity, frequency, perPage, page);
        }
      }
    }
    else {
      console.log('No power readings indexed.');
    }
  }
  catch(e) {
    console.error('Error indexing data');
    throw new Error(e);
  }
  return sampleResults
}

getEnergyStats = async (c8, client, sensorId, start, end, granularity, frequency) => {
  const energyType = adapter.types[2].name;
  let statsResults = 0
  try {
    const stats = await client.stats(sensorId, start, end, granularity, frequency)
    let bulk = [];
    for (values of stats) {
      let st = new Date(values.start);
      const start = moment(st.getTime());
      let et = new Date(values.end);
      // let consumption = values.consumptionEnergy || 0;
      // if (values.generationEnergy) {
      //   consumption -= values.generationEnergy;
      // }
      // const logStr = values.start + ' ' + consumption + ' Ws';
      // console.log(logStr);
      bulk.push({index: {_index: c8.type(energyType)._index, _id: st}});
      let data = {
        "@timestamp": st,
        "ecs": {
          "version": '1.6.0'
        },
        "event": {
          "created": st,
          "dataset": "neurio.energy_stats",
          "duration": (et.getTime() - st.getTime()) * 1E6, // ms to ns
          "end": et,
          "ingested": new Date(),
          "kind": "metric",
          "module": "neurio",
          "original": JSON.stringify(values),
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
        "time_slice": time2slice(start),
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
      // console.log(data['@timestamp']);
    }
    if (bulk.length > 0) {
      // console.log(JSON.stringify(bulk, null, 1));
      const response = await c8.type(energyType).bulk(bulk)
      const result = c8.trimBulkResults(response);
      if (result.errors) {
        const messages = [];
        for (var i=0; i<result.items.length; i++) {
          if (result.items[i].index.error) {
            messages.push(i + ': ' + result.items[i].index.error.reason);
          }
        }
        throw(new Error(messages.length + ' errors in bulk insert:\n ' + messages.join('\n ')));
      }
      else {
        statsResults += result.items.length
      }
      console.log('Indexed ' + result.items.length + ' energy stats in ' + result.took + ' ms.');
      bulk = null;
    }
    else {
      console.log('No energy stats indexed.');
    }
  }
  catch(e) {
    throw new Error(e);
  }
  return statsResults
}

function time2slice(t) {
  // creates a time_slice from a moment object
  let hour = t.format('H');
  let minute = (5 * Math.floor(t.format('m') / 5 )) % 60;
  let idTime = parseInt(hour) + parseInt(minute)/60;
  let time_slice = {
    id: Math.round((idTime + (idTime >= 4 ? -4 : 20)) * 12),
    name: [hour, minute].join(':'),
    start_hour: parseInt(hour)
  };
  if (minute < 10) {
    time_slice.name = [hour, '0' + minute].join(':');
  }
  return time_slice;
}

module.exports = adapter;
