// const nordpool = require("nordpool")
// import nordpool from 'nordpool'
// nordpool module imported later using dynamic import()
const dayjs = require('dayjs')
const utc = require('dayjs/plugin/utc')
const timezone = require('dayjs/plugin/timezone')

dayjs.extend(utc)
dayjs.extend(timezone)

const MS_IN_DAY = 24 * 60 * 60 * 1000

const adapter = {}

adapter.sensorName = 'nordpool-price-hourly'

adapter.types = [
  {
    name: 'nordpool-price-hourly',
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
      "nordpool": {
        "area": 'string',
        "value": 'float',
        "price": 'float'
      }
    }
  }
]

adapter.promptProps = {
  properties: {
    area: {
      description: 'Area'.magenta,
      default: 'ALL'
    },
    currency: {
      description: 'Currency'.magenta,
      default: 'EUR'
    }
  }
}

adapter.storeConfig = async (c8, result) => {
  await c8.config(result)
  console.log('Configuration stored.')
}

adapter.importData = async (c8, conf, opts) => {
  try {
    let firstDate, lastDate
    if (opts.firstDate) {
      firstDate = dayjs(opts.firstDate)
      console.log('Setting first time to ' + firstDate.format())
    }
    else if (!opts.lastDate) {
      const response = await c8.search({
        _source: ['@timestamp'],
        size: 1,
        sort: [{'@timestamp': 'desc'}],
      })
      const resp = c8.trimResults(response)
      if (resp && resp["@timestamp"]) {
        firstDate = dayjs(resp["@timestamp"])
      }
    }
    if (opts.lastDate) {
      lastDate = dayjs(opts.lastDate)
      // console.log("Setting lastDate to " + lastDate.format())
      if (!firstDate) {
        firstDate = lastDate.subtract(1, 'day')
        console.log("Setting firstDate to " + firstDate.format())
      }
    }
    else if (firstDate) {
      // lastDate = firstDate.add(1, 'day')
    }
    else {
      firstDate = dayjs()
      // lastDate = firstDate.add(1, 'day')
    }
    // don't specify "to" param, it'll just strip out stuff we want to index
    var params = {area: conf.area, currency: conf.currency, from: firstDate.format()}
    // console.log(params)
    if (firstDate) {
      params.from = firstDate
    }
    const { nordpool } = await import('nordpool')
    const prices = new nordpool.Prices()
    const data = await prices.hourly(params)
    if (data && data.length) {
      var bulk = []
      for (row of data) {
        const st = dayjs(row.date).tz("Europe/Oslo")
        const et = st.add(1, 'hour')
        const id = 'price-hourly-' + row.area + '-' + st.format()
        const values = c8.initECS("nordpool", row, st.toISOString(), et.toISOString(), null, 'metric', 'nordpool.prices')
        values.nordpool = row
        // EUR / MWh => snt/kWh, add VAT 24 %
        values.nordpool.price = Math.round(values.nordpool.value * 1.24 * 100) / 1000
        bulk.push({index: {_index: c8._index, _id: id}})
        bulk.push(values)
        console.log(id + ': ' + values.nordpool.price)
      }
      // console.log(JSON.stringify(bulk, null, 1))
      if (bulk.length > 0) {
        const res = await c8.bulk(bulk)
        let result = c8.trimResults(res)
        if (result.errors) {
          var messages = []
          for (var i=0; i<result.items.length; i++) {
            if (result.items[i].index.error) {
              messages.push(i + ': ' + result.items[i].index.error.reason)
            }
          }
          throw new Error(messages.length + ' errors in bulk insert:\n ' + messages.join('\n '))
          return
        }
        return 'Indexed ' + result.items.length + ' documents in ' + result.took + ' ms.'
        }
      else {
        throw new Error('Got data but could not parse indexable items!')
      }
    }
    else {
      return 'No data available'
    }
  }
  catch(e) {
    throw new Error(e)
  }
  return "(in async mode...)"
}
module.exports = adapter
