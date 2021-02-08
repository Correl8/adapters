const nodeFetch = require('node-fetch')
const fetch = require('fetch-cookie/node-fetch')(nodeFetch)

// if no start date is given as an argument,
// fetch data from last DEFAULT_DAYS days
var DEFAULT_DAYS = 30

var adapter = {}

adapter.sensorName = 'expense'
adapter.types = [
  {
    name: 'expense',
    fields: {
      "@timestamp": 'date',
      "ecs": {
        "version": 'keyword'
      },
      "event": {
        "created": "date",
        "module": "keyword",
        "original": "keyword",
        "start": "date",
      },
      "expense": {
        "category": {
          "id": 'long',
          "name": 'keyword'
        },
        "type": {
          "id": 'float',
          "name": 'keyword'
        },
        "cost": 'float'
      }
    }
  }
]

adapter.promptProps = {
  properties: {
    url: {
      description: 'Your expense url'.magenta
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
      firstDate = new Date(opts.firstDate)
      console.log('Setting first time to ' + firstDate)
    }
    else {
      const d = new Date()
      firstDate = new Date(d.getTime() - DEFAULT_DAYS * 24 * 60 * 60 * 1000)
      console.log('Setting first time to ' + firstDate)
    }
    let url = conf.url + '&from=' + firstDate.getDate() + '.' + (firstDate.getMonth() + 1) + '.' + firstDate.getFullYear()
    if (opts.lastDate) {
      const lastDate = opts.lastDate
      url += '&to=' + lastDate.getDate() + '.' + (lastDate.getMonth() + 1) + '.' + lastDate.getFullYear()
    }
    const data = await fetch(url).then(res => res.json())
    if (data && data.length) {
      const bulk = []
      for (dayData of data) {
        let dayCost = 0
        for (dd of dayData) {
          var id = dd.date + '-' + dd.t
          let data = {
              "@timestamp": dd.date,
              "event": {
                "created": new Date(),
                "module": "expense",
                "original": JSON.stringify(dd),
                "start": dd.date,
              },
              "expense": {
                "category": {
                  "id": dd.c,
                  "name": dd.category,
                },
                "type": {
                  "id": dd.t,
                  "name": dd.type,
                },
                "cost": dd.cost
              }
            }
            bulk.push({index: {_index: c8._index, _id: id}})
            bulk.push(data)
            dayCost += dd.cost
        }
        console.log(dayData[0].date + ': ' + dayCost)
      }
      if (bulk.length > 0) {
        const res = await c8.bulk(bulk)
        let result = c8.trimResults(res)
        if (result.errors) {
          var messages = [];
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
}

module.exports = adapter;
