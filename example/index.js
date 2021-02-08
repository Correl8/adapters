const fetch = require('node-fetch')

// if no start date is given as an argument,
// fetch data from last DEFAULT_DAYS days
var DEFAULT_DAYS = 30

var adapter = {}

adapter.types = [
  {
    name: 'example-sensor',
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
      "sensor": {
        "id": 'keyword',
        "foo": 'keyword',
        "bar": {
          "id": 'keyword',
          "name": 'keyword',
          "value": 'float'
        },
        "date": 'date'
      }
    }
  }
]

adapter.promptProps = {
  properties: {
    url: {
      description: 'URL'.magenta
    }
  }
}

adapter.storeConfig = async (c8, result) => {
  console.log(result)
  await c8.config(result)
  console.log('Configuration stored.')
}

adapter.importData = async (c8, conf, opts) => {
  try {
    let firstDate = new Date();
    if (opts.firstDate) {
      firstDate = new Date(opts.firstDate);
      console.log('Setting first time to ' + firstDate);
    }
    else {
      response = await c8.search({
        _source: ['@timestamp'],
        size: 1,
        sort: [{'@timestamp': 'desc'}],
      });
      const resp = c8.trimResults(response);
      if (resp && resp["@timestamp"]) {
        const d = new Date(resp["@timestamp"]);
        firstDate.setTime(d.getTime() + 1);
        console.log('Setting first time to ' + firstDate);
      }
    }
    let url = conf.url + '&from=' + firstDate.toISOString();
    // const data = await fetch(url).then(res => res.json()) // example.com doesn't really return JSON
    data = [
      {
        "id": 1,
        "foo": "test-item",
        "bar": {
          "id": "test-1",
          "name": "propertyName",
          "value": 1.23
        },
        "date": "2020-12-31T23:59:59Z"
      },
      {
        "id": 2,
        "foo": "another-test-item",
        "bar": {
          "id": "test-1",
          "name": "propertyName",
          "value": 2.34
        },
        "date": "2021-01-01T00:00:00Z"
      }
    ]
    if (data && data.length) {
      const bulk = []
      for (item of data) {
        let values = {
          "@timestamp": item.date,
          "event": {
            "created": new Date(),
            "module": adapter.types[0].name,
            "original": JSON.stringify(item),
            "start": item.date,
          },
          "sensor": item
        }
        bulk.push({index: {_index: c8._index, _id: item.id}})
        bulk.push(values)
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
