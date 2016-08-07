var request = require("request");

var adapter = {};

// name that describes the data, used in index names in elasticsearch
adapter.sensorName = 'example';

// data structure
adapter.types = [
  {
    name: 'example-sensor',
    // field names and types will be learned if not specified here
    fields: {
      timestamp: 'date',
      value: 'integer',
      name: 'string',
      type: 'string'
    }
  }
];

// configurable settings, see https://www.npmjs.com/package/prompt
// and
adapter.promptProps = {
  properties: {
    username: {
      description: 'Username'.magenta
    },
    password: {
      description: 'Password'.magenta,
      hidden: true
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
  console.log('Getting first date...');
  var firstDate, lastDate;
  return c8.search({
    _source: ['timestamp'],
    size: 1,
    sort: [{'timestamp': 'desc'}],
  }).then(function(response) {
    if (opts.firstDate) {
      console.log("Setting first time by opts to " + opts.firstDate);
      firstDate = opts.firstDate;
    }
    else if (response && response.hits && response.hits.hits && response.hits.hits[0] && response.hits.hits[0]._source && response.hits.hits[0]._source.timestamp) {
      console.log("Setting first time to " + response.hits.hits[0]._source.timestamp);
      firstDate = new Date(response.hits.hits[0]._source.timestamp);
    }
    else {
      console.warn("No previously indexed data, setting first time to 0!");
      firstDate = new Date(0);
    }
    var url = conf.url + '&from=' + firstDate.getDate() + '.' + (firstDate.getMonth() + 1) + '.' + firstDate.getFullYear();
    if (opts.lastDate) {
      var lastDate = opts.lastDate;
      url += '&to=' + lastDate.getDate() + '.' + (lastDate.getMonth() + 1) + '.' + lastDate.getFullYear();
    }
    var cookieJar = request.jar();
    // console.log(url);
    return request({url: url, jar: cookieJar}, function(error, response, body) {
      if (error || !response || !body) {
        // console.warn('Error getting data: ' + JSON.stringify(response.body));
      }
      // console.log(body);
      var data = JSON.parse(body);
      if (data && data.length) {
        var bulk = [];
        for (var i=0; i<data.length; i++) {
          var dayData = data[i];
          for (var j=0; j<dayData.length; j++) {
            var id = dayData[j].date + '-' + dayData[j].t;
            bulk.push({index: {_index: c8._index, _type: c8._type, _id: id}});
            dayData[j].id = id;
            dayData[j].timestamp = dayData[j].date;
            bulk.push(dayData[j]);
            console.log(dayData[j].date);
          }
        }
        // console.log(bulk);
        return c8.bulk(bulk).then(function(result) {
          console.log('Indexed ' + result.items.length + ' documents in ' + result.took + ' ms.');
          c8.release();
        }).catch(function(error) {
          console.trace(error);
          c8.release();
        });
      }
    });
  });
}
module.exports = adapter;