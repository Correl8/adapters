var request = require("request");
var elasticsearch = require('elasticsearch');
var client = new elasticsearch.Client({
  host: 'localhost:9200',
  log: 'warning'
});

var INDEX_NAME = 'correl8';

var firstDate;
var apiUrl;
var configIndex = {index: 'config', type: 'config-adapter'};

if (process.argv[2]) {
  var params = configIndex;
  params.id = 'config-adapter-expense';
  params.body = {id: params.id, url: process.argv[2]};
  client.index(params, function (error, response) {
    if (error) {
      console.warn(error);
      res.json(error);
      return;
    }
    console.log('Configuration saved.');
    process.exit();
  });
}
else {
  client.indices.exists({index: 'config'}, function(error, response) {
    if (!response) {
        console.log('Configure by ' + process.argv[0] + ' ' + process.argv[1] + ' <expense_url>')
    }
    else {
      getConfig(importData);
    }
  });
}

function getConfig(next) {
  var params = configIndex;
  params.q = 'id:config-adapter-expense',
  params.body = {
    fields: ['url'],
    size: 1
  }
  client.search(params, function (error, response) {
    if (error) {
      console.warn("Config search got error: " + JSON.stringify(error));
      return;
    }
    if (response && response.hits && response.hits.hits[0]) {
      apiUrl = response.hits.hits[0].fields.url;
      next();
    }
    else {
      console.log('Configure by ' + process.argv[0] + ' ' + process.argv[1] + ' <expense_url>');
    }
  });
}

function importData(next) {
  // console.log('Getting first date...');
  var query = {
    index: INDEX_NAME + '-expense',
    type: 'expense',
    body: {
      fields: ['timestamp'],
      size: 1,
      sort: [{'timestamp': 'desc'}],
    }
  };
  client.search(query, function (error, response) {
    if (error) {
      console.warn("search got error: " + JSON.stringify(error));
      return;
    }
    if (response && response.hits && response.hits.hits && response.hits.hits[0] && response.hits.hits[0].fields && response.hits.hits[0].fields.timestamp) {
      console.log("Setting first time to " + response.hits.hits[0].fields.timestamp);
      firstDate = new Date(response.hits.hits[0].fields.timestamp).toISOString();
    }
    else {
      console.warn("No previously indexed data, setting first time to 1!");
      firstDate = '2000-01-01';
    }
    var url = apiUrl + '&from=' + firstDate;
    var cookieJar = request.jar();
    console.log(url);
    request({url: url, jar: cookieJar}, function(error, response, body) {
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
            var id = dayData[j].date + '-' + dayData[j].cat;
            bulk.push({index: {_index: INDEX_NAME + '-expense', _type: 'expense', _id: id}});
            dayData[j].id = id;
            dayData[j].timestamp = dayData[j].date;
            bulk.push(dayData[j]);
            // console.log(dayData[j]);
          }
        }
        // console.log(bulk);
        client.bulk(
          {
            index: INDEX_NAME + '-expense',
            type: 'expense',
            body: bulk
          },
          function (error, response) {
            if (error) {
              console.warn('ES Error: ' + error);
            }
          }
        );
      }
    });
  });
  if (next) {
    next();
  }
}
