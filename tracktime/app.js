var request = require("request");
var elasticsearch = require('elasticsearch');
var client = new elasticsearch.Client({
  host: 'localhost:9200',
  log: 'warning'
});

var INDEX_NAME = 'correl8';

var firstDate;
var apiUrl;

var acts = [
  'Unspecified',
  'Sleep',
  'Eating',
  'Other personal care',
  'Main and second job',
  'Employment activities',
  'School and university',
  'Homework',
  'Freetime study',
  'Food preparation',
  'Dish washing',
  'Cleaning dwelling',
  'Other household upkeep',
  'Laundry',
  'Ironing',
  'Handicraft',
  'Gardening',
  'Tending domestic animals',
  'Caring for pets',
  'Walking the dog',
  'Construction and repairs',
  'Shopping and services',
  'Child care',
  'Playing with and teaching kids',
  'Other domestic work',
  'Organisational work',
  'Help to other households',
  'Participatory activities',
  'Visits and feasts',
  'Other social life',
  'Entertainment and culture',
  'Resting',
  'Walking and hiking',
  'Sports and outdoors',
  'Computer and video games',
  'Other computing',
  'Other hobbies and games',
  'Reading books',
  'Other reading',
  'TV and video',
  'Radio and music',
  'Unspecified leisure',
  'Travel to/from work',
  'Travel related to study',
  'Travel related to shopping',
  'Transporting a child',
  'Travel related to other domestic',
  'Travel related to leisure',
  'Unspecified travel',
  'Unspecified'
];

var locs = {
  '10': 'Unspecified',
  '11': 'Home',
  '12': 'Second home',
  '13': 'Workplace/school',
  '14': 'Other\'s home',
  '15': 'Restaurant',
  '16': 'Shop, market',
  '17': 'Hotel, camping',
  '19': 'Other',
  '20': 'Unspecified',
  '21': 'Walking, waiting',
  '22': 'Bicycle',
  '23': 'Motorbike',
  '24': 'Car',
  '29': 'Other private',
  '31': 'Public transport'
};

var parents = [];
for (var i=1; i<acts.length; i++) {
  if (i <= 3) {
    parents[i] = 'Personal care';
  }
  else if (i <= 5) {
    parents[i] = 'Employment';
  }
  else if (i <= 8) {
    parents[i] = 'Study';
  }
  else if (i <= 24) {
    parents[i] = 'Domestic';
  }
  else if (i <= 41) {
    parents[i] = 'Leisure';
  }
  else if (i <= 48) {
    parents[i] = 'Travel';
  }
  else {
    parents[i] = 'Unspecified';
  }
}

var withValues = ['', 'alone', 'partner', 'parent', 'kids', 'family', 'others'];
var configIndex = {index: 'config', type: 'config-adapter'};

if (process.argv[2]) {
  var params = configIndex;
  params.id = 'config-adapter-time';
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
        console.log('Configure by ' + process.argv[0] + ' ' + process.argv[1] + ' <tracktime_url>');
    }
    else {
      getConfig(importData);
    }
  });
}

function getConfig(next) {
  var params = configIndex;
  params.q = 'id:config-adapter-time',
  params.body = {
    fields: ['url'],
    size: 1
  }
  client.search(params, function (error, response) {
    if (error) {
      console.warn("Config search got error: " + JSON.stringify(error));
      return;
    }
    if (response && response.hits && response.hits.hits) {
      apiUrl = response.hits.hits[0].fields.url;
      next();
    }
    else {
      console.log('Configure by ' + process.argv[0] + ' ' + process.argv[1] + ' <tracktime_url>');
    }
  });
}

function importData(next) {
  // console.log('Getting first date...');
  var query = {
    index: INDEX_NAME + '-time',
    type: 'time',
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
      firstDate = new Date(response.hits.hits[0].fields.timestamp);
    }
    else {
      console.warn("No previously indexed data, setting first time to 1!");
      firstDate = 1;
    }
    var url = apiUrl + '?starttime=' + Math.floor(firstDate/1000)
    // console.log(url);
    request(url, function(error, response, body) {
      if (error || !response || !body) {
        // console.warn('Error getting data: ' + JSON.stringify(response.body));
      }
      var data = JSON.parse(body);
      if (data && data.length) {
        var bulk = [];
        for (var i=0; i<data.length; i++) {
          bulk.push({index: {_index: INDEX_NAME + '-time', _type: 'time', _id: data[i].id}});
          data[i].starttime = new Date(data[i].starttime);
          data[i].endtime = new Date(data[i].endtime);
          data[i].duration = data[i].endtime - data[i].starttime;
          data[i].timestamp = data[i].starttime;
          data[i].location = locs[data[i].location];
          data[i].mainparent = parents[data[i].mainaction];
          data[i].mainaction = acts[data[i].mainaction];
          data[i].sideparent = parents[data[i].sideaction];
          data[i].sideaction = acts[data[i].sideaction];
          if (data[i]['with'] == 1) {
            data[i]['with'] = 'alone';
          }
          else {
            var a = [];
            for (var j=0; j<=6; j++) {
             var val = j;
             if (data[i]['with'] & Math.pow(2, (val-1))) {
              a.push(withValues[j].toLowerCase());
             }
            }
            data[i]['with'] = a.join(' ');
          }
          bulk.push(data[i]);
          console.log(data[i].timestamp);
        }
        // console.log(bulk);
        client.bulk(
          {
            index: INDEX_NAME + '-time',
            type: 'time',
            body: bulk
          },
          function (error, response) {
            if (error) {
              console.warn('ES Error: ' + error);
            }
            // console.log(response);
            // console.log('Done ' + doneCount++);
          }
        );
      }
    });
  });
  if (next) {
    next();
  }
}
