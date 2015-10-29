var request = require('request');
var nopt = require('nopt'),
 noptUsage = require("nopt-usage"),
 knownOpts = {
    'url': [String, null],
    'help': Boolean,
    'init': Boolean,
    'clear': Boolean,
    'start': Date,
    'end': Date
  },
  shortHands = {
    'h': ['--help'],
    'i': ['--init'],
    'c': ['--clear'],
    'u': ['--url'],
    's': ['--start'],
    'e': ['--end']
  },
  description = {
    'url': ' Store the URI of your Tracktime instance and exit',
    'help': ' Display this usage text and exit',
    'init': ' Create the index and exit',
    'clear': ' Clear all data in the index',
    'start': ' Start date as YYYY-MM-DD',
    'end': ' End date as YYYY-MM-DD'
  },
  options = nopt(knownOpts, shortHands, process.argv, 2);

// console.log(options);
if (options['help']) {
  console.log('Usage: ');
  console.log(noptUsage(knownOpts, shortHands, description));
  process.exit();
}

var elasticsearch = require('elasticsearch');
var client = new elasticsearch.Client({
  host: 'localhost:9200',
  log: 'warning'
});

var INDEX_BASE = 'correl8';
var sensor = 'time';
var CONFIG_BASE = 'config-adapter';
var CONFIG_INDEX = 'config';

var firstDate = options['start'] || null;
var lastDate = options['end'] || null;
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
var configIndex = {index: CONFIG_INDEX, type: CONFIG_BASE};

if (options['url']) {
  var params = configIndex;
  params.id = CONFIG_BASE + '-' + sensor;
  params.body = {id: params.id, url: options['url']};
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
  client.indices.exists({index: CONFIG_INDEX}, function(error, response) {
    if (!response) {
        console.log('Usage: ');
        console.log(noptUsage(knownOpts, shortHands, description));
        // console.log('Configure by ' + process.argv[0] + ' ' + process.argv[1] + ' <tracktime_url>');
    }
    else {
      getConfig(importData);
    }
  });
}

function getConfig(next) {
  var params = configIndex;
  params.q = CONFIG_BASE + '-' + sensor;
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
      console.log('Usage: ');
      console.log(noptUsage(knownOpts, shortHands, description));
    }
  });
}

function importData(next) {
  // console.log('Getting first date...');
  var query = {
    index: INDEX_BASE + '-' + sensor,
    type: sensor,
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
    if (firstDate) {
      console.log("Setting first time to " + firstDate);
    }
    else if (response && response.hits && response.hits.hits && response.hits.hits[0] && response.hits.hits[0].fields && response.hits.hits[0].fields.timestamp) {
      console.log("Setting first time to " + response.hits.hits[0].fields.timestamp);
      firstDate = new Date(response.hits.hits[0].fields.timestamp);
    }
    else {
      console.warn("No previously indexed data, setting first time to 1!");
      firstDate = 1;
    }
    var url = apiUrl + '?starttime=' + Math.floor(firstDate/1000);
    if (lastDate) {
      console.log("Setting last time to " + lastDate);
      url += '&endtime=' + Math.ceil(lastDate/1000);
    }
    // console.log(url);
    request(url, function(error, response, body) {
      if (error || !response || !body) {
        // console.warn('Error getting data: ' + JSON.stringify(response.body));
      }
      var data = JSON.parse(body);
      if (data && data.length) {
        var bulk = [];
        for (var i=0; i<data.length; i++) {
          bulk.push({index: {_index: INDEX_BASE + '-' + sensor, _type: sensor, _id: data[i].id}});
          data[i].starttime = new Date(data[i].starttime);
          data[i].endtime = new Date(data[i].endtime);
          data[i].duration = (data[i].endtime - data[i].starttime)/1000;
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
            index: INDEX_BASE + '-' + sensor,
            type: sensor,
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
