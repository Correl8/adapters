var request = require("request");
var correl8 = require("correl8");
var nopt = require('nopt');
var noptUsage = require("nopt-usage");

var c8 = correl8('tracktime');

var fields = {
  id: 'string', // actually integer, stored as string for compatibility
  starttime: 'date',
  endtime: 'date',
  duration: 'integer',
  mainid: 'integer',
  mainaction: 'string',
  maincategory: 'string',
  sideid: 'integer',
  sideaction: 'string',
  sidecategory: 'string',
  with: 'text',
  usecomputer: 'boolean',
  location: 'string',
  locid: 'integer',
  description: 'text',
  rating: 'integer'
};
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

var knownOpts = {
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
    'from': ['--start'],
    's': ['--start'],
    'to': ['--end'],
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
var firstDate = options['start'] || null;
var lastDate = options['end'] || null;

// console.log(options);
if (options['help']) {
  console.log('Usage: ');
  console.log(noptUsage(knownOpts, shortHands, description));
}
else if (options['url']) {
  c8.config({url: options['url']}).then(function(){
    console.log('Configuration stored.');
    c8.release();
  });
}
else if (options['clear']) {
  c8.clear().then(function(res) {
    console.log('Index cleared.');
    c8.release();
  }).catch(function(error) {
    console.trace(error);
    c8.release();
  });
}
else if (options['init']) {
  c8.init(fields).then(function(res) {
    console.log('Index initialized.');
    c8.release();
  }).catch(function(error) {
    console.trace(error);
    c8.release();
  });
}
else {
  c8.config().then(function(res) {
    if (res.hits && res.hits.hits && res.hits.hits[0] && res.hits.hits[0]._source['url']) {
      // console.log(res.hits.hits[0]);
      apiUrl = res.hits.hits[0]._source['url'];
      // console.log('Url set to ' + apiUrl);
      importData();
    }
    else {
      console.log('Configure first using --url. Usage: ');
      console.log(noptUsage(knownOpts, shortHands, description));
      c8.release();
    }
  });
}

function importData() {
  // console.log('Getting first date...');
  c8.search({
    fields: ['timestamp'],
    size: 1,
    sort: [{'timestamp': 'desc'}],
  }).then(function(response) {
    if (firstDate) {
      console.log("Setting first time to " + firstDate);
    }
    else if (response && response.hits && response.hits.hits && response.hits.hits[0] && response.hits.hits[0].fields && response.hits.hits[0].fields.timestamp) {
      console.log("Setting first time to " + response.hits.hits[0].fields.timestamp);
      firstDate = new Date(response.hits.hits[0].fields.timestamp);
    }
    else {
      console.warn("No previously indexed data, setting first time to 0!");
      firstDate = new Date(0);
    }
    var url = apiUrl + '?starttime=' + Math.floor(firstDate/1000);
    if (lastDate) {
      console.log("Setting last time to " + lastDate);
      url += '&endtime=' + Math.ceil(lastDate/1000);
    }
    var cookieJar = request.jar();
    // console.log(url);
    request({url: url, jar: cookieJar}, function(error, response, body) {
      if (error || !response || !body) {
        // console.warn('Error getting data: ' + JSON.stringify(response.body));
      }
      // console.log(body);
      var data = JSON.parse(body);
      if (data && data.length) {
        var bulk = [];
        for (var i=0; i<data.length; i++) {
          bulk.push({index: {_index: c8._index, _type: c8._type, _id: data[i].id}});
          data[i].starttime = new Date(data[i].starttime);
          data[i].endtime = new Date(data[i].endtime);
          data[i].duration = (data[i].endtime - data[i].starttime)/1000;
          data[i].timestamp = data[i].starttime;
          data[i].mainid = data[i].mainaction;
          data[i].mainaction = acts[data[i].mainaction];
          data[i].maincategory = parents[data[i].mainid];
          data[i].sideid = data[i].sideaction;
          data[i].sideaction = acts[data[i].sideaction];
          data[i].sidecategory = parents[data[i].sideid];
          data[i].locid = data[i].location;
          data[i].location = locs[data[i].location];
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
        c8.bulk(bulk).then(function(result) {
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
