var request = require('request');

var MAX_DAYS = 100;
var MS_IN_DAY = 24 * 60 * 60 * 1000;
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

var adapter = {};

adapter.sensorName = 'tracktime';

adapter.types = [
  {
    name: 'tracktime',
    fields: {
      id: 'keyword',
      timestamp: 'date',
      starttime: 'date',
      starthour: 'integer',
      endtime: 'date',
      sliceid: 'integer',
      slice: 'keyword',
      duration: 'integer',
      mainid: 'integer',
      mainaction: 'keyword',
      maincategory: 'keyword',
      sideid: 'integer',
      sideaction: 'keyword',
      sidecategory: 'keyword',
      withid: 'integer',
      with: 'keyword',
      usecomputer: 'boolean',
      location: 'keyword',
      locid: 'integer',
      description: 'text',
      rating: 'integer'
    }
  }
];

adapter.promptProps = {
  properties: {
    url: {
      description: 'Enter your TrackTime URL'.magenta
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
    c8.search({
      _source: ['timestamp'],
      size: 1,
      sort: [{'timestamp': 'desc'}],
    }).then(function(response) {
      var resp = c8.trimResults(response);
      if (opts.firstDate) {
        firstDate = new Date(opts.firstDate);
        console.log('Setting first time to ' + firstDate);
      }
      else if (resp && resp.timestamp) {
        var d = new Date(resp.timestamp);
        firstDate = new Date(d.getTime() + 1);
        console.log('Setting first time to ' + firstDate);
      }
      else {
        firstDate = new Date();
        firstDate.setMonth(firstDate.getMonth() - 1);
        console.warn('No previously indexed data, setting first time to ' + firstDate);
      }
      if (opts.lastDate) {
        lastDate = new Date(opts.lastDate);
      }
      else {
        lastDate = new Date();
      }
      if (lastDate.getTime() >= (firstDate.getTime() + (MAX_DAYS * MS_IN_DAY))) {
        lastDate.setTime(firstDate.getTime() + (MAX_DAYS * MS_IN_DAY));
        console.warn('Max date range %d days, setting lastDate to %s', MAX_DAYS, lastDate);
      }
      var url = conf.url + '?starttime=' + Math.floor(firstDate/1000);
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
          for (var i=data.length-1; i>=0; i--) {
            // bulk.push({index: {_index: c8._index, _type: c8._type, _id: data[i].id}});
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
            data[i].usecomputer = data[i].usecomputer ? true : false;
            data[i].locid = data[i].location;
            data[i].location = locs[data[i].location];
            data[i].withid = data[i]['with'];
            if (data[i] == 1) {
              data[i].withid = ['alone'];
            }
            else {
              data[i]['with'] = [];
              for (var j=0; j<=6; j++) {
                if (data[i].withid & Math.pow(2, (j-1))) {
                  data[i]['with'].push(withValues[j].toLowerCase());
                }
              }
            }
            // split into 10 minute slices
            var tenMinutes = 10 * 60 * 1000;
            var start = data[i].starttime.getTime();
            var end = data[i].endtime.getTime();
            for (var t = start; t < end; t += tenMinutes) {
              var copy = JSON.parse(JSON.stringify(data[i]));
              // var id = t + '-' + data[i].id;
              var sliceTime = new Date(t);
              var startHour = sliceTime.getHours();
              var startMinute = sliceTime.getMinutes();
              copy.slice = [startHour, startMinute].join(':');
              if (startMinute == 0) {
                copy.slice += '0';
              }
              var idTime = startHour + startMinute/60;
              copy.sliceid = Math.round((idTime + (idTime >= 4 ? -4 : 20)) * 6);
              copy.timestamp = t;
              copy.starttime = t;
              copy.starthour = startHour;
              copy.endtime = t + tenMinutes;
              copy.duration = tenMinutes / 1000;
              bulk.push({index: {_index: c8._index, _type: c8._type, _id: t}});
              bulk.push(copy);
              // console.log(JSON.stringify(copy.sliceId) + ': ' + sliceTime);
            }
            console.log(data[i].timestamp);
          }
          if (bulk.length > 0) {
            c8.bulk(bulk).then(function(result) {
              if (result.errors) {
                var messages = [];
                for (var i=0; i<result.items.length; i++) {
                  if (result.items[i].index.error) {
                    messages.push(i + ': ' + result.items[i].index.error.reason);
                  }
                }
                reject(new Error(messages.length + ' errors in bulk insert:\n ' + messages.join('\n ')));
              }
              fulfill('Indexed ' + result.items.length + ' documents in ' + result.took + ' ms.');
            }).catch(function(error) {
              reject(error);
            });
          }
          else {
            fulfill('No data to import.');
          }
        }
      });
    }).catch(function(error) {
      reject(error);
      c8.release();
    });
  });
};

module.exports = adapter;
