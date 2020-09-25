var request = require('request');
var moment = require('moment');

var MAX_DAYS = 31;
var MS_IN_DAY = 24 * 60 * 60 * 1000;
var apiUrl;
var acts = {
  "01": "Sleep",
  "02": "Eating",
  "03": "Other personal care",
  "11": "Main job and second job",
  "12": "Activities related to employment",
  "21": "School and university",
  "22": "Free time study",
  "30": "Unspecified household and family care",
  "31": "Food management",
  "32": "Household upkeep",
  "33": "Care for textiles",
  "34": "Gardening and pet care",
  "35": "Construction and repairs",
  "36": "Shopping and services",
  "37": "Household management",
  "38": "Childcare",
  "39": "Help to an adult household member",
  "41": "Organisational work",
  "42": "Informal help to other households",
  "43": "Participatory and religious activities",
  "51": "Social life",
  "52": "Entertainment and culture",
  "53": "Resting - time out",
  "61": "Physical excercise",
  "62": "Productive excercise",
  "63": "Sports related activities",
  "71": "Arts and hobbies",
  "72": "Computing",
  "73": "Games",
  "81": "Reading",
  "82": "TV, video and DVD",
  "83": "Radio and recordings",
  "91": "Travel to/from work",
  "92": "Travel related to study",
  "93": "Travel r. to shopping, services, childcare &amp;c.",
  "94": "Travel related to voluntary work and meetings",
  "95": "Travel related to social life",
  "96": "Travel related to other leisure",
  "98": "Travel related to changing locality",
  "90": "Other or unspecified travel purpose",
  "99": "Other unspecified time use",
};
var parents = {};
for (var i in acts) {
  let n = parseInt(i);
  if (i < 10) {
    parents[i] = "Personal care";
  }
  else if (i < 20) {
    parents[i] = "Employment";
  }
  else if (i < 30) {
    parents[i] = "Study";
  }
  else if (i < 40) {
    parents[i] = "Household and family care";
  }
  else if (i < 50) {
    parents[i] = "Voluntary work and meetings"; 
  }
  else if (i < 60) {
    parents[i] = "Social life and entertainment"; 
  }
  else if (i < 70) {
    parents[i] = "Sports and outdoor activities"; 
  }
  else if (i < 80) {
    parents[i] = "Hobbies"; 
  }
  else if (i < 90) {
    parents[i] = "Mass media"; 
  }
  else if (i < 99) {
    parents[i] = "Travel by purpose"; 
  }
  else {
    parents[i] = "Unspecified";
  }
}

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

var withValues = ['', 'alone', 'partner', 'parent', 'kids', 'family', 'others'];

var adapter = {};

adapter.sensorName = 'tracktime';

adapter.types = [
  {
    name: 'tracktime',
    fields: {
      "@timestamp": "date",
      "ecs": {
        "version": "keyword"
      },
      "event": {
        "created": "date",
        "dataset": "keyword",
        "duration": "long",
        "end": "date",
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
        "start_hour": 'integer',
        "id": 'integer',
        "name": 'keyword',
      },
      "hetus": {
        "activity": {
          "main": {
            "id": "long",
            "name": "keyword",
            "category": "keyword"
          },
          "side": {
            "id": "long",
            "name": "keyword",
            "category": "keyword"
          },
        },
        "with_whom": {
          "id": 'long',
          "name": 'keyword',
        },
        "location": {
          "id": 'long',
          "name": 'keyword',
        },
        "use_computer": 'boolean',
        // "description": 'text',
        // "rating": "long"
      },
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
      _source: ['@timestamp'],
      size: 1,
      sort: [{'@timestamp': 'desc'}],
    }).then(function(response) {
      var resp = c8.trimResults(response);
      if (opts.firstDate) {
        firstDate = new Date(opts.firstDate);
        console.log('Setting first time to ' + firstDate);
      }
      else if (resp && resp['@timestamp']) {
        var d = new Date(resp['@timestamp']);
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
        if (lastDate < firstDate) {
          console.error('ERROR: end time ' + lastDate + ' before start time ' + firstDate + '!');
          process.exit(-1);
        }
        url += '&endtime=' + Math.ceil(lastDate/1000);
      }
      var cookieJar = request.jar();
      // console.log(url);
      request({url: url, jar: cookieJar}, function(error, response, body) {
        if (error || !response || !body) {
          // console.warn('Error getting data: ' + JSON.stringify(response.body));
        }
        // console.log(body);
        var times = JSON.parse(body);
        if (times && times.length) {
          var bulk = [];
          for (var i=times.length-1; i>=0; i--) {
            let entry = times[i];
            let st = moment(entry.starttime);
            let et = moment(entry.endtime);
            let withId = entry['with'];
            let withArray = [];
            if (withId == 1) {
              withArray = ['alone'];
            }
            else {
              for (var j=0; j<=6; j++) {
                if (withId & Math.pow(2, (j-1))) {
                  withArray.push(withValues[j].toLowerCase());
                }
              }
            }
            let data = {
              "@timestamp": st.format(),
              "ecs": {
                "version": "1.0.1"
              },
              "event": {
                "created": new Date(),
                "dataset": "tracktime.hetus",
                "duration": moment.duration(et.diff(st)).as('ms') * 1E6, // ms to ns
                "end": et.format(),
                "module": "tracktime",
                "original": JSON.stringify(entry),
                "start": st.format(),
              },
              "date_details": {
              },
              "time_slice": {
              },
              "hetus": {
                "activity": {
                  "main": {
                    "id": entry.mainaction,
                    "name": acts[entry.mainaction],
                    "category": parents[entry.mainaction]
                  },
                  "side": {
                    "id": entry.sideaction,
                    "name": acts[entry.sideaction],
                    "category": parents[entry.sideaction]
                  },
                },
                "with_whom": {
                  "id": withId,
                  "name": withArray
                },
                "location": {
                  "id": entry.location,
                  "name": locs[entry.location],
                },
                "use_computer": entry.usecomputer = entry.usecomputer ? true : false,
                // "description": entry.description,
                // "rating": entry.rating
              }
            };
            
            // split into 5 minute slices
            var fiveMinutesInNanos = 5 * 60 * 1000 * 1E6;
            var t = st;
            while (t.valueOf() < et.valueOf()) {
              var copy = JSON.parse(JSON.stringify(data));
              copy.date_details = {
                "year": t.format('YYYY'),
                "month": {
                  "number": t.format('M'),
                  "name": t.format('MMMM'),
                },
                "week_number": t.format('W'),
                "day_of_year": t.format('DDD'),
                "day_of_month": t.format('D'),
                "day_of_week": {
                  "number": t.format('d'),
                  "name": t.format('dddd'),
                }
              }
              var startHour = t.format('H');
              var startMinute = t.format('m');
              copy.time_slice.name = [startHour, startMinute].join(':');
              if (startMinute == 5) {
                copy.time_slice.name = [startHour, '0' + startMinute].join(':');
              }
              else if (startMinute == 0) {
                copy.time_slice.name += '0';
              }
              var idTime = parseInt(startHour) + parseInt(startMinute)/60;
              copy.time_slice.id = Math.round((idTime + (idTime >= 4 ? -4 : 20)) * 12);
              copy['@timestamp'] = t.format();
              copy.event.start = t.format();
              copy.time_slice.start_hour = startHour;
              // the start of next interval is the end of current one
              t.add(5, 'minutes');
              copy.event.end = t.format();
              copy.event.duration = fiveMinutesInNanos;
              bulk.push({index: {_index: c8._index, _type: c8._type, _id: t.valueOf()}});
              bulk.push(copy);
            }
            // console.log(entry['@timestamp']);
          }
          if (bulk.length > 0) {
            c8.bulk(bulk).then(function(response) {
              let result = c8.trimBulkResults(response);
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
