var prompt = require('prompt');
var correl8 = require('correl8');
var lockFile = require('lockfile');
var nopt = require('nopt');
var noptUsage = require('nopt-usage');
var moment = require('moment');
var LastfmAPI = require('lastfmapi');

var lockFile = require('lockfile');

var recentType = 'lastfm-recent';
var c8 = correl8(recentType);
var MS_IN_DAY = 24 * 60 * 60 * 1000;
var MAX_DAYS = 365;

var recentFields = {
  'timestamp': 'date',
  'name': 'string', // text?
  'artist': {
    '#text': 'string',
  },
  'album': {
    '#text': 'string',
  }
};

var knownOpts = {
  'authenticate': Boolean,
  'help': Boolean,
  'init': Boolean,
  'clear': Boolean,
  'start': Date,
  'end': Date
};
var shortHands = {
  'h': ['--help'],
  'i': ['--init'],
  'c': ['--clear'],
  'k': ['--key'],
  'from': ['--start'],
  's': ['--start'],
  'to': ['--end'],
  'e': ['--end']
};
var description = {
  'authenticate': ' Store your API credentials and exit',
  'help': ' Display this usage text and exit',
  'init': ' Create the index and exit',
  'clear': ' Clear all data in the index',
  'start': ' Start date as YYYY-MM-DD',
  'end': ' End date as YYYY-MM-DD'
};
var options = nopt(knownOpts, shortHands, process.argv, 2);
var firstDate = options['start'] || null;
var lastDate = options['end'] || new Date();
var conf;

var lock = '/tmp/correl8-googlefit-lock';
lockFile.lock(lock, {}, function(er) {
  if (er) {
    console.error('Lockfile ' + lock + ' exists!');
  }
  if (options['help']) {
    console.log('Usage: ');
    console.log(noptUsage(knownOpts, shortHands, description));
  }
  else if (options['authenticate']) {
    var config = {};
    prompt.start();
    prompt.message = '';
    var promptProps = {
      properties: {
        clientId: {
          description: 'Enter your Last.fm API key'.magenta
        },
        clientSecret: {
          description: 'Enter your Last.fm API shared secret'.magenta
        },
        username: {
          description: 'Enter your Last.fm username (Registered To)'.magenta
        }
      }
    }
    prompt.get(promptProps, function (err, result) {
      if (err) {
        console.trace(err);
      }
      else {
        config = result;
        console.log(config);
        c8.config(config).then(function(){
          console.log('Configuration stored.');
        }).catch(function(error) {
          console.trace(error);
        });
      }
    });
  }
  else if (options['clear']) {
    c8.clear().then(function() {
      console.log('Index cleared.');
      c8.release();
    }).catch(function(error) {
      console.trace(error);
      c8.release();
    });
  }
  else if (options['init']) {
    c8.init(recentFields).then(function() {
      console.log('Index initialized.');
    }).catch(function(error) {
      console.trace(error);
      c8.release();
    });
  }
  else {
    importData();
  }
  lockFile.unlock(lock, function (er) {
    if (er) {
      console.error('Cannot release lockfile ' + lock + '!');
    }
  })
});

function importData() {
  c8.config().then(function(res) {
    if (res.hits && res.hits.hits && res.hits.hits[0] && res.hits.hits[0]._source['clientSecret']) {
      conf = res.hits.hits[0]._source;
      var lfm = new LastfmAPI({
        'api_key' : conf.clientId,
        'secret' : conf.clientSecret
      });
      c8.search({
        _source: ['timestamp'],
        size: 1,
        sort: [{'timestamp': 'desc'}],
      }).then(function(response) {
        if (firstDate) {
          console.log('Setting first time to ' + firstDate);
        }
        else if (response && response.hits && response.hits.hits && response.hits.hits[0] && response.hits.hits[0]._source && response.hits.hits[0]._source.timestamp) {
          var d = new Date(parseInt(response.hits.hits[0]._source.timestamp));
          firstDate = new Date(d.getTime() + 1);
          console.log('Setting first time to ' + firstDate);
        }
        else {
          firstDate = new Date();
          firstDate.setTime(firstDate.getTime() - MS_IN_DAY);
          console.warn('No previously indexed data, setting first time to ' + firstDate);
        }
        if (lastDate.getTime() >= (firstDate.getTime() + (MS_IN_DAY * MAX_DAYS))) {
          lastDate = new Date();
          lastDate.setTime(firstDate.getTime() + (MS_IN_DAY * MAX_DAYS) - 1000);
          console.warn('Max time range ' + MAX_DAYS + ' days, setting end time to ' + lastDate);
        }
        var params = {
          limit: 200,
          user: conf.username,
          from: firstDate.getTime()/1000,
          to: lastDate.getTime()/1000,
          extended: true,
          page: 1
        };
        // console.log(JSON.stringify(params));
        getRecent(lfm, params);
      });
    }
  });
}

function getRecent(lfm, params) {
  lfm.user.getRecentTracks(params, function(err, recentTracks) {
    if (err) {
      console.trace(err);
    }
    console.log(recentTracks.track.length + ' tracks');
    var bulk = [];
    if (params.page < recentTracks['@attr'].totalPages) {
      params.page++;
      getRecent(lfm, params);
    }
    if (!recentTracks || !recentTracks.track || !recentTracks.track.length) {
      return;
    }
    for (var i=0; i<recentTracks.track.length; i++) {
      var track = recentTracks.track[i];
      track.timestamp = track.date.uts * 1000;
      // console.log(JSON.stringify(track));
      var id = track.date.uts + track.artist['#text'] + track.name;
      bulk.push({index: {_index: c8._index, _type: c8._type, _id: id}});
      bulk.push(track);
    }
    // console.log(JSON.stringify(bulk, null, 2));
    c8.bulk(bulk).then(function(result) {
      console.log('Indexed ' + result.items.length + ' items in ' + result.took + ' ms.');
      bulk = null;
    }).catch(function(error) {
      console.trace(error);
      bulk = null;
    });
  });
}
