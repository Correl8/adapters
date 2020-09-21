var LastfmAPI = require('lastfmapi');

var MS_IN_DAY = 24 * 60 * 60 * 1000;
var MAX_DAYS = 365;

var adapter = {};

adapter.sensorName = 'lastfm';

adapter.types = [
  {
    name: 'lastfm-recent',
    fields: {
      'timestamp': 'date',
      'name': 'string', // text?
      'artist': {
        '#text': 'string',
      },
      'album': {
        '#text': 'string',
      }
    }
  }
];

adapter.promptProps = {
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
};

adapter.storeConfig = function(c8, result) {
  return c8.config(result).then(function(){
    console.log('Configuration stored.');
    c8.release();
  });
}

adapter.importData = function(c8, conf, opts) {
  return new Promise(function (fulfill, reject){
    console.log('Getting first date...');
    var lfm = new LastfmAPI({
      'api_key' : conf.clientId,
      'secret' : conf.clientSecret
    });
    var firstDate, lastDate;
    return c8.type(adapter.types[0].name).search({
      _source: ['timestamp'],
      size: 1,
      sort: [{'timestamp': 'desc'}],
    }).then(function(response) {
      var resp = c8.trimResults(response);
      var firstDate = new Date();
      var lastDate = opts.lastDate || new Date();;
      if (opts.firstDate) {
        firstDate = new Date(opts.firstDate);
        console.log('Setting first time to ' + firstDate);
      }
      else if (resp && resp.timestamp) {
        var d = new Date(resp.timestamp);
        firstDate.setTime(d.getTime() + 1);
        console.log('Setting first time to ' + firstDate);
      }
      else {
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
	getRecent(c8, lfm, params).then(function(message) {
          fulfill(message);
	}).catch(function(error) {
          reject(error);
	});;
    });
  });
}

function getRecent(c8, lfm, params) {
  return new Promise(function (fulfill, reject){
    lfm.user.getRecentTracks(params, function(err, recentTracks) {
      if (err) {
        console.trace(err);
      }
      console.log(recentTracks.track.length + ' tracks');
      var bulk = [];
      if (params.page < recentTracks['@attr'].totalPages) {
        params.page++;
        getRecent(c8, lfm, params);
      }
      if (!recentTracks || !recentTracks.track || !recentTracks.track.length) {
        fulfill('No recent tracks.');
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
          bulk = null;
        });
      }
      else {
        fulfill('No data available');
      }
    });
  });
}

module.exports = adapter;
