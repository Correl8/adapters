var fs = require('fs');
var path = require('path');
var request = require('request');
var prompt = require('prompt');
var correl8 = require('correl8');
var lockFile = require('lockfile');
var nopt = require('nopt');
var noptUsage = require('nopt-usage');
var moment = require('moment');
var GitHubApi = require("github");
var lockFile = require('lockfile');

var github = new GitHubApi({
    // required
    version: "3.0.0",
    // optional
    debug: false,
    protocol: "https",
    host: "api.github.com", //
    timeout: 5000,
    headers: {
	"user-agent": "Correl8"
    }
});

var commitType = 'github-commit';
var c8 = correl8(commitType);
var SCOPES = ['user', 'repo'];
var MS_IN_DAY = 24 * 60 * 60 * 1000;
var MAX_DAYS = 365;
var MAX_COMMITS = 100;

var commitFields = {
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
  'authenticate': ' Store your Github API credentials and exit',
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

var lock = '/tmp/correl8-github-lock';
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
          description: 'Enter your Github client ID'.magenta
        },
        clientSecret: {
          description: 'Enter your Github client secret'.magenta
        },
        user: {
          description: 'Enter your Github username'.magenta
        },
        password: {
          description: 'Enter your Github password (not stored)'.magenta,
          hidden: true
        },
        otp: {
          description: 'One  time password (if two-factor authentication)'.magenta
        }
      }
    }
    prompt.get(promptProps, function (err, result) {
      if (err) {
        console.trace(err);
      }
      else {
        config = {
          user: result.user,
          clientId: result.clientId,
          clientSecret: result.clientSecret
        };
	var authOpts = {
          scopes: SCOPES,
          note: "Correl8 adapter",
          note_url: "http://corel8.me/",
        };
        if (result.otp) {
          authOpts.headers = {"X-GitHub-OTP": result.otp};
        }
        github.authenticate({
          type: "basic",
          username: result.user,
          password: result.password
        });
        github.authorization.create(authOpts, function(err, res) {
	  if (err) {
            console.warn(err);
	  }
          else if (res.token) {
            config.token = res.token;
            // console.log(config);
            c8.config(config).then(function(){
              console.log('Configuration stored.');
            }).catch(function(error) {
              console.trace(error);
            });
          }
          else {
            console.error('Autorization failed');
            console.trace(res);
          }
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
    c8.init(commitFields).then(function() {
      console.log('Index initialized.');
      // c8.release();
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
    // console.log(res.hits.hits);
    if (res.hits && res.hits.hits && res.hits.hits[0] && res.hits.hits[0]._source['token']) {
      conf = res.hits.hits[0]._source;
      // var clientSecret = conf.clientSecret;
      // var clientId = conf.clientId;
      github.authenticate({
        type: "oauth",
        token: conf.token,
        // key: clientId,
        // secret: clientSecret
      });
      c8.search({
        fields: ['timestamp'],
        size: 1,
        sort: [{'timestamp': 'desc'}],
      }).then(function(response) {
        if (firstDate) {
          console.log('Setting first time to ' + firstDate);
        }
        else if (response && response.hits && response.hits.hits && response.hits.hits[0] && response.hits.hits[0].fields && response.hits.hits[0].fields.timestamp) {
          var d = new Date(response.hits.hits[0].fields.timestamp);
          firstDate = new Date(d.getTime() + 1);
          // firstDate = d;
          console.log('Setting first time to ' + firstDate);
        }
        else {
          firstDate = new Date();
          firstDate.setTime(firstDate.getTime() - MS_IN_DAY);
          console.warn('No previously indexed data, setting first time to ' + firstDate);
        }
        if (lastDate.getTime() >= (firstDate.getTime() + (MS_IN_DAY * MAX_DAYS))) {
          lastDate = new Date();
          lastDate.setTime(firstDate.getTime() + (MS_IN_DAY * MAX_DAYS) - 1);
          console.warn('Max time range ' + MAX_DAYS + ' days, setting end time to ' + lastDate);
        }
        github.repos.getAll({per_page: 100}, function(err, res) {
          if (err) {
            console.error(err);
            return;
          }
          for (var i=0; i<res.length; i++) {
            var repo = res[i];
            // console.log(repo.name);
            var msg = {
              repo: repo.name,
              user: conf.user,
              since: firstDate.toISOString(),
              until: lastDate.toISOString(),
              per_page: 100
            };
            // console.log(msg);
            github.repos.getCommits(msg, function(err, subres) {
              if (err) {
                if (err.code != 404) {
                  console.error(err.message);
                }
                return;
              }
              // console.log(JSON.stringify(subres[0], null, 2));
              // console.log(subres.length);
              var bulk = [];
              for (var j=0; j<subres.length; j++) {
                var commit = subres[j];
                var match;
                if (match = commit.url.match(/github\.com\/repos\/(.*?)\/commits/)) {
                  var repo = match[1].split('/');
/*
                  commit.repo = {
                    owner: repo[0],
                    name: repo[1],
                    full_name: match[1]
		  };
*/
                }
                // var commits = event.payload.commits;
                // console.log(JSON.stringify(commit, null, 2));
		// process.exit();
                commit.timestamp = commit.commit.author.date;
                // console.log(commit);
                bulk.push({index: {_index: c8._index, _type: c8._type, _id: commit.sha}});
                bulk.push(commit);
              }
              // console.log(JSON.stringify(bulk, null, 2));
              if (bulk.length > 0) {
                c8.bulk(bulk).then(function(result) {
                  console.log('Indexed ' + result.items.length + ' commits in ' + result.took + ' ms.');
                  bulk = null;
                }).catch(function(error) {
                  console.trace(error);
                  bulk = null;
                });
              }
            });
          }
        });
      });
    }
    else {
      console.log('Authenticate first!\nnode ' + process.argv[1] + ' --authenticate');
    }
  });
}

function storeData(auth) {
  var dsNames = {};
  var dTypes = {};
  var devices = {};
  var fit = google.fitness('v1');
  fit.users.dataSources.list({auth: auth, userId: 'me'}, function(err, resp) {
    if (err) {
      console.err('The fitness API returned an error when reading data sources: ' + err);
      return;
    }
    // console.log(resp);
    for (var i=0; i<resp.dataSource.length; i++) {
      var dsId = resp.dataSource[i].dataStreamId;
      dsNames[dsId] = resp.dataSource[i].dataStreamName;
      dTypes[dsId] = resp.dataSource[i].dataType;
      devices[dsId] = resp.dataSource[i].device;
      // console.log('Reading stream ' + dsId);
/*
      var params = {
        auth: auth,
        userId: 'me',
        aggregateBy: [{dataTypeName: dtName}],
        startTimeMillis: firstDate.getTime(),
        endTimeMillis: lastDate.getTime(),
        bucketBySession: {minDurationMillis: 60 * 1000},
      };
      console.log(params);
      fit.users.dataset.aggregate(params, function(err, resp) {
        if (err) {
          console.log('The fitness API returned an error when reading aggregated sessions: ' + err);
          return;
        }
        console.log(resp);
      });
      continue;
*/
      var datasetId = (firstDate.getTime() * 1000000).toString() + '-' +
          (((lastDate.getTime() + 1) * 1000000)-1).toString(); // don't miss a ns
      var params = {
        auth: auth,
        userId: 'me',
        dataSourceId: dsId,
        datasetId: datasetId
      }
      // console.log(params);
      fit.users.dataSources.datasets.get(params, function(err, resp) {
        if (err) {
          console.err('The fitness API returned an error when reading data set: ' + err);
          return;
        }
        if (resp.point && resp.point.length > 0) {
          var dsId = resp.dataSourceId;
          var dType = dTypes[dsId];
          var dsName = dsNames[dsId];
          var device = devices[dsId];
          // console.log(dType.name + ': ' + resp.point[0].value);
          // console.log(resp.point[0].value);
          // console.log(resp);
          // console.log(resp.dataSourceId);
          var points = resp.point;
          // console.log(points);
          var bulk = [];
          for (var j=0; j<points.length; j++) {
            var item = points[j];
            var values = {}
            var id = resp.dataSourceId + ':' + dType.name + ':' + item.startTimeNanos;
            values.timestamp = new Date(item.startTimeNanos/1000000);
            values.startTimeNanos = item.startTimeNanos;
            values.endTimeNanos = item.endTimeNanos;
            values.dataSourceName = dsName;
            values.dataType = dType.name;
            // item.dataType = dType;
            var ll = [];
            for (var k=0; k<dType.field.length; k++) {
              if (!points[j].value[k]) {
                 // console.warn('Undefined ' + dType.field[k].name);
                 // console.log(points[j]);
                 continue;
              }
              values[dType.field[k].name] = getValue(points[j].value[k]);
              if (dType.field[k].name == 'latitude') {
                ll[0] = points[j].value[k].fpVal;
              }
              else if (dType.field[k].name == 'longitude') {
                ll[1] = points[j].value[k].fpVal;
              }
            }
            if (ll.length == 2) {
              values.position = ll.join(',');
            }
            values.dataSourceId = resp.dataSourceId;
            if (item.originDataSourceId) {
              values.originDataSourceId = item.originDataSourceId;
            }
            if (device) {
              values.device = device;
            }
            // console.log('%s: %d', dsName, j+1);
            bulk.push({index: {_index: c8._index, _type: c8._type, _id: id}});
            bulk.push(values);
          }
          //  console.log(JSON.stringify(bulk, null, 2));
          c8.bulk(bulk).then(function(result) {
            // console.log('Indexed ' + result.items.length + ' items in ' + result.took + ' ms.');
            bulk = null;
          }).catch(function(error) {
            console.trace(error);
            bulk = null;
          });
        }
        else {
          var sd = new Date(resp.minStartTimeNs/1000);
          var ed = new Date(resp.maxEndTimeNs/1000);
          // console.log('No data between ' + sd.toISOString() + ' and ' + ed.toISOString());
        }
      });
    };
  });
}

function getValue(obj) {
  // what about string types?
  return obj.intVal || obj.fpVal || obj.value;
}
