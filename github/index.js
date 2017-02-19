var GitHubApi = require("github");
var passwoid = require('passwoid');

var adapter = {};

var SCOPES = ['user', 'repo'];
var CLIENT_ID = "Correl8-adapter-" + passwoid(8);
var CLIENT_SECRET = passwoid(40);
var API_NOTE = "Correl8 adapter created on " + new Date().toISOString();
var API_NOTE_URL = "http://correl8.me/";
var MS_IN_DAY = 24 * 60 * 60 * 1000;
var MAX_DAYS = 365;
var MAX_COMMITS = 100;
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

adapter.sensorName = 'github-commit';

adapter.types = [
  {
    name: 'github-commit',
    fields: {
      timestamp: 'date'
    }
  }
];

adapter.promptProps = {
  properties: {
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
};

adapter.storeConfig = function(c8, result) {
  var config = {
    user: result.user,
    clientId: CLIENT_ID,
    clientSecret: CLIENT_SECRET
  };
  var authOpts = {
    scopes: SCOPES,
    note: API_NOTE,
    note_url: API_NOTE_URL,
  };
  if (result.otp) {
    authOpts.headers = {"X-GitHub-OTP": result.otp};
  }
  github.authenticate({
    type: "basic",
    username: result.user,
    password: result.password
  });
  console.log(c8.config);
  console.log(result);
  github.authorization.create(authOpts, function(err, res) {
    if (err) {
      console.trace(err);
    }
    else if (res.token) {
      config.token = res.token;
      // console.log(config);
      return c8.config(config).then(function(){
        console.log('Configuration stored.');
      }).catch(function(error) {
        console.trace(error);
      });
    }
    else {
      console.error('Authorization failed');
      console.trace(res);
    }
  });
}

adapter.importData = function(c8, conf, opts) {
  return new Promise(function (fulfill, reject){
    github.authenticate({
      type: "oauth",
      token: conf.token,
    });
    return c8.search({
      _source: ['timestamp'],
      size: 1,
      sort: [{'timestamp': 'desc'}],
    }).then(function(response) {
      var resp = c8.trimResults(response);
      var firstDate, lastDate;
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
        firstDate.setTime(firstDate.getTime() - MS_IN_DAY);
        console.warn('No previously indexed data, setting first time to ' + firstDate);
      }
      if (opts.lastDate) {
        lastDate = new Date(opts.lastDate);
      }
      else {
        lastDate = new Date();
      }
      if (lastDate.getTime() >= (firstDate.getTime() + (MS_IN_DAY * MAX_DAYS))) {
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
          // console.log(JSON.stringify(repo.name));
          var msg = {
            user: repo.owner.login,
            repo: repo.name,
            author: conf.user,
            since: firstDate.toISOString(),
            until: lastDate.toISOString(),
            per_page: 100
          };
          // console.log(msg);
          github.repos.getCommits(msg, function(err, subres) {
            if (err) {
              // don't bother with "not found" and "repo empty" messages
              if ((err.code != 404) && (err.code != 409)) {
                console.error(err.code + ': ' + err.message);
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
              }
              commit.timestamp = commit.commit.author.date;
              bulk.push({index: {_index: c8._index, _type: c8._type, _id: commit.sha}});
              bulk.push(commit);
            }
            // console.log(JSON.stringify(bulk, null, 2));
            // console.log(bulk.length);
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
                  c8.release();
                  return;
                }
                console.log('Indexed ' + result.items.length + ' commit documents in ' + result.took + ' ms.');
              }).catch(function(error) {
                console.error(error);
                c8.release();
              });
            }
            else {
              // console.log('No data available');
            }
          });
        }
        fulfill('Checked ' + res.length + ' repositories');
      });
    }).catch(function(error) {
      console.trace(error);
      bulk = null;
    });
  });
}

module.exports = adapter;
