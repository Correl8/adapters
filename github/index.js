const GithubGraphQLApi = require('node-github-graphql')

const SCOPES = ['read:user', 'repo:status'];
const MS_IN_DAY = 24 * 60 * 60 * 1000;
const MAX_DAYS = 365;
const MAX_COMMITS = 100;

const adapter = {};

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
    token: {
      description: 'Enter your Github Personal access token'.magenta,
      hidden: true
    }
  }
};

adapter.storeConfig = function(c8, result) {
  var config = {
    user: result.user,
    token: result.token
  };
  return c8.config(config).then(function(){
    console.log('Configuration stored.');
  }).catch(function(error) {
    console.trace(error);
  });
}

adapter.importData = function(c8, conf, opts) {
  return new Promise(function (fulfill, reject){
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
      var github = new GithubGraphQLApi({
        token: conf.token,
        promise: Promise,
        userAgent: 'correl8 adapter',
        debug: true
      });
      query = `{
        viewer {
          repositories(first: 30) {
            pageInfo {
              hasNextPage
              endCursor
            }
            edges {
              node {
                name
              }
            }
          }
        }
      }`;
      params = {};
      github.query(query, params).then((res) => {
        console.log(JSON.stringify(res));
        return;
/*
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
        fulfill('Checked ' + res.length + ' repositories');
*/
      }).catch((err) => {reject(new Error(err));});
    }).catch((err) => {reject(new Error(err));});
  });
};

module.exports = adapter;
