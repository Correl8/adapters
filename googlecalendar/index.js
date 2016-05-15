var google = require('googleapis');
var googleAuth = require('google-auth-library');

var adapter = {};

var eventType = 'gcal-event';
var c8 = correl8(eventType);
var MS_IN_DAY = 24 * 60 * 60 * 1000;
var MAX_DAYS = 10;
var MAX_EVENTS = 100;
var SCOPES = ['https://www.googleapis.com/auth/calendar.readonly'];

adapter.sensorName = 'googlecalendar-event';

adapter.types = [
  {
    name: 'googlecalendar-event',
    fields: {
      "timestamp": "date",
      "calendar": "string",
      "duration": "integer",
      "kind": "string",
      "etag": "string",
      "id": "string",
      "status": "string",
      "htmlLink": "string",
      "created": "date",
      "updated": "date",
      "summary": "string",
      "description": "string",
      "location": "string",
      "colorId": "string",
      "creator": {
        "id": "string",
        "email": "string",
        "displayName": "string",
        "self": "boolean"
      },
      "organizer": {
        "id": "string",
        "email": "string",
        "displayName": "string",
        "self": "boolean"
      },
      "start": {
        "date": "date",
        "dateTime": "date",
        "timeZone": "string"
      },
      "end": {
        "date": "date",
        "dateTime": "date",
        "timeZone": "string"
      },
      "endTimeUnspecified": "boolean",
      "recurrence": "string",
      "recurringEventId": "string",
      "originalStartTime": {
        "date": "date",
        "dateTime": "date",
        "timeZone": "string"
      },
      "transparency": "string",
      "visibility": "string",
      "iCalUID": "string",
      "sequence": "integer",
      "attendees": {
        "id": "string",
        "email": "string",
        "displayName": "string",
        "organizer": "boolean",
        "self": "boolean",
        "resource": "boolean",
        "optional": "boolean",
        "responseStatus": "string",
        "comment": "string",
        "additionalGuests": "integer"
      },
      "attendeesOmitted": "boolean",
      "hangoutLink": "string",
      "gadget": {
        "type": "string",
        "title": "string",
        "link": "string",
        "iconLink": "string",
        "width": "integer",
        "height": "integer",
        "display": "string",
      },
      "anyoneCanAddSelf": "boolean",
      "guestsCanInviteOthers": "boolean",
      "guestsCanModify": "boolean",
      "guestsCanSeeOtherGuests": "boolean",
      "privateCopy": "boolean",
      "locked": "boolean",
      "reminders": {
        "useDefault": "boolean",
        "overrides": {
          "method": "string",
          "minutes": "integer"
        }
      },
      "source": {
        "url": "string",
        "title": "string"
      },
      "attachments": {
        "fileUrl": "string",
        "title": "string",
        "mimeType": "string",
        "iconLink": "string",
        "fileId": "string"
      }
    }
  }
];

adapter.promptProps = {
  properties: {
    code: {
      description: 'Configuration file'.magenta,
      default: 'client_secret.json'
    },
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
        // console.log(JSON.stringify(repo));
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
          if (bulk.length > 0) {
            return c8.bulk(bulk).then(function(result) {
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

module.exports = adapter;
