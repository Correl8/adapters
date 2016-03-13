var fs = require('fs');
var path = require('path');
var request = require('request');
var prompt = require('prompt');
var correl8 = require('correl8');
var nopt = require('nopt');
var noptUsage = require('nopt-usage');
var moment = require('moment');
var express = require('express');
var google = require('googleapis');
var googleAuth = require('google-auth-library');

var eventType = 'gcal-event';
var c8 = correl8(eventType);
var defaultPort = 3456;
var defaultUrl = 'http://localhost:' + defaultPort + '/';
var MS_IN_DAY = 24 * 60 * 60 * 1000;
var MAX_DAYS = 10;
var MAX_EVENTS = 100;
var SCOPES = ['https://www.googleapis.com/auth/calendar.readonly'];

// 
var fields = {
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
};

var knownOpts = {
  'authenticate': path,
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
  'authenticate': ' Google API client credentials file (e.g. client_id.json)',
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

if (options['help']) {
  console.log('Usage: ');
  console.log(noptUsage(knownOpts, shortHands, description));
}
else if (options['authenticate']) {
  var conf = {};
  fs.readFile(options['authenticate'], function (err, content) {
    if (err) {
      console.log('Error loading client secret file: ' + err);
      return;
    }
    conf = JSON.parse(content);
    // console.log(conf);
    var auth = new googleAuth();
    var clientSecret = conf.installed.client_secret;
    var clientId = conf.installed.client_id;
    var redirectUrl = conf.installed.redirect_uris[0];
    // var redirectUrl = defaultUrl;
    var oauth2Client = new auth.OAuth2(clientId, clientSecret, redirectUrl);
    var authUrl = oauth2Client.generateAuthUrl({
      access_type: 'offline',
      scope: SCOPES
    });
    console.log('Authorize this app by visiting this url\n', authUrl, '\n\n');
    prompt.start();
    prompt.message = '';
    var promptProps = {
      properties: {
        code: {
          description: 'Enter the code shown on page'.magenta
        },
      }
    }
    prompt.get(promptProps, function (err, result) {
      if (err) {
        console.trace(err);
      }
      else {
        oauth2Client.getToken(result.code, function(err, token) {
          if (err) {
            console.log('Error while trying to retrieve access token', err);
            return;
          }
          conf.credentials = token;
          // console.log(conf);

          c8.config(conf).then(function(){
            console.log('Access credentials saved.');
            c8.release();
            process.exit;
          });

        });
      }
    });
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
  c8.init(fields).then(function() {
    console.log('Index initialized.');
  }).catch(function(error) {
    console.trace(error);
    c8.release();
  });
}
else {
  importData();
}

function importData() {
  c8.config().then(function(res) {
    if (res.hits && res.hits.hits && res.hits.hits[0] && res.hits.hits[0]._source['credentials']) {
      conf = res.hits.hits[0]._source;
      var clientSecret = conf.installed.client_secret;
      var clientId = conf.installed.client_id;
      var redirectUrl = conf.installed.redirect_uris[0];
      var auth = new googleAuth();
      var oauth2Client = new auth.OAuth2(clientId, clientSecret, redirectUrl);
      oauth2Client.credentials = conf.credentials;
      // oauth2Client.setToken({conf.credentials});
      // console.log(oauth2Client);
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
        oauth2Client.credentials = conf.credentials;
        storeEvents(oauth2Client);
      });
    }
  });
}

function storeEvents(auth) {
  var calendar = google.calendar('v3');
  calendar.calendarList.list({auth: auth}, function(err, resp) {
    if (err) {
      console.log('The calendar API returned an error when reading calendars: ' + err);
      return;
    }
    // console.log(resp.items);
    for (var i=0; i<resp.items.length; i++) {
      var calId = resp.items[i].id;
      // console.log('Reading calendar ' + calId);
      calendar.events.list({
        auth: auth,
        calendarId: calId,
        timeMin: firstDate.toISOString(),
        timeMax: lastDate.toISOString(),
        maxResults: MAX_EVENTS,
        singleEvents: true,
        orderBy: 'startTime'
      }, function(error, response) {
        if (error) {
          if (error == 'Error: Not Found') {
            // silently ignore
            return;
          }
          console.log('The calendar API returned an error when reading events: ' + error);
          return;
        }
        // console.log(response);
        var cal = response.summary;
        var events = response.items;
        if (!events || events.length === 0) {
          console.log(cal + ': no events found between '+ firstDate + ' and ' + lastDate + '.');
          return;
        }
        console.log(cal + ': found ' + events.length + ' events:');
        var bulk = [];
        for (var j=0; j<events.length; j++) {
          var event = events[j];
          var start = new Date(event.start.dateTime || event.start.date);
          var end = new Date(event.end.dateTime || event.end.date);
          event.calendar = cal;
          event.timestamp = start;
          event.duration = (end.getTime() - start.getTime())/1000;
            console.log('%s %d: %s - %s (%d s)', cal, j+1, start, event.summary, event.duration);
          bulk.push({index: {_index: c8._index, _type: c8._type, _id: event.id}});
          bulk.push(event);
        }
        // console.log(bulk);
        c8.bulk(bulk).then(function(result) {
          console.log('Indexed ' + result.items.length + ' events in ' + result.took + ' ms.');
          bulk = null;
        }).catch(function(error) {
          console.trace(error);
          bulk = null;
        });
      });
    };
  });
}
