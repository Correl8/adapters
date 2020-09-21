var fs = require('fs');
var google = require('googleapis');
var googleAuth = require('google-auth-library');
var prompt = require('prompt');

var adapter = {};

var MS_IN_DAY = 24 * 60 * 60 * 1000;
var MAX_DAYS = 366;
var MAX_EVENTS = 1000;
var SCOPES = ['https://www.googleapis.com/auth/calendar.readonly'];

adapter.sensorName = 'googlecalendar-event';

adapter.types = [
  {
    name: 'googlecalendar-event',
    fields: {
      "timestamp": "date",
      "calendar": "string",
      "calendarId": "string",
      "duration": "integer",
      "kind": "string",
      "etag": "string",
      "id": "string",
      "status": "string",
      "htmlLink": "string",
      "created": "date",
      "updated": "date",
      "summary": "string",
      "analyzedSummary": "text",
      "description": "text",
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
    authconfig: {
      description: 'Configuration file'.magenta,
      default: 'client_secret.json'
    },
  }
};

adapter.storeConfig = function(c8, result) {
  var conf = {};
  fs.readFile(result['authconfig'], function (err, content) {
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

adapter.importData = function(c8, conf, opts) {
  return new Promise(function (fulfill, reject){
    var clientSecret = conf.installed.client_secret;
    var clientId = conf.installed.client_id;
    var redirectUrl = conf.installed.redirect_uris[0];
    var auth = new googleAuth();
    var oauth2Client = new auth.OAuth2(clientId, clientSecret, redirectUrl);
    oauth2Client.credentials = conf.credentials;
    var calendar = google.calendar('v3');
    calendar.calendarList.list({auth: oauth2Client}, function(err, resp) {
      if (err) {
        reject(new Error('The calendar API returned an error when reading calendars: ' + err));
        return;
      }
      var params = [];
      var calendars = [];
      for (var i=0; i<resp.items.length; i++) {
        var calId = resp.items[i].id;
        calendars[i] = calId;
        // console.log(calId);
        params[i] = {
          _source: ['timestamp', 'calendarId'],
          query: {
            match: {
              calendarId: calId
            }
          },
          size: 1,
          sort: [{'timestamp': 'desc'}],
        };
      }
      c8.msearch(params).then(function(resp) {
        let response = c8.trimBulkResults(resp);
        if (!response || !response.responses) {
          console.log(response);
          return;
        }
        // console.log(response);
        for (var i=0; i<response.responses.length; i++) {
          let calId = calendars[i];
          var res = response.responses[i];
          var firstDate, lastDate;
          if (opts.firstDate) {
            firstDate = opts.firstDate;
            // console.log('Setting first time for ' + calId + ' to ' + firstDate);
          }
          else if (res.hits.hits && res.hits.hits[0] && res.hits.hits[0]._source.timestamp) {
            // console.log(res.hits.hits[0]._source);
            var d = new Date(res.hits.hits[0]._source.timestamp);
            firstDate = new Date(d.getTime() + 1);
            // console.log('Setting first time for ' + calId + '  to ' + firstDate);
          }
          else {
            firstDate = new Date();
            firstDate.setTime(firstDate.getTime() - MS_IN_DAY);
            console.warn('No previously indexed data, setting first time for ' + calId + '  to ' + firstDate);
          }
          lastDate = opts.lastDate || new Date();
          if (lastDate.getTime() >= (firstDate.getTime() + (MS_IN_DAY * MAX_DAYS))) {
            lastDate.setTime(firstDate.getTime() + (MS_IN_DAY * MAX_DAYS) - 1);
            console.warn('Max time range ' + MAX_DAYS + ' days, setting end time for ' + calId + '  to ' + lastDate);
          }
          oauth2Client.credentials = conf.credentials;
          var searchOpts = {
            auth: oauth2Client,
            calendarId: calId,
            timeMin: firstDate.toISOString(),
            timeMax: lastDate.toISOString(),
            maxResults: MAX_EVENTS,
            singleEvents: true,
            orderBy: 'startTime'
          };
          // console.log('Reading calendar ' + calId);
          // console.log(searchOpts);
          calendar.events.list(searchOpts, function(error, response) {
            // console.log(response);
            if (error) {
              // console.log(error);
              if (error == 'Error: Not Found') {
                // console.log(cal + ': no events found between '+ firstDate + ' and ' + lastDate + '.');
                // silently ignore
                return;
              }
              reject(new Error('The calendar API returned an error when reading events: ' + error));
              return;
            }
            // console.log(response);
            let cal = response.summary;
            let events = response.items;
            if (!events || events.length === 0) {
              console.log(cal + ': no events found between '+ firstDate + ' and ' + lastDate + '.');
              return;
            }
            // console.log(cal + ': found ' + events.length + ' events:');
            var bulk = [];
            for (var j=0; j<events.length; j++) {
              var event = events[j];
              var start = new Date(event.start.dateTime || event.start.date);
              var end = new Date(event.end.dateTime || event.end.date);
              event.calendar = cal;
              event.analyzedSummary = event.summary; // will be treated as full text, not string
              event.calendarId = calId;
              event.timestamp = start;
              event.duration = (end.getTime() - start.getTime())/1000;
              // console.log('%s %d: %s - %s (%d s)', cal, j+1, start, event.summary, event.duration);
              // the htmlLink is the only permanent unique identifier for the event!
              bulk.push({index: {_index: c8._index, _type: c8._type, _id: event.htmlLink}});
              bulk.push(event);
            }
            // console.log(bulk);
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
                  bulk = null;
                  return;
                }
                // fulfill('Indexed ' + result.items.length + ' event documents in ' + result.took + ' ms.');
                console.log(cal + ': Indexed ' + result.items.length + ' event documents in ' + result.took + ' ms.');
                bulk = null;
              }).catch(function(error) {
                reject(error);
                bulk = null;
              });
            }
            else {
              // fulfill('No data available');
              console.log('No data available');
            }
          });
        }
        // fulfill('Checked ' + response.responses.length + ' calendars.');
        console.log('Checked ' + response.responses.length + ' calendars.');
      }).catch(function(error) {
        reject(error);
        bulk = null;
      });
    });
  });
};

module.exports = adapter;
