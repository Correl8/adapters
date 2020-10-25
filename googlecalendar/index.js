const fs = require('fs');
const {google} = require('googleapis');
const prompt = require('prompt');
const moment = require('moment');

let adapter = {};

const MS_IN_DAY = 24 * 60 * 60 * 1000;
const MAX_DAYS = 366;
const MAX_EVENTS = 1000;
const SCOPES = ['https://www.googleapis.com/auth/calendar.readonly'];

adapter.sensorName = 'googlecalendar-event';

adapter.types = [
  {
    name: 'googlecalendar-event',
    fields: {
      "@timestamp": "date",
      "ecs": {
        "version": 'keyword'
      },
      "event": {
        "created": "date",
        "dataset": "keyword",
        "duration": "long",
        "end": "date",
        "ingested": "date",
        "kind": "keyword",
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
        "start_hour": 'long',
        "id": 'long',
        "name": 'keyword',
      },
      "calendarevent": {
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

adapter.storeConfig = (c8, result) => {
  let conf = {};
  fs.readFile(result['authconfig'], (err, content) => {
    if (err) {
      console.log('Error loading client secret file: ' + err);
      return;
    }
    conf = JSON.parse(content);
    // console.log(conf);
    const auth = google.auth;
    const clientSecret = conf.installed.client_secret;
    const clientId = conf.installed.client_id;
    const redirectUrl = conf.installed.redirect_uris[0];
    const oauth2Client = new auth.OAuth2(clientId, clientSecret, redirectUrl);
    const authUrl = oauth2Client.generateAuthUrl({
      access_type: 'offline',
      scope: SCOPES
    });
    console.log('Authorize this app by visiting this url\n', authUrl, '\n\n');
    prompt.start();
    prompt.message = '';
    const promptProps = {
      properties: {
        code: {
          description: 'Enter the code shown on page'.magenta
        },
      }
    }
    prompt.get(promptProps, (err, result) => {
      if (err) {
        console.trace(err);
      }
      else {
        oauth2Client.getToken(result.code, async (err, token) => {
          if (err) {
            console.log('Error while trying to retrieve access token', err);
            return;
          }
          conf.credentials = token;
          // console.log(conf);

          await c8.config(conf);
          console.log('Access credentials saved.');
          c8.release();
          process.exit;
        });
      }
    });
  });
}

adapter.importData = (c8, conf, opts) => {
  return new Promise((fulfill, reject) => {
    const clientSecret = conf.installed.client_secret;
    const clientId = conf.installed.client_id;
    const redirectUrl = conf.installed.redirect_uris[0];
    const auth = google.auth;
    const oauth2Client = new auth.OAuth2(clientId, clientSecret, redirectUrl);
    oauth2Client.credentials = conf.credentials;
    const calendar = google.calendar('v3');
    calendar.calendarList.list({auth: oauth2Client}, async (err, calResponse) => {
      if (err) {
        reject(new Error('The calendar API returned an error when reading calendars: ' + err));
        return;
      }
      const params = [];
      const calendars = [];
      // console.log(calResponse);
      // process.exit();
      for (var i=0; i<calResponse.data.items.length; i++) {
        const calId = calResponse.data.items[i].id;
        calendars[i] = calId;
        // console.log(calId);
        params[i] = {
          _source: ['@timestamp', 'calendarevent.calendarId'],
          query: {
            match: {
              'calendarevent.calendarId': calId
            }
          },
          size: 1,
          sort: [{'@timestamp': 'desc'}],
        };
      }
      try {
        const resp = await c8.msearch(params);
        let response = c8.trimBulkResults(resp);
        if (!response || !response.responses) {
          console.log(response);
          return;
        }
        // console.log(response);
        for (var i=0; i<response.responses.length; i++) {
          let calId = calendars[i];
          const res = response.responses[i];
          if (res.error) {
            console.log(res.error);
            continue;
          }
          // console.log(res.hits.hits);
          let firstDate, lastDate;
          if (opts.firstDate) {
            firstDate = opts.firstDate;
            // console.log('Setting first time for ' + calId + ' to ' + firstDate);
          }
          else if (res.hits.hits && res.hits.hits[0] && res.hits.hits[0]._source['@timestamp']) {
            // console.log(res.hits.hits[0]);
            // console.log(res.hits.hits[0]._source);
            let d = new Date(res.hits.hits[0]._source['@timestamp']);
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
          const searchOpts = {
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
          calendar.events.list(searchOpts, async (error, response) => {
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
            let cal = response.data.summary;
            let events = response.data.items;
            if (!events || events.length === 0) {
              console.log(cal + ': no events found between '+ firstDate + ' and ' + lastDate + '.');
              return;
            }
            // console.log(cal + ': found ' + events.length + ' events:');
            let bulk = [];
            for (var j=0; j<events.length; j++) {
              let event = events[j];
              const start = moment(event.start.dateTime || event.start.date);
              const end = moment(event.end.dateTime || event.end.date);
              const created = moment(event.created).format();
              event.calendar = cal;
              event.analyzedSummary = event.summary; // will be treated as full text, not string
              event.calendarId = calId;
              event.durationSeconds = Math.round(end.diff(start) / 1E3);
              // console.log('%s %d: %s - %s (%d s)', cal, j+1, start, event.summary, event.duration);
              // the htmlLink is the only permanent unique identifier for the event!
              let data = {
                "@timestamp": start,
                "ecs": {
                  "version": "1.6.0"
                },
                "event": {
                  "created": created,
                  "dataset": "google.calendar",
                  "ingested": new Date(),
                  "kind": "event",
                  "module": cal,
                  "original": JSON.stringify(events[j]),
                  "start":  start.format(),
                  "end":  end.format(),
                  "duration": event.durationSeconds/1E9
                },
                "time_slice": time2slice(start),
                "date_details": {
                  "year": start.format('YYYY'),
                  "month": {
                    "number": start.format('M'),
                    "name": start.format('MMMM'),
                  },
                  "week_number": start.format('W'),
                  "day_of_year": start.format('DDD'),
                  "day_of_month": start.format('D'),
                  "day_of_week": {
                    "number": start.format('d'),
                    "name": start.format('dddd'),
                  }
                },
                "calendarevent": event
              };
              bulk.push({index: {_index: c8._index, _id: event.htmlLink}});
              bulk.push(data);
            }
            // console.log(bulk);
            if (bulk.length > 0) {
              const response = await c8.bulk(bulk);
              let result = c8.trimBulkResults(response);
              if (result.errors) {
                const messages = [];
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
            }          
            else {
              // fulfill('No data available');
              console.log('No data available');
            }
          });
        }
        // fulfill('Checked ' + response.responses.length + ' calendars.');
        console.log('Checked ' + response.responses.length + ' calendars.');
      }
      catch (error) {
        reject(error);
        bulk = null;
      }
    });
  });
};

function time2slice(t) {
  // creates a time_slice from a moment object
  let time_slice = {};
  let hour = t.format('H');
  let minute = (5 * Math.floor(t.format('m') / 5 )) % 60;
  time_slice.name = [hour, minute].join(':');
  if (minute == 5) {
    time_slice.name = [hour, '0' + minute].join(':');
  }
  else if (minute == 0) {
    time_slice.name += '0';
  }
  let idTime = parseInt(hour) + parseInt(minute)/60;
  time_slice.id = Math.round((idTime + (idTime >= 4 ? -4 : 20)) * 12);
  return time_slice;
}

module.exports = adapter;
