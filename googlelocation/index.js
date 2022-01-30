const JSONStream = require('JSONStream');
const {google} = require('googleapis');
const tar = require('tar');
const eos = require('end-of-stream');
const prompt = require('prompt');
const fs = require('fs');
const request = require('request');
const moment = require('moment');

const SCOPES = ['https://www.googleapis.com/auth/drive'];

const MAX_FILES = 1;
// const MAX_ZIP_ENTRIES = 1;
const MAX_BULK_BATCH = 10000;
// const BULK_BATCH_MS = 2500;

var adapter = {};
let startedBatches = 0;
let finishedBatches = 0;

adapter.sensorName = 'googlelocation';

adapter.types = [
  {
    name: adapter.sensorName,
    fields: {
      "@timestamp": "date",
      "ecs": {
        "version": 'keyword'
      },
      "event": {
        "created": "date",
        "dataset": "keyword",
        "ingested": "date",
        "kind": "keyword",
        "module": "keyword",
        "original": "keyword",
        "start": "date",
        "timezone": "keyword",
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
      "position": {
        "timestampMs": 'keyword',
        "geo": {
          "location": "geo_point"
        },
        "accuracy": 'integer',
        "velocity": 'integer',
        "heading": 'integer',
        "altitude": 'integer',
        "verticalAccuracy": 'integer',
      },
    }
  },
  {
    name: adapter.sensorName + '-activity',
    fields: {
      "@timestamp": "date",
      "ecs": {
        "version": 'keyword'
      },
      "event": {
        "created": "date",
        "dataset": "keyword",
        "ingested": "date",
        "kind": "keyword",
        "module": "keyword",
        "original": "keyword",
        "start": "date",
        "timezone": "keyword",
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
      "activity": {
        "time": 'date',
        "type": 'keyword',
        "confidence": 'integer',
      }
    }
  },
  {
    name: adapter.sensorName + '-semantic',
    fields: {
      "@timestamp": "date",
      "ecs": {
        "version": 'keyword'
      },
      "event": {
        "created": "date",
        "duration": "long",
        "end": "date",
        "dataset": "keyword",
        "ingested": "date",
        "kind": "keyword",
        "module": "keyword",
        "original": "keyword",
        "start": "date",
        "timezone": "keyword",
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
      "activity_segment": {
        "start": {
          "geo": {
            "location": "geo_point"
          },
          "sourceInfo": {
            "deviceTag": "keyword"
          },
        },
        "end": {
          "geo": {
            "location": "geo_point"
          },
          "sourceInfo": {
            "deviceTag": "keyword"
          },
        },
        "distance": 'long',
        "activityType": 'keyword',
        "confidence": 'keyword',
        "activities": {
          "activityType": 'keyword',
          "probability": 'float',
        },
        "waypointPath": {
          "waypoints": {
            "geo": {
              "location": "geo_point"
            },
          },
        },
      },
      "place_visit": {
        "accuracyMetres": 'integer',
        "placeId": 'keyword',
        "address": 'keyword',
        "name": 'keyword',
        "semanticType": 'keyword',
        "sourceInfo": {
          "deviceTag": 'keyword',
        },
        "locationConfidence": 'float',
        "placeConfidence": 'keyword',
        "center": {
          "geo": {
            "location": "geo_point"
          },
        },
        "visitConfidence": 'float',
        "otherCandidateLocations": {
          "geo": {
            "location": "geo_point"
          },
          "placeId": 'keyword',
          "locationConfidence": 'float',
        },
        "editConfirmationStatus": 'keyword',
      },
      "position": {
        "geo": {
          "location": "geo_point"
        },
      },
    },
  },
  {
    name: adapter.sensorName + '-rawpath',
    fields: {
      "@timestamp": "date",
      "ecs": {
        "version": 'keyword'
      },
      "event": {
        "created": "date",
        "duration": "long",
        "end": "date",
        "dataset": "keyword",
        "ingested": "date",
        "kind": "keyword",
        "module": "keyword",
        "original": "keyword",
        "start": "date",
        "timezone": "keyword",
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
      "position": {
        "geo": {
          "location": "geo_point"
        },
        "accuracyMetres": 'integer',
      },
    }
  },
  {
    name: adapter.sensorName + '-parking',
    fields: {
      "@timestamp": "date",
      "ecs": {
        "version": 'keyword'
      },
      "event": {
        "created": "date",
        "duration": "long",
        "end": "date",
        "dataset": "keyword",
        "ingested": "date",
        "kind": "keyword",
        "module": "keyword",
        "original": "keyword",
        "start": "date",
        "timezone": "keyword",
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
      "position": {
        "geo": {
          "location": "geo_point"
        },
        "accuracyMetres": 'integer',
      },
    }
  },
];

adapter.promptProps = {
  properties: {
    authconfig: {
      description: 'OAUth credentials file for Google Drive (type "none" to use local file system)',
      default: 'client_secret.json'
    },
    inputDir: {
      description: 'Google Drive folder ID where Takeout files reside'
    },
    outputDir: {
      description: 'Google Drive folder ID where indexed files are moved to'
    }
  }
};

adapter.storeConfig = async function(c8, result) {
  let conf = result;
  await c8.config(conf);
  if (conf.authconfig && conf.authconfig != 'none') {
    fs.readFile(conf.authconfig, async (err, content) => {
      if (err) {
        console.log('Error loading client secret file: ' + err);
        return;
      }
      Object.assign(conf, JSON.parse(content));
      // console.log(conf);
      await c8.config(conf);
      var auth = google.auth;
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
            description: 'Enter the code shown on page'
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
            await c8.config(conf);
            console.log('Access credentials saved.');
            c8.release();
            process.exit;
          });
        }
      });
    });
  }
};

adapter.importData = async (c8, conf, opts) => {
  if (!conf.credentials) {
    throw new Error('Authentication credentials not found. Configure first!');
  }
  
  for (let i=0; i<adapter.types.length; i++) {
    await c8.type(adapter.types[i].name).clear();
    console.log(c8._index + ' cleared');
  }
  
  var drive = google.drive('v3');
  var auth = google.auth;
  var clientSecret = conf.installed.client_secret;
  var clientId = conf.installed.client_id;
  var redirectUrl = conf.installed.redirect_uris[0];
  var oauth2Client = new auth.OAuth2(clientId, clientSecret, redirectUrl);
  oauth2Client.credentials = conf.credentials;
  const response = await drive.files.list({
    auth: oauth2Client,
    spaces: "drive",
    q: "trashed != true and '" + conf.inputDir + "' in parents and mimeType='application/x-gtar'",
    pageSize: MAX_FILES,
    fields: "files(id, name)"
  });
  var files = response.data.files;
  if (files.length <= 0) {
    fulfill('No Takeout archives found in Drive folder ' + conf.inputDir);
  }
  else {
    for (let i = 0; i < files.length; i++) {
      let file = files[i];
      let fileName = file.name;
      let bulk = [];
      console.log('Processing file ' + (i+1) + ': ' + fileName);
      
      let oauth = {
        consumer_key: clientId,
        consumer_secret: clientSecret,
        token: conf.credentials.access_token
      }
      let url = 'https://www.googleapis.com/drive/v3/files/' + file.id + '?alt=media';
      let driveStream = request.get({url: url, headers: {'Authorization': 'Bearer ' + oauth2Client.credentials.access_token}});
      if (!driveStream) {
        console.error('stream failed for file ' + file.id + '!');
        continue;
      }
      driveStream
      .on('error', (error) => {
        console.error(new Error('driveStream error: ' + error));
        return;
      })
      .pipe(tar.x({filter: (path, entry) => {
        if (path.indexOf('.json') > 0) {
          return true;
        }
        console.log('Skipping ' + path);
        return false;
      }}))
      .on('error', (error) => {
        console.error(new Error('tar.x stream error: ' + error));
        return;
      })
      .on('entry', (substream) => {
        console.log(substream.mtime + ': ' + substream.path + ' (' + Math.round(substream.size/1024) + ' kB)');
        eos(substream, (err) => {
          if (err) {
            console.log('tar entry stream had an error or closed early');
            return;
          }
        });
        if (substream.path.indexOf('Location History.json') > 0) {
          let parse = JSONStream.parse('locations.*');
          let results = [];
          substream.pipe(parse)
          .on('data', async (data) => {
            let start = moment(Number(data.timestampMs));
            let position = data.location || {};
            position.geo = {
              "location": data.latitudeE7/1E7+','+data.longitudeE7/1E7
            };
            if (data.accuracy) {
              position.accuracy = data.accuracy;
            }
            let values = {
              "@timestamp": start.format(),
              "ecs": {
                "version": "1.6.0"
              },
              "event": {
                "created": substream.mtime,
                "dataset": "google.location",
                "ingested": new Date(),
                "kind": "event",
                "module": "Takeout",
                "original": JSON.stringify(data),
                "start":  start.format(),
              },
              "time_slice": time2slice(start),
              "date_details": time2details(start),
            };
            if (data.activity && data.activity.length) {
              for (let j=0; j<data.activity.length; j++) {
                let a = data.activity[j];
                let clone = Object.assign({}, values);
                clone["@timestamp"] = moment(Number(a.timestampMs));
                clone["time_slice"] = time2slice(clone["@timestamp"]),
                clone["date_details"] = time2details(clone["@timestamp"]),
                clone.event.original = JSON.stringify(a);
                for (let k=0; k<a.activity.length; k++) {
                  let subclone = Object.assign({}, clone);
                  subclone.activity = a.activity[k];
                  let meta = {
                    index: {
                      _index: c8.type(adapter.types[1].name)._index,
                      _id: subclone["@timestamp"] + '-' + k
                    }
                  };
                  bulk.push(meta);
                  bulk.push(subclone);
                  await checkBulk(bulk, substream.path, c8, parse);
                }
              }
            }
            values.position = position;
            let meta = {
              index: {
                _index: c8.type(adapter.types[0].name)._index,
                _id: data.timestampMs
              }
            };
            bulk.push(meta);
            bulk.push(values);
            await checkBulk(bulk, substream.path, c8, parse);
          })
          .on('end', async () => {
            // console.log('Last batch of ' + substream.path + '!');
            await checkBulk(bulk, substream.path, c8);
          })
          .on('error', (error) => {
            console.log('Error processing ' + substream.path);
            console.log(new Error(error));
          });
        }
        else {
          let parse = JSONStream.parse('timelineObjects.*');
          let results = [];
          substream.pipe(parse)
          .on('data', async (data) => {
            let template = {
              "ecs": {
                "version": "1.6.0"
              },
              "event": {
                "created": substream.mtime,
                "dataset": "google.location",
                "ingested": new Date(),
                "kind": "event",
                "module": "Takeout",
              }
            };
            
            let as = data.activitySegment;
            let pv = data.placeVisit;
            if (as) {
              let s = Number(as.duration.startTimestampMs);
              let e = Number(as.duration.endTimestampMs);
              let start = moment(s);
              let end = moment(e);
              let duration = (e - s) * 1E6;
              let sl = as.startLocation;
              let el = as.endLocation;
              
              let values = Object.assign(Object.assign({}, template), {
                "@timestamp": start.format(),
                "event": {
                  "end": end.format(),
                  "duration": duration,
                  "original": JSON.stringify(as),
                  "start": start.format(),
                },
                "time_slice": time2slice(start),
                "date_details": time2details(start),
                "activity_segment": {
                  "start": {
                    "sourceInfo": sl.sourceInfo
                  },
                  "end": {
                    "sourceInfo": el.sourceInfo
                  },
                  "distance": as.distance,
                  "confidence": as.confidence,
                  "activities": as.activities,
                  "waypoints": as.waypoints,
                  "editConfirmationStatus": as.editConfirmationStatus
                }
              });
              if (sl.latitudeE7) {
                values.activity_segment.start.geo = {
                  "location": sl.latitudeE7/1E7+','+sl.longitudeE7/1E7,
                };
                values.position = {
                  "geo": {
                    "location": sl.latitudeE7/1E7+','+sl.longitudeE7/1E7
                  }
                };
              }
              if (el.latitudeE7) {
                values.activity_segment.end.geo = {
                  "location": el.latitudeE7/1E7+','+el.longitudeE7/1E7,
                };
              }
              let meta = {
                index: {
                  _index: c8.type(adapter.types[2].name)._index,
                  _id: values["@timestamp"] + '-activitysegment'
                }
              };
              bulk.push(meta);
              bulk.push(values);
              await checkBulk(bulk, substream.path, c8, parse);
            }
            if (as && as.parkingEvent) {
              let e = as.parkingEvent;
              let el = e.location;
              let start = moment(Number(e.timestampMs));
              let values = Object.assign(Object.assign({}, template), {
                "@timestamp": start.format(),
                "event": {
                  "original": JSON.stringify(e),
                  "start": start.format(),
                },
                "time_slice": time2slice(start),
                "time_details": time2details(start),
                "position": {
                  "geo": {
                    "location": el.latitudeE7/1E7+','+el.longitudeE7/1E7,
                  },
                  "accuracy": el.accuracyMetres
                }
              });
              let meta = {
                index: {
                  _index: c8.type(adapter.types[4].name)._index,
                  _id: values["@timestamp"]
                }
              };
              bulk.push(meta);
              bulk.push(values);
              await checkBulk(bulk, substream.path, c8, parse);
            }
            if (pv) {
              let p = false;
              let s = Number(pv.duration.startTimestampMs);
              let e = Number(pv.duration.endTimestampMs);
              let start = moment(s);
              let end = moment(e);
              let duration = (e - s) * 1E6;
              let l = pv.location;
              if (l.latitudeE7) {
                p = {
                  "geo": {
                    "location": l.latitudeE7/1E7+','+l.longitudeE7/1E7
                  }
                };
                delete(l.latitudeE7);
                delete(l.longitudeE7);
              }
              l.placeConfidence = pv.placeConfidence;
              l.visitConfidence = pv.visitConfidence;
              if (pv.centerLatE7) {
                l.center = {
                  "geo": {
                    "location": pv.centerLatE7/1E7+','+pv.centerLngE7/1E7
                  },
                }
              }
              l.otherCandidateLocations = pv.otherCandidateLocations;
              l.editConfirmationStatus = pv.editConfirmationStatus;
              let values = Object.assign(template, {
                "@timestamp": start.format(),
                "event": {
                  "end": end.format(),
                  "duration": duration,
                  "original": JSON.stringify(as),
                  "start": start.format(),
                },
                "time_slice": time2slice(start),
                "date_details": time2details(start),
                "place_visit": l
              });
              if (p) {
                values.position = p;
              }
              let meta = {
                index: {
                  _index: c8.type(adapter.types[2].name)._index,
                  _id: values["@timestamp"] + '-placevisit'
                }
              };
              bulk.push(meta);
              bulk.push(values);
              await checkBulk(bulk, substream.path, c8, parse);
            }
          })
          .on('end', async () => {
            // console.log('Last batch of ' + substream.path + '!');
            await checkBulk(bulk, substream.path, c8);
          })
          .on('error', (error) => {
            console.log('Error processing ' + substream.path);
            throw new Error(error);
          });
        }
      })
      .on('end', () => {
        if (finishedBatches > 0) {
          var updateParams = {
            auth: oauth2Client,
            fileId: file.id,
            addParents: conf.outputDir,
            removeParents: conf.inputDir,
            fields: 'id, parents'
          };
          drive.files.update(updateParams, (err, updated) => {
            if(err) {
              throw new Error(err);
              return;
            }
            else {
              fulfill('Moved ' + file.name + ' from ' + conf.inputDir + ' to ' + conf.outputDir);
            }
          });
        }
        else {
          fulfill('No location history in ' + file.name);
        }
      })
      .on('error', err => {
        throw new Error(err);
      });
    }
    console.log('Found ' + files.length + ' file ' + (files.length == 1 ? '' : 's') + ' in ' + conf.inputDir);
  }
};

async function checkBulk(bulk, filename, c8, parse=null) {
  let results = [];
  if (bulk.length >= (MAX_BULK_BATCH * 2)) {
    if (parse) {
      parse.pause();
    }
    let clone = bulk.slice(0);
    bulk.length = 0;
    try {
      // console.log('Started ' + (++startedBatches) + ' bulk batches of ' + (clone.length/2) + ' locations (' + clone[1]['@timestamp'] + ')');
      process.stdout.write('<');
      results.push(await indexBulk(clone, filename, c8));
    }
    catch(e) {
      console.warn(e);
    }
    if (parse) {
      parse.resume();
    }
  }
  return results;
}

async function indexBulk(bulkData, filename, c8) {
  try {
    let response = await c8.bulk(bulkData);
    let result = c8.trimBulkResults(response);
    if (result.errors) {
      if (result.items) {
        let errors = [];
        for (let x=0; x<result.items.length; x++) {
          if (result.items[x].index.error) {
            errors.push(x + ': ' + result.items[x].index.error.reason);
          }
        }
        throw new Error(errors.length + ' errors in ' + filename);
        // throw new Error(errors.length + ' errors in bulk insert:\n ' + errors.join('\n '));
      }
      else {
        throw new Error(JSON.stringify(result.errors)); 
      }
    }
    process.stdout.write('>');
    finishedBatches += 1;
    // console.log('Finished ' + (finishedBatches) + ' bulk batches. (' + result.items.length + ' items)');
    return result;
  }
  catch (e) {
    throw new Error(e);
  }
}

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

function time2details(t) {
  return {
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
  };
}  

module.exports = adapter;
