var fs = require('fs');
var google = require('googleapis');
var googleAuth = require('google-auth-library');
var prompt = require('prompt');
var activityTypes = require('google-fit-activity-types');

var adapter = {};

var MS_IN_DAY = 24 * 60 * 60 * 1000;
var MAX_DAYS = 1;
var MAX_EVENTS = 100;
var SCOPES = [
  'https://www.googleapis.com/auth/fitness.activity.read',
  'https://www.googleapis.com/auth/fitness.body.read',
  'https://www.googleapis.com/auth/fitness.location.read'
];

adapter.sensorName = 'googlefit';

adapter.types = [
  {
    name: 'googlefit-dataset',
    fields: {
      'accuracy': 'float',
      'activity': 'integer',
      'activityName': 'string',
      'altitude': 'float',
      'bpm': 'float',
      'calories': 'float',
      'confidence': 'float',
      'dataSourceId': 'string',
      'dataSourceName': 'string',
      'dataType': 'string',
       device: {
         uid: 'string',
         type: 'string',
         version: 'string',
         model: 'string',
         manufacturer: 'string',
      },
      'distance': 'float',
      'duration': 'float',
      'endTime': 'date',
      'endTimeNanos': 'long',
      'grams': 'float',
      'height': 'float',
      'IU': 'float',
      'latitude': 'float',
      'longitude': 'float',
      'originDataSourceId': 'string',
      'position': 'geo_point',
      'rpm': 'float',
      'resistance': 'float',
      'speed': 'float',
      'startTime': 'date',
      'startTimeNanos': 'long',
      'steps': 'float',
      'timestamp': 'date',
      'timestamp': 'date',
      'watts': 'float',
      'weight': 'float'
    }
  },
  {
    name: 'googlefit-session',
    fields: {
      duration: 'integer',
      timestamp: 'date',
      position: 'geo_point'
    }
  }
];

adapter.promptProps = {
  properties: {
    authconfig: {
      description: 'Configuration file'.magenta,
      default: 'client_secret.json'
    }
  }
};

adapter.storeConfig = function(c8, result) {
  var conf = result;
  fs.readFile(result.authconfig, function (err, content) {
    if (err) {
      console.log('Error loading client secret file: ' + err);
      return;
    }
    Object.assign(conf, JSON.parse(content));
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
  var clientSecret = conf.installed.client_secret;
  var clientId = conf.installed.client_id;
  var redirectUrl = conf.installed.redirect_uris[0];
  var auth = new googleAuth();
  var oauth2Client = new auth.OAuth2(clientId, clientSecret, redirectUrl);
  var firstDate = new Date();
  firstDate.setTime(firstDate.getTime() - MS_IN_DAY);
  var lastDate = opts.lastDate || new Date();
  oauth2Client.credentials = conf.credentials;
  c8.type(adapter.types[0].name).search({
    _source: ['timestamp'],
    size: 1,
    sort: [{'timestamp': 'desc'}],
  }).then(function(response) {
    if (opts.firstDate) {
      firstDate = opts.firstDate;
      console.log('Setting first time to ' + firstDate);
    }
    else if (response && response.hits && response.hits.hits && response.hits.hits[0] && response.hits.hits[0]._source && response.hits.hits[0]._source.timestamp) {
      var d = new Date(response.hits.hits[0]._source.timestamp);
      // firstDate = new Date(d.getTime() + 1);
      firstDate = d;
      console.log('Setting first time to ' + firstDate);
    }
    else {
      firstDate = new Date();
      firstDate.setTime(firstDate.getTime() - MS_IN_DAY);
      console.warn('No previously indexed data, setting first time to ' + firstDate);
    }
    if (lastDate.getTime() >= (firstDate.getTime() + (MS_IN_DAY * MAX_DAYS))) {
      lastDate.setTime(firstDate.getTime() + (MS_IN_DAY * MAX_DAYS) - 1);
      console.warn('Max time range ' + MAX_DAYS + ' days, setting end time to ' + lastDate);
    }
    var dsNames = {};
    var dTypes = {};
    var devices = {};
    var fit = google.fitness('v1');
    fit.users.dataSources.list({auth: oauth2Client, userId: 'me'}, function(err, resp) {
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
          auth: oauth2Client,
          userId: 'me',
          dataSourceId: dsId,
          datasetId: datasetId
        }
        // console.log(params);
        fit.users.dataSources.datasets.get(params, function(err, resp) {
          if (err) {
            console.error('The fitness API returned an error when reading data set: ' + err);
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
              values.timestamp = new Date(item.startTimeNanos/1000000); // ms
              var st = parseInt(item.startTimeNanos);
              var et = parseInt(item.endTimeNanos);
              values.startTimeNanos = st;
              values.endTimeNanos = et;
              values.startTime = new Date(parseInt(st/1000000)); // ms
              values.endTime = new Date(parseInt(et/1000000)); // ms
              values.duration = parseFloat((et - st)/1000000000); // fractions of seconds!
              values.dataSourceName = dsName;
              values.dataType = dType.name;
              // item.dataType = dType;
              var ll = [];
              for (var k=0; k<dType.field.length; k++) {
                if (!points[j].value[k]) {
                   //console.warn('Undefined ' + dType.field[k].name);
                   console.log(points[j]);
                   continue;
                }
                values[dType.field[k].name] = getValue(points[j].value[k]);
                if (dType.field[k].name == 'activity') {
                  var activity = activityTypes[points[j].value[k].intVal];
                  if (activity) {
                    values['activityName'] = activity;
                  }
                }
                else if (dType.field[k].name == 'latitude') {
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
            // console.log(JSON.stringify(bulk, null, 2));
              c8.bulk(bulk).then(function(result) {
		  if (result.errors) {
		      console.trace(result.items);
		  }
		// console.log(result);
		console.log('Indexed ' + result.items.length + ' items in ' + result.took + ' ms.');
              bulk = null;
            }).catch(function(error) {
              console.trace(error);
              bulk = null;
            });
          }
          else {
            var sd = new Date(resp.minStartTimeNs/1000000);
            var ed = new Date(resp.maxEndTimeNs/1000000);
            // console.log('No data between ' + sd.toISOString() + ' and ' + ed.toISOString());
          }
        });
      };
    });
  }).catch(function(error) {
    console.trace(error);
  });
};

function getValue(obj) {
    // what about string types?
    if (obj.stringVal || obj.stringVal === 0) {
	return obj.stringVal;
    }
    if (obj.fpVal || obj.fpVal === 0) {
	return obj.fpVal;
    }
    if (obj.intVal || obj.intVal === 0) {
	return obj.intVal;
    }
    if (obj.mapVal) {
	var values = [];
	for (var i=0; i<obj.mapVal.length; i++) {
	    var tmp = {};
	    tmp[obj.mapVal[i].key] = obj.mapVal[i].value.fpVal;
	    values.push(tmp);
	}
	if (values.length) {
	    return values;
	}
    }
    if (obj.value || obj.value === 0) {
	return obj.value;
    }
	// console.trace(obj);
    return null;
}

module.exports = adapter;
