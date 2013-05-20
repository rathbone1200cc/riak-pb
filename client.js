var EventEmitter = require('events').EventEmitter;
var assert = require('assert');
var Options = require('./options');
var DumbClient = require('./dumb_client');
var ClientStream = require('./client_stream');

exports =
module.exports =
function RiakClient(options) {

  options = Options(options);

  var c = new EventEmitter;
  var queue = [];
  var busy = false;
  var ending = false;
  var expectMultiple;
  var stream;
  var isDone = false;

  var client = DumbClient(options);
  client.on('readable', clientOnReadable);
  client.on('error', clientOnError);
  client.on('warning', clientOnWarning);

  function request(type, data, expectMultiple, callback, stream) {
    var req = {payload: {type: type, data: data}, expectMultiple: expectMultiple, stream: stream}
    queue.push(req);
    queue.push(callback);

    flush();
  }

  function flush() {
    if (!busy) {
      isDone = false;
      if (queue.length) {
        actuallyDoRequest();
      } else if (ending) {
        // no more jobs in the queue
        // and we're ending
        // this is the time to say goodbye...
        client.destroy();
      }
    }
  }

  function actuallyDoRequest() {
    busy = true;
    var args = queue.shift();
    expectMultiple = args.expectMultiple;
    var s = stream = args.stream;
    if (s) {
      // streaming
      client.pipe(stream);
      client.once('done', function() {
        s.emit('end');
      });
      client.once('interrupted', clientInterrupted);
      stream.once('end', function() {
        cleanup();
        done(null, s.results);
      });
      stream.once('error', function(err) {
        cleanup();
        done(err);
      });
      stream.once('results', function(results) {
        cleanup();
        done(null, results);
      });

      function cleanup() {
        client.unpipe(stream);
        client.removeListener('interrupted', clientInterrupted);
      }

      function clientInterrupted() {
        s.emit('error', err);
        s.emit('end');
      }
    }
    client.write(args);
  }

  function clientOnReadable() {
    if (! expectMultiple && busy) {
      var response;
      while (response = client.read()) {
        if (! expectMultiple) done(null, response);
      }
    }
  }

  function clientOnError(err) {
    if (busy) done(err);
    else c.emit('error', err);
  }

  function clientOnWarning(warn) {
    c.emit('warning', warn);
  }

  /// Done
  function done(err, result) {
    if (! isDone) {
      isDone = true;
      busy = false;
      var callback = queue.shift();
      if (err) {
        if (callback) callback(err);
        else client.emit('error', err);
      } else {
        if (callback) callback(null, result);
      }
      flush();
    }
  }

  /// Disconnect

  c.disconnect =
  function disconnect() {
    ending = true;
    flush();
  };

  /// Client utility methods
  c.getBuckets = function getBuckets(callback) {
    request('RpbListBucketsReq', null, false, function(err, reply) {
      if (err) return callback(err);
      callback(null, reply.buckets);
    });
  };

  c.getBucket = function getBucket(bucket, callback) {
    request('RpbGetBucketReq', {bucket: bucket}, false, function(err, reply) {
      if (err) return callback(err);
      callback(null, reply.props);
    });
  }

  c.setBucket = function setBucket(bucket, props, callback) {
    request('RpbSetBucketReq', {bucket: bucket, props: props}, false, callback);
  }

  c.getKeys = function getKeys(bucket, callback) {
    var s = ClientStream(!!callback, getKeysMap, getKeysReduce);
    request('RpbListKeysReq', {bucket: bucket}, true, callback, s);
    return s;
  };

  c.setClientId = function setClientId(clientId, callback) {
    request('RpbSetClientIdReq', { client_id: clientId }, false, callback);
  };


  c.getClientId = function getClientId(callback) {
    request('RpbGetClientIdReq', null, false, function(err, reply) {
      if (err) return callback(err);
      callback(null, reply.client_id);
    });
  };

  c.ping = function ping(callback) {
    request('RpbPingReq', null, false, callback);
  };

  c.getServerInfo = function getServerInfo(callback) {
    request('RpbGetServerInfoReq', null, false, callback);
  };

  c.put = function put(params, callback) {
    request('RpbPutReq', params, false, callback);
  };

  c.get = function get(params, callback) {
    request('RpbGetReq', params, false, callback);
  };

  c.del = function del(bucket, id, callback) {
    var options = {bucket: bucket};
    if (typeof id == 'object') {
      extend(options, id);
    } else options.key = id;
    request('RpbDelReq', options, false, callback);
  };

  c.getIndex = function getIndex(params, callback) {
    request('RpbIndexReq', params, false, callback);
  };

  c.search = function search(params, callback) {
    var s = ClientStream(!!callback);
    request('RpbSearchQueryReq', params, false, callback, s);
    return s;
  };

  c.mapred = function mapred(params, callback) {
    var s = ClientStream(!!callback, undefined, mapRedReduce);
    request('RpbMapRedReq', params, true, callback, s);
    return s;
  };


  return c;
};

exports.createClient =
function createClient(options) {
  return exports(options);
};

return;

//// Utils

function getKeysMap(result) {
  return result.keys;
}

function getKeysReduce(o, n) {
  return o.concat(n);
}

function mapRedReduce(o, n) {
  return o.concat(n);
}
