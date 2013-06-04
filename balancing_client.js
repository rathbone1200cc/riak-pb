var assert = require('assert');
var EventEmitter = require('events').EventEmitter;
var Options = require('./options');
var ClientStream = require('./client_stream');
var Pool = require('./pool');

exports =
module.exports =
function BalancingClient(options) {
  options = Options(options);

  var c = new EventEmitter();


  /// Pool

  var pool = Pool(options);

  pool.on('warning', function(warn) {
    c.emit('warning', warn);
  });

  pool.on('error', function(err) {
    c.emit('error', err);
  });

  pool.once('end', function() {
    c.emit('end');
  });


  /// Request

  function request() {
    var args = arguments;
    pool.get(function(client) {
      client.request.apply(client, args);
    });
  }


  /// Disconnect

  c.end =
  c.destroy =
  c.disconnect = pool.disconnect;

  /// Utility methods

  c.getBuckets = function getBuckets(callback) {
    request('RpbListBucketsReq', null, false, function(err, reply) {
      if (err) return callback(err);
      callback(null, reply.buckets || []);
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
    request('RpbSearchQueryReq', params, false, callback);
  };

  c.mapred = function mapred(params, callback) {
    var s = ClientStream(!!callback, undefined, mapRedReduce);
    request('RpbMapRedReq', params, true, callback, s);
    return s;
  };

  return c;

}

//// Utils

function getKeysMap(result) {
  return result.keys;
}

function getKeysReduce(o, n) {
  var v;
  for (var i = 0; i < n.length; i++) {
    v = n[i];
    if (v && !~o.indexOf(v)) o.push(v);
  }
  return o;
}

function mapRedReduce(o, n) {
  return o.concat(n);
}

/// Create Client

exports.createClient =
function createClient(options) {
  return exports(options);
};