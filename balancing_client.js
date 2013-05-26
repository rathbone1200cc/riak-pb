var EventEmitter = require('events').EventEmitter;
var Options = require('./options');
var Client = require('./client');
var ClientStream = require('./client_stream');
var assert = require('assert');

module.exports =
function BalancingClient(options) {
  options = Options(options);

  var maxPool = options.maxPool || 5;

  var c = new EventEmitter();

  var pool = [];


  function getClient() {
    var client = getClientFromPool();
    if (! client) {
      if (pool.length < maxPool)
        client = createClient();
      else
        client = pool[Math.floor(Math.random() * pool.length)];
    }
    return client;
  }

  function getClientFromPool() {
    var client;
    for (var i = 0; i < pool.length; i ++) {
      client = pool[i];
      if (! client.busy && client.queue.length == 0) return client;
    }
  }

  function createClient() {
    var client = Client(options);
    pool.push(client);
    return client;
  }

  /// Request

  function request() {
    var client = getClient();
    assert(client, 'getClient must return a client');
    return client.request.apply(client, arguments);
  }

  /// Disconnect

  c.end =
  c.destroy =
  c.disconnect =
  function disconnect() {
    pool.forEach(function(client) {
      client.disconnect();
    });
  };


  /// Utility methods

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