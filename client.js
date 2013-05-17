var EventEmitter = require('events').EventEmitter;
var assert = require('assert');
var DumbClient = require('./dumb_client');
var PassThrough = require('stream').PassThrough;

exports =
module.exports =
function RiakClient(options) {
  var c = {};
  var queue = [];
  var busy = false;
  var ending = false;
  var expectMultiple;

  if (! options) options = {};

  var client = DumbClient(options);
  client.on('readable', clientOnReadable);
  client.on('error', clientOnError);
  client.on('done', clientOnDone);


  function request(type, data, expectMultiple, callback) {
    queue.push({payload: {type: type, data: data}, expectMultiple: expectMultiple});
    queue.push(callback);
    flush();
  }

  function flush() {
    if (!busy) {
      if (queue.length) {
        busy = true;
        var args = queue.shift();
        expectMultiple = args.expectMultiple;
        client.write(args);
      } else if (ending) {
        // no more jobs in the queue
        // and we're ending
        // this is the time to say goodbye...
        client.destroy();
      }
    }
  }

  function clientOnReadable() {
    if (! expectMultiple) {
      assert(busy, 'shouldnt get a readable when not waiting for response');
      var response;
      while (response = client.read()) {
        if (! expectMultiple) {
          busy = false;
          var callback = queue.shift();
          if (callback) callback(null, response);
          flush();
        }
      }
    }
  }

  function clientOnDone() {
    if (busy && expectMultiple) {
      busy = false;
      var callback = queue.shift();
      if (callback) callback(null);
      flush();
    }
  }

  function clientOnError(err) {
    if (busy) {
      busy = false;
      var callback = queue.shift();
      if (callback) callback(err);
      else client.emit('error', err);
      flush();
    } else {
      client.emit('error', err);
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

  c.getBucket = function getBucket(params, callback) {
    request('RpbGetBucketReq', params, false, function(err, reply) {
      if (err) return callback(err);
      callback(null, reply.props);
    });
  }

  c.setBucket = function setBucket(params, callback) {
    request('RpbSetBucketReq', params, false, callback);
  }

  c.getKeys = function getKeys(params, callback) {
    var s = new PassThrough({objectMode: true});

    client.once('done', function() {
      client.removeListener('readable', clientOnReadable);
      s.emit('end');
    });

    var keys;
    if (callback) {
      keys = [];
    }
    client.on('readable', clientOnReadable);

    request('RpbListKeysReq', params, true, function(err) {
      if (err) {
        if (! callback) s.emit('error', err);
        else callback(err);
      }
      else if (callback) {
        callback(null, keys);
      }

    });

    return s;

    function clientOnReadable() {
      var reply;
      while (reply = client.read()) {
        if (keys) keys = keys.concat(reply.keys);
        reply.keys.forEach(function(key) {
          s.push(key);
        });
      }
    }
  };

  c.setClientId = function setClientId(params, callback) {
    request('RpbSetClientIdReq', params, false, callback);
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

  c.del = function del(params, callback) {
    request('RpbDelReq', params, false, callback);
  };

  c.getIndex = function getIndex(params, callback) {
    request('RpbIndexReq', params, false, callback);
  };

  c.search = function search(params, callback) {
    request('RpbSearchQueryReq', params, false, callback);
  };

  c.mapred = function mapred(params, callback) {
    var s = new PassThrough({objectMode: true});

    client.once('done', function() {
      client.removeListener('readable', clientOnReadable);
      s.emit('end');
    });

    var results;
    if (callback) {
      results = [];
    }

    client.on('readable', clientOnReadable);

    request('RpbMapRedReq', params, true, function(err) {
      if (err) {
        if (! callback) s.emit('error', err);
        else callback(err);
      }
      else if (callback) {
        callback(null, results);
      }
    });

    return s;

    function clientOnReadable() {
      var result;
      while(result = client.read()) {
        if (results) results.push(result);
      }
    }

  };


  return c;
};

exports.createClient =
function createClient(options) {
  return exports(options);
};