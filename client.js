var EventEmitter = require('events').EventEmitter;
var assert = require('assert');
var DumbClient = require('./dumb_client');
var PassThrough = require('stream').PassThrough;

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
    assert(busy, 'shouldnt get a readable when not waiting for response');
    var response;
    while (response = client.read()) {
      var callback = queue.shift();
      if (! expectMultiple) {
        busy = false;
        if (callback) callback(null, response);
        flush();
      }
    }
  }

  function clientOnDone() {
    if (busy && expectMultiple) {
      busy = false;
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
    var calledback = false;
    request('RpbListKeysReq', params, true, function(err) {
      if (err) {
        if (! callback) s.emit('error', err);
        else {
          calledback = true;
          callback(err);
        }
      }
    });
    client.pipe(s);
    client.once('done', function() {
      if (callback) client.removeListener('readable', clientOnReadable);
      client.unpipe(s);
      if (callback && ! calledback) {
        calledback = true;
        callback(null, keys);
      }
    });

    var keys;
    if (callback) {
      keys = [];
      client.on('readable', clientOnReadable);
    }
    return s;

    function clientOnReadable() {
      var key;
      while (key = client.read()) {
        console.log('client got', key);
        keys.push(key);
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
    this.makeRequest('RpbDelReq', params, false, callback);
  };

  c.getIndex = function getIndex(params, callback) {
    request('RpbIndexReq', params, false, callback);
  };

  return c;
};

return;

RiakPBC.prototype.getKeys = function (params, streaming, callback) {
  if (typeof streaming === 'function') {
    callback = streaming;
    streaming = false;
  }

  if (streaming) {
    var emitter = new EventEmitter();
    this.makeRequest('RpbListKeysReq', params, callback, true, emitter);
    return emitter;
  } else {
    this.makeRequest('RpbListKeysReq', params, callback, true);
  }
};





RiakPBC.prototype.mapred = function (params, streaming, callback) {
  if (typeof streaming === 'function') {
    callback = streaming;
    streaming = false;
  }

  if (streaming) {
    var emitter = new EventEmitter();
    this.makeRequest('RpbMapRedReq', params, callback, true, emitter);
    return emitter;
  } else {
    this.makeRequest('RpbMapRedReq', params, callback, true);
  }
};



RiakPBC.prototype.search = function (params, callback) {
  this.makeRequest('RpbSearchQueryReq', params, callback);
};

RiakPBC.prototype.getClientId = function (callback) {
  this.makeRequest('RpbGetClientIdReq', null, callback);
};

RiakPBC.prototype.setClientId = function (params, callback) {
  this.makeRequest('RpbSetClientIdReq', params, callback);
};



RiakPBC.prototype.ping = function (callback) {
  this.makeRequest('RpbPingReq', null, callback);
};

RiakPBC.prototype.connect = function (callback) {
  if (this.connected) return callback();
  var self = this;
  self.client.connect(self.port, self.host, function () {
    self.connected = true;
    callback();
  });
};

RiakPBC.prototype.disconnect = function () {
  if (!this.connected) return;
  this.client.end();
  this.connected = false;
  if (this.task) {
    this.queue.unshift(this.task);
    this.task = undefined;
  }
};

exports.createClient = function (options) {
  return new RiakPBC(options);
};