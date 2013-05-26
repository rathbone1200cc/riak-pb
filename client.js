var EventEmitter = require('events').EventEmitter;
var assert = require('assert');
var extend = require('util')._extend;
var Options = require('./options');
var DumbClient = require('./dumb_client');
var ClientStream = require('./client_stream');
var log = require('./log')('client');

exports =
module.exports =
function RiakClient(options) {

  options = Options(options);

  var c = new EventEmitter();
  var ending = false;
  var expectMultiple;
  var callback;
  var stream;
  var isDone = false;

  c.busy = false;
  c.queue = [];

  var client = DumbClient(options);
  client.on('readable', clientOnReadable);
  client.on('error', clientOnError);
  client.on('warning', clientOnWarning);

  c.request =
  function request(type, data, expectMultiple, callback, stream) {
    var payload = {type: type};
    if (data) payload.data = data;
    var req = {
      payload: payload,
      expectMultiple: expectMultiple,
      stream: stream,
      callback: callback};

    c.queue.push(req);

    flush();
  }

  function flush() {
    if (!c.busy) {
      if (c.queue.length) {
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
    isDone = false;
    c.busy = true;
    var args = c.queue.shift();
    expectMultiple = args.expectMultiple;
    callback = args.callback;
    var s = stream = args.stream;

    if (s) {
      var ended = false;
      var resulted = false;
      client.removeListener('readable', clientOnReadable);

      // streaming

      client.pipe(stream);

      client.once('done', function() {
        if (! ended) s.emit('end');
      });

      client.once('interrupted', clientInterrupted);

      stream.once('end', function() {
        if (! ended) {
          ended = true;
          finish(null, s.results);
        }
      });

      stream.once('error', function(err) {
        cleanup();
        done(err);
      });

      stream.once('results', function(results) {
        finish(null, results);
      });

      var cleanedup = false;

      function cleanup() {
        if (! cleanedup) {
          cleanedup = true;
          client.unpipe(stream);
          client.removeListener('interrupted', clientInterrupted);
          client.addListener('readable', clientOnReadable);
        }
      }

      function finish(err, result) {
        if (! resulted) {
          cleanup();
          resulted = true;
          done(err, result);
        }
      }

      function clientInterrupted(err) {
        s.emit('error', err);
        s.emit('end');
      }
    }
    client.write(args);
  }

  function clientOnReadable() {
    log('dumb client is readable');
    if (! expectMultiple && c.busy) {
      log('not expecting multiple and busy');
      var response;
      while (response = client.read()) {
        log('read from the dumb client:', response);
        if (! expectMultiple) done(null, response);
      }
    }
  }

  function clientOnError(err) {
    if (c.busy) done(err);
    else c.emit('error', err);
  }

  function clientOnWarning(warn) {
    c.emit('warning', warn);
  }

  /// Done
  function done(err, result) {
    if (! isDone) {
      log('done. setting busy to false');
      isDone = true;
      c.busy = false;
      if (err) {
        if (callback) callback(err);
        else client.emit('error', err);
      } else {
        if (callback) {
          log('calling back with result', result);
          callback(null, result);
        }
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

  return c;
};

exports.createClient =
function createClient(options) {
  return exports(options);
};