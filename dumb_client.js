var assert = require('assert');
var Domain = require('domain');
var Duplex = require('stream').Duplex;
var Pool = require('./pool');
var _Protocol = require('./protocol');

var reconnectErrorCodes = ['ECONNREFUSED', 'ECONNRESET'];
var riakReconnectErrorCodes = ['all_nodes_down', 'Unknown message code.'];

module.exports =
function Client(options) {
  var pool = options.pool || Pool(options);
  var Protocol = options.protocol || _Protocol;

  var s = new Duplex({objectMode: true, highWaterMark: 1});

  var destroyed = false;
  var connection;
  var parser;
  var lastCommand;
  var lastSerializedPayload;
  var callback;
  var response = {};
  var expectMultiple;

  var retries = 0;
  var maxRetries = options.maxRetries || 10;
  var maxDelay = options.maxDelay || 2000;

  /// Command

  var oldWrite = s.write;
  s.write = function(command) {
    assert(typeof command == 'object', 'command must be an object');
    return oldWrite.apply(s, arguments);
  }

  s._write =
  function (command, encoding, _callback) {
    assert(typeof command == 'object', 'command must be an object');
    if (callback) throw new Error('I\'m in the middle of a request');
    lastCommand = command;
    callback = _callback;
    lastSerializedPayload = Protocol.serialize(command.payload);
    expectMultiple = command.expectMultiple;
    sendCommand();
    return false;
  };

  function sendCommand() {
    assert(lastCommand, 'no overlapping commands allowed at this level');
    if (! connection) {
      connection = connect();

      /// All this following code just to
      /// detect unsuccessful connection
      var domain = Domain.create();
      domain.on('error', onDomainError);
      domain.add(connection);
      connection.once('connect', function() {
        domain.remove(connection);
        domain.dispose();
      });
    }
    parser.expectMultiple(expectMultiple);
    connection.write(lastSerializedPayload);
  }

  s._read = function() {};


  /// Connect

  function connect() {
    if (destroyed) throw new Error('Destroyed');
    connection = pool.connect();
    connection.on('error', onConnectionError);
    parser = Protocol.parse();
    connection.pipe(parser);
    parser.on('readable', onParserReadable);
    parser.on('done', onParserDone);
    return connection;
  }


  /// Error handling

  var lastError;
  function onDomainError(err) {
    if (err == lastError) return;

    lastError = err;
    process.nextTick(function() {
      lastError = undefined;
    });
    if (err.code && ~reconnectErrorCodes.indexOf(err.code)) onConnectionError(err);
    else s.emit('error', err);
  }

  function onConnectionError(err) {
    // throw away this connection so that
    // we get a new one when retrying

    s.emit('warning', err);
    s.emit('interrupted', err);

    if (connection) {
      connection.removeListener('error', onConnectionError);
      connection.unpipe(parser);
      connection.destroy();
      connection = undefined;
      parser.destroy();
      parser.removeListener('readable', onParserReadable);
      parser.removeListener('done', onParserDone);
      parser = undefined;
    }

    retry();
  }


  /// On Parser Done

  function onParserDone() {
    s.emit('done');
    if (expectMultiple) finishResponse();
  }


  /// Read from parser

  function onParserReadable() {
    var reply;
    while (parser && (reply = parser.read())) {
      handleReply(reply);
    }
  }


  /// Handle response buffer

  function handleReply(reply) {
    if (reply.errmsg) {
      if (~riakReconnectErrorCodes.indexOf(reply.errmsg)) {
        var error = new Error(reply.errmsg);
        /// HACK
        /// When shutting down a riak node
        // I've observed that pending connections get this error
        // Since I've never seen this error message
        // in any other situation, I'm going to assume that
        // this is the case where the node is shutting down.
        // *sigh*
        error.code = 'ECONNREFUSED'; // more HACK
        onConnectionError(new Error(reply.errmsg));

      }
      else respondError(new Error(reply.errmsg));
    }
    else {
      if (! expectMultiple) {
        s.push(reply);
        response = reply;
        finishResponse();
      } else {
        if (reply.done) {
          finishResponse();
          s.emit('done');
        } else {
          s.push(reply);
        }
      }
    }
  }


  /// Finish Response

  function finishResponse(err) {
    var _response = response;
    var _callback = callback;
    cleanup();
    if (_callback) {
      if (err) _callback(err);
      else _callback(null, _response);
    }
    s.emit('drain');
  }


  /// Retry

  function retry() {
    retries ++;
    if (retries > maxRetries)
      respondError(new Error('max retries reached'));
    else {
      setTimeout(function() {
        if (lastCommand) sendCommand();
      }, Math.min(Math.exp(10, retries), maxDelay));
    }
  }


  /// Respond Error

  function respondError(err) {
    var _callback = callback;
    cleanup();
    if (_callback) _callback(err);
    else s.emit('error', err);
    s.emit('drain');
  }

  /// Cleanup

  function cleanup() {
    response = {};
    callback = undefined;
    retries = 0;
    lastCommand = undefined;
    lastSerializedPayload = undefined;
  }


  /// Destroy

  s.destroy =
  function destroy() {
    destroyed = true;
    if (connection) connection.destroy();
  }

  return s;
};


//// Utils
