var assert = require('assert');
var Duplex = require('stream').Duplex;
var Pool = require('./pool');
var _Protocol = require('./protocol');

module.exports =
function Client(options) {
  var pool = options.pool || Pool(options);
  var Protocol = options.protocol || _Protocol;

  var s = new Duplex({objectMode: true, highWaterMark: 1});

  var destroyed = false;
  var connection;
  var parser;
  var lastCommand;
  var callback;
  var response = {};
  var expectMultiple;

  var retries = 0;
  var maxRetries = options.maxRetries || 100;

  /// Command

  s._write =
  function (command, encoding, _callback) {
    if (callback) throw new Error('I\'m in the middle of a request');
    lastCommand = command;
    callback = _callback;
    sendCommand(command);
    return false;
  };

  function sendCommand(command) {
    assert(command.payload, 'need command.payload');
    if (! connection) connection = connect();
    var serialized = Protocol.serialize(command.payload);
    expectMultiple = command.expectMultiple;
    parser.expectMultiple(command.expectMultiple);
    connection.write(serialized);
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

  function onConnectionError(err) {
    // throw away this connection so that
    // we get a new one when retrying
    connection = undefined;
    // retry
    retry();
  }


  /// On Parser Readable

  function onParserDone() {
    console.log('parser done');
    s.emit('done');
    finishResponse();
  }


  /// Read from parser

  function onParserReadable() {
    var reply;
    while (reply = parser.read()) {
      console.log('from parser: %j', reply);
      s.push(reply);
      handleReply(reply);
    }
  }


  /// Handle response buffer

  function handleReply(reply) {
    if (reply.errmsg) {
      respondError(new Error(reply.errmsg));
    } else {
      if (! expectMultiple) {
        response = reply;
        finishResponse();
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
    if (lastCommand) {
      retries ++;
      if (retries > maxRetries)
        respondError(new Error('max retries reached'));
      else sendCommand(lastCommand);
    }
  }


  /// Respond Error

  function respondError(err) {
    var _callback = callback;
    cleanup();
    if (_callback) _callback(err);
    else s.emit('error');
    s.emit('drain');
  }

  /// Cleanup

  function cleanup() {
    response = {};
    callback = undefined;
    retries = 0;
    lastCommand = undefined;
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
