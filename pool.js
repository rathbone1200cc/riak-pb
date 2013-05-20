var EventEmitter = require('events').EventEmitter;
var net = require('net');
var extend = require('util')._extend;

var defaultOptions = {
  innactivityTimeout: 3000
};

module.exports =
function Pool(options) {
  if (! options) options = {};

  options = extend({}, options);

  if (options.nodes && ! Array.isArray(options.nodes))
    options.nodes = [options.nodes];

  if (! options.nodes) throw new Error('No options.nodes defined');

  var nodes = options.nodes;
  var e = new EventEmitter();

  /// Connect

  e.connect =
  function connect() {
    var node = randomNode();
    var nodeStr = node.host + ':' + node.port;
    connection = net.connect(node);

    connection.___node__ = node;

    connection.setTimeout(options.innactivityTimeout);
    connection.once('timeout', onTimeout.bind(connection));
    connection.once('error', onError.bind(connection));
    connection.once('end', onEnd.bind(connection));

    return connection;
  };

  /// Events

  function onError(err) {
    e.emit('error', err);
    connection.end();
  }

  function onEnd() {
    e.emit('end', this.___node__);
  }

  function onTimeout() {
    connection.end();
  }

  /// Random node

  function randomNode() {
    var index = Math.floor(Math.random() * nodes.length);
    return nodes[index];
  }

  return e;
};