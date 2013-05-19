var extend = require('util')._extend;

module.exports =
function Options(options) {
  if (! options) options = {};
  options = extend({}, options);
  if (! options.nodes) options.nodes = {host: '127.0.0.1', port: 8087};
  if (! Array.isArray(options.nodes)) options.nodes = [options.nodes];
  options.nodes = options.nodes.map(function(node) {
    if (typeof node == 'string') node = { host: node };
    if (typeof node == 'number') node = { port: node };
    if (! node.host) node.host = '127.0.0.1';
    if (! node.port) node.port = 8087;
    return node;
  });

  return options;
}