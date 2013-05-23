var noop = function() {};

module.exports =
function Log(prefix) {
  var log;

  if (process.env.RIAK_PB_DEBUG) {
    log = function() {
      arguments[0] = '[riak-pb][' + prefix + '] ' + arguments[0];
      console.log.apply(console, arguments);
    };
  } else {
    log = noop;
  }

  return log;
}

