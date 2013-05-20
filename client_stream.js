var Transform = require('stream').Transform;

module.exports =
function ClientStream(hasCallback, map, reduce) {

  if (! map) map = identity;
  if (! reduce) reduce = pushReduce;

  var s = new Transform({objectMode: true});

  var results;
  if (hasCallback) this.results = results = [];

  s._write =
  function _write(result, encoding, callback) {
    result = map(result);
    if (!Array.isArray(result)) result = [result];
    result.forEach(function(oneResult) {
      if (results) results = reduce(results, result);
      s.push(oneResult);
    });
    callback();
  }

  s.once('end', function() {
    s.emit('results', results);
  });

  return s;
};


function identity(o) {
  return o;
}

function pushReduce(o, n) {
  o.push(n);
  return o;
}