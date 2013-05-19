var async = require('async');
var assert = require('assert');
var client = require('../')({nodes: [{host: '127.0.0.1', port: 8087}]});

max = 10000;
var args = [];
for(var i = 0 ; i < max; i ++) {
  args.push({bucket: 'test-massive', key: i.toString(), content: {
    value: '{"test":"' + i + '"}',
    content_type: 'application/json'},
    return_body: true });
}

async.mapSeries(args, put, done);

var count = 0;
function put(arg, cb) {
  client.put(arg, function(err, res) {
    if (err) throw err;
    console.log('done', count);
    if (! res) throw new Error('Expected result here');
    cb(null, JSON.parse(res.content[0].value).test);
    count ++;
  });
}

function done(err, rets) {
  if (err) throw err;
  var next = 0;
  rets.forEach(function(ret) {
    assert(ret == next);
    next ++;
  });
  client.disconnect();
}