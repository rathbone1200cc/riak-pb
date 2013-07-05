var async = require('async');
var assert = require('assert');
var client = require('../')({nodes: [{host: '127.0.0.1', port: 8087}]});

client.on('warning', function(warn) {
  console.log('client warning:', warn);
});

max = 100000;

var args = [];
for(var i = 0 ; i < max; i ++) {
  args.push({bucket: 'test-massive', key: i.toString(), content: {
    value: '{"test":"' + i + '"}',
    content_type: 'application/json'},
    return_body: true });
}

async.mapLimit(args, 20, put, done);

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
  console.log('inserts OK.');
  setTimeout(massiveGet, 1000);
}

function massiveGet() {
  console.log('starting gets...');
  var missing = max;
  console.log('getting all keys...');
  client.getKeys('test-massive', function(err, keys) {
    if (err) throw err;
    assert(keys.length >= max);

    console.log('got all the keys: %j', keys.length);

    keys.forEach(function(i) {
      client.get({bucket: 'test-massive', key: i}, function(err, doc) {
        if (err) throw err;
        missing --;
        assert(!!doc, 'no doc');
        assert.equal(JSON.parse(doc.content[0].value).test, i);
        if (missing == 0) {
          console.log('ALL DONE!');
          client.disconnect();
        }
      });
    });
  });
}