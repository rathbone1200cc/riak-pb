var Duplex = require('stream').Duplex;
var extend = require('util')._extend;
var test = require('tap').test;
var Client = require('../');

test('it connects from pool', function(t) {

  var mockConnection = new Duplex({objectMode: true});
  mockConnection._write = function(o, encoding, cb) {
    o = JSON.parse(o);
    o.done = true;
    this.push(JSON.stringify(o));
    cb();
  };

  mockConnection._read = function() {}

  var mockProtocol = {
    parse: function() {
      var parser = new Duplex({objectMode: true});

      parser._write = function(b, encoding, cb) {
        this.push(JSON.parse(b));
        cb(null);
      };

      parser._read = function() {};

      return parser;

    },
    serialize: function(o) {
      return JSON.stringify(o);
    },
    merge: extend
  };

  var mockPool = {
    connect: function() {
      return mockConnection;
    }
  };

  var client = Client({
    pool: mockPool,
    protocol: mockProtocol
  });

  var writeCalledBack = false;
  var ret = client.write({a: 'WAT'}, function(err) {
    writeCalledBack = true;
    t.notOk(err, err && err.message);
  });

  var replyCount = 0;
  client.on('readable', function() {
    var buf;
    while (buf = client.read()) {
      replyCount ++;
      t.equal(replyCount, 1);
      t.deepEqual(buf, {a: 'WAT', done: true});
      t.ok(writeCalledBack);
      t.end();
    }
  });

  t.strictEqual(ret, false);
});