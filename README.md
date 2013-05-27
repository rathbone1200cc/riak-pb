# riak-pb

Riak Protocol Buffer Client for Node.js.

Features:

* streaming
* fail-over
* automatic retry
* connection pooling
* load balancing

## Install

Include `riak-pb` in your dependency list or install:

```bash
$ npm install riak-pb
```

## Use

### Require

```javascript
var riak = require('riak-pb');
```

### Create Client

```javascript
var client = riak();
```

Or, with options:

```javascript
var options = {
  nodes: [
    {
      host: 'myriakhostname.acme.com',
      port: 8321 }],
  maxPool: 5      // Maximum number of connections in the connection pool - default is 5
  maxRetries: 10, // maximum times the client tries to reconnect before failing - default is 10
  maxDelay: 2000, // maximum time (ms) between reconnections - reconnections haev an exponential backoff, but limited to this value - default is 2000
};

var client = riak(options);
```

### Access Riak

The API is based on [the Riak Protocol spec](http://docs.basho.com/riak/1.3.1/references/apis/protocol-buffers/), check it out to find out about what arguments you need.


#### put(params, callback)

Examples:

```javascript
client.put({
  bucket: 'test',
  key: 'test',
  content: { value: '{"test":"data"}',
  content_type: 'application/json',
  indexes: [{ key: 'test_bin', value: 'test' }] } },
  function (err, reply) {
    //...
  });
```

With indexes:

```javascript
  var indexes = [{ key: 'key1_bin', value: 'value1' }, { key: 'key2_bin', value: 'value2' }];
  var options = { bucket: 'test', key: 'test-put-index', content: { value: '{"test":"data"}', content_type: 'application/json', indexes: indexes }, return_body: true };

  client.put(options, function(err, reply) {
    //...
  });
```

With vector clock:

```javascript
var options = { bucket: 'test', key: 'test-vclock', content: { value: '{"test":"data"}', content_type: 'application/json' }, return_body: true };
client.put(options, function (err, reply) {
  if (err) throw err;
  var options = { bucket: 'test', key: 'test-vclock', content: { value: '{"test":"data"}', content_type: 'application/json' }, return_body: true };
  options.vclock = reply.vclock;
  client.put(options, function(reply) {
    // ...
  });
});
```

#### get(params, callback)

Example:

```javascript
client.get({ bucket: 'test', key: 'test' }, function (err, reply) {
  t.equal(++cbCount, 1);
  t.notOk(err, err && err.message);
  t.ok(Array.isArray(reply.content));
  t.equal(reply.content.length, 1);
  t.equal(reply.content[0].value, '{"test":"data"}');
  t.end();
});
```

#### getIndex(params, callback)

Example:

```javascript
client.getIndex({
  bucket: 'test',
  index: 'test_bin',
  qtype: 0,
  key: 'test' },
  function (err, reply) {
    //...
  });
```

#### setBucket(params, callback)

Example:

```javascript
client.setBucket('test', { allow_mult: true, n_val: 3 },
  function (err, reply) {
    /// ...
  });
```

#### getKeys(bucket[, callback])

With callback:

```javascript
client.getKeys('test', function (err, keys) {
  /// ...
});
```

Streaming:

```javascript
var s = client.getKeys('test');

s.on('readable', function() {
  var key;
  while(key = s.read()) {
    console.log('got key:', key);
  }
});
```

#### search(params[, callback])

With callback:

```javascript
client.search({ index: 'key1_bin', q: 'test' }, function (err, reply) {
  /// ...
});
```

Streaming:

```javascript
var s = client.search({ index: 'key1_bin', q: 'test' });

s.on('readable', function() {
  var res;
  while(res = s.read()) {
    console.log('got res:', res);
  }
});
```

#### mapred(params[, callback])

With callback:

```javascript
var request = {
  inputs: 'test',
  query: [
    {
      map: {
        source: 'function (v) { return [[v.bucket, v.key]]; }',
        language: 'javascript',
        keep: true
      }
    }]};

var params = { request: JSON.stringify(request), content_type: 'application/json' };

client.mapred(params, function (err, responses) {
  /// ...
});
```

Streaming:

```javascript
var request = {
  inputs: 'test',
  query: [
    {
      map: {
        source: 'function (v) { return [[v.bucket, v.key]]; }',
        language: 'javascript',
        keep: true
      }
    }]};

var params = { request: JSON.stringify(request), content_type: 'application/json' };

var s = client.mapred(params);

s.on('readable', function() {
  var res;
  while(res = s.read()) {
    console.log('got res:', res);
  }
});
```

### del(bucket, id[, callback])
### del(bucket, options[, callback])

```javascript
client.del('test', key, function(err) {
  // ...
});
```

or, with options:

```javascript
client.del('test', {key: key, vclock: vclock}, function(err) {
  // ...
});
```

### Disconnect

Queues a disconnect after all the pending requests are complete

```javascript
client.disconnect();
```

#### Others

* getBuckets(callback) // callback(err, buckets)
* getBucket(bucket[, callback]) // callback(err, bucketInfo)
* setBucket(bucket, props[, callback]) // callback(err)
* setClientId (client_id[, callback]) // callback(err)
* getClientId (callback) // callback(err, clientId)
* ping (callback) // callback(err)
* getServerInfo(callback) // callback(err, reply)

#### Events

The client object emits these events:

* 'error' - (err)
* 'warning' - (warning) - Emitted when there is an internal error, like a disconnection. In this case, the client will transparently attempt to reconnect (up to a limit of attempts) and a "warning" will be emitted with the underlying error object.
