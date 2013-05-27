# About

[Riak-pb](https://npmjs.org/package/riak-pb#readme) is a Riak Node.js client module that uses Protocol Buffers (instead of HTTP) to talk to Riak. It has the following features:

* Automatically reconnects if disconnected
* Can be configured with an array of nodes, load-balancing between them
* Uses connection pooling, using many connections in parallel to speed up
* Automatically retries if client gets disconnected during request
* Provides a streaming interface for when Riak uses streaming replies

Riak-pb is entirely written in JavaScript, using [protobuf.js](https://npmjs.org/package/protobuf.js) to encode and decode protocol buffers.

Because it makes use of Streams2, Riak-pb is only compatible with Node.js >= 0.10.

Enjoy!