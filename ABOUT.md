# About

Riak-pb is a Riak Node.js client module that uses Protocol Buffers (instead of HTTP) to talk to Riak. It has the following features:

* Automatically reconnects if disconnected
* Can be configured with an array of nodes, load-balancing between them
* Uses connection pooling, using many connections in parallel to speed up
* Automatically retries if client gets disconnected during request

Riak-pb is entirely written in JavaScript, using [protobuf.js](https://npmjs.org/package/protobuf.js) to encode and decode protocol buffers.

Enjoy!