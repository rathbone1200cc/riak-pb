var Duplex = require('stream').Duplex;
var butils = require('butils');
var messageCodes = require('./message_codes');
var log = require('../log')('parser');

module.exports =
function Parser(translator) {

  var numBytesAwaiting = 0;
  var resBuffers = [];
  var expectMultiple = false;
  var ended = false;

  var s = new Duplex({objectMode: true, highWaterMark: 0});

  s._write =
  function _write(buf, encoding, callback) {
    log('_write:', buf);
    if (ended) return callback();
    splitPacket(buf);
    if (numBytesAwaiting == 0) doReply();
    callback();
    process.nextTick(function() {
      s.emit('drain');
    });
  };

  s._read = function _read() {};

  /// Expect Multiple

  s.expectMultiple =
  function (v) {
    expectMultiple = v;
  };

  /// Split Packet

  function splitPacket(pkt) {
    var pos = 0, len;
    if (numBytesAwaiting > 0) {
      len = Math.min(pkt.length, numBytesAwaiting);
      var oldBuf = resBuffers[resBuffers.length - 1];
      var newBuf = new Buffer(oldBuf.length + len);
      oldBuf.copy(newBuf, 0);
      pkt.slice(0, len).copy(newBuf, oldBuf.length);
      resBuffers[resBuffers.length - 1] = newBuf;
      pos = len;
      numBytesAwaiting -= len;
    }
    while (pos < pkt.length) {
      len = butils.readInt32(pkt, pos);
      numBytesAwaiting = len + 4 - pkt.length + pos;
      resBuffers.push(pkt.slice(pos + 4, Math.min(pos + len + 4, pkt.length)));
      pos += len + 4;
    }
  }


  /// Reply

  function doReply() {
    log('doing reply, I have %d response buffers', resBuffers.length);
    resBuffers.forEach(function (packet) {
      if (ended) return;
      var mc = messageCodes[packet[0]];
      var response = translator.decode(mc, packet.slice(1));
      if (response.content && Array.isArray(response.content)) {
        response.content.forEach(function (item) {
          if (item.value && item.content_type &&
              item.content_type.match(/^(text\/\*)|(application\/json)$/))
          {
            item.value = item.value.toString();
          }
        });
      }

      log('pushing response buffer %j', response);
      s.push(response);

      if (! expectMultiple || response.done || mc === 'RpbErrorResp') {
        if (! ended) s.emit('done');
      }
    });

  }


  /// Cleanup
  var cleanup =
  s.cleanup =
  function cleanup() {
    log('cleaning up');
    resBuffers = [];
    numBytesAwaiting = 0;
  }


  /// Destroy

  s.destroy =
  function destroy() {
    log('destroying');
    ended = true;
    // HACK: when destroying reset output buffer
    s._readableState.buffer = [];
  };

  return s;
};