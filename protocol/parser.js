var Duplex = require('stream').Duplex;
var butils = require('butils');
var messageCodes = require('./message_codes');

module.exports =
function Parser(translator) {

  var numBytesAwaiting = 0;
  var resBuffers = [];
  var reply = {};
  var expectMultiple = false;

  var s = new Duplex({objectMode: true, highWaterMark: 0});

  s._write =
  function _write(buf, encoding, callback) {
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
    resBuffers.forEach(function (packet) {
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

      s.push(response);

      reply = merge(reply, response);

      if (! expectMultiple || reply.done || mc === 'RpbErrorResp') {
        cleanup();
        s.emit('done');
      }
    });

  }


  /// Cleanup

  function cleanup() {
    reply = {};
    resBuffers = [];
    numBytesAwaiting = 0;
  }

  return s;
};


/// Merge

function merge(obj1, obj2) {
  var obj = {};
  if (obj2.hasOwnProperty('phase')) {
    obj = obj1;
    if (obj[obj2.phase] === undefined) obj[obj2.phase] = [];
    obj[obj2.phase] = obj[obj2.phase].concat(JSON.parse(obj2.response));
  } else {
    [obj1, obj2].forEach(function (old) {
      Object.keys(old).forEach(function (key) {
        if (!old.hasOwnProperty(key)) return;
        if (Array.isArray(old[key])) {
          if (!obj[key]) obj[key] = [];
          obj[key] = obj[key].concat(old[key]);
        } else {
          obj[key] = old[key];
        }
      });
    });
  }
  return obj;
}