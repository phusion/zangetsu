var CRC32 = require('./crc32.js').CRC32;

const RECEIVING_COMMAND  = 0,
      RECEIVING_ADD_DATA = 1,
      DISCONNECTED       = 2;

const NO_COMMAND  = 0,
      ADD_COMMAND = 1,
      GET_COMMAND = 2;

function Client(socket, id) {
	this.socket = socket;
	this.id     = id;
	this.state  = RECEIVING_COMMAND;
	this.commandBuffer  = '';
	this.addBuffers = [];
	this.addChecksum = new CRC32();
}

Client.prototype.connected = function() {
	return this.state != DISCONNECTED && typeof(this.socket.fd) == 'number';
}

exports.Client = Client;

exports.RECEIVING_COMMAND  = RECEIVING_COMMAND;
exports.RECEIVING_ADD_DATA = RECEIVING_ADD_DATA;
exports.DISCONNECTED       = DISCONNECTED;

exports.NO_COMMAND        = NO_COMMAND;
exports.ADD_COMMAND       = ADD_COMMAND;
exports.GET_COMMAND       = GET_COMMAND;
