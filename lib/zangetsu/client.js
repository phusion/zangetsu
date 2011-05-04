var util   = require('util');
var events = require('events');
var CRC32  = require('./crc32.js').CRC32;

const RECEIVING_COMMAND  = 0,
      RECEIVING_ADD_DATA = 1,
      DISCONNECTED       = 2;

const NO_COMMAND  = 0,
      ADD_COMMAND = 1,
      GET_COMMAND = 2;

function Client(socket, input, id) {
	events.EventEmitter.call(this);
	this.socket = socket;
	this.id     = id;
	this.input  = input;
	this.state  = RECEIVING_COMMAND;
	this.commandBuffer = '';
	this.operationsInProgress = 0;
	this.results = {};
	this.addBuffers = [];
	this.addChecksum = new CRC32();
}
util.inherits(Client, events.EventEmitter);

Client.prototype.connected = function() {
	return this.state != DISCONNECTED && typeof(this.socket.fd) == 'number';
}

Client.prototype.incOperations = function() {
	this.operationsInProgress++;
}

Client.prototype.decOperations = function() {
	this.operationsInProgress--;
	if (this.operationsInProgress == 0) {
		this.emit('operationsDrained');
	}
}

exports.Client = Client;

exports.RECEIVING_COMMAND  = RECEIVING_COMMAND;
exports.RECEIVING_ADD_DATA = RECEIVING_ADD_DATA;
exports.DISCONNECTED       = DISCONNECTED;

exports.NO_COMMAND        = NO_COMMAND;
exports.ADD_COMMAND       = ADD_COMMAND;
exports.GET_COMMAND       = GET_COMMAND;
