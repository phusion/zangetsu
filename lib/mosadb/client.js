const RECEIVING_COMMAND = 0,
      RECEIVING_BODY    = 1,
      DISCONNECTED      = 2;

const NO_COMMAND  = 0,
      ADD_COMMAND = 1,
      GET_COMMAND = 2;

function Client(socket, id) {
	this.socket = socket;
	this.id     = id;
	this.state  = RECEIVING_COMMAND;
	this.commandBuffer  = '';
}

Client.prototype.connected = function() {
	return this.state != DISCONNECTED;
}

exports.Client = Client;

exports.RECEIVING_COMMAND = RECEIVING_COMMAND;
exports.RECEIVING_BODY    = RECEIVING_BODY;
exports.DISCONNECTED      = DISCONNECTED;

exports.NO_COMMAND        = NO_COMMAND;
exports.ADD_COMMAND       = ADD_COMMAND;
exports.GET_COMMAND       = GET_COMMAND;
