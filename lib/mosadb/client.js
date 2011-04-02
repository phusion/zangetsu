const RECEIVING_COMMAND = 0,
      RECEIVING_BODY    = 1,
      DISCONNECTED      = 2;

function Client(socket, id) {
	this.socket = socket;
	this.id     = id;
	this.state  = RECEIVING_COMMAND;
	this.commandBuffer = '';
}

Client.prototype.connected = function() {
	return this.state != DISCONNECTED;
}

exports.Client = Client;
exports.RECEIVING_COMMAND = RECEIVING_COMMAND;
exports.RECEIVING_BODY    = RECEIVING_BODY;
exports.DISCONNECTED      = DISCONNECTED;