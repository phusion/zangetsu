/**
 * Represents a connection to a normal client.
 */

var util           = require('util');
var CommandHandler = require('./command_handler.js');
var log            = require('./default_log.js').log;

function Client(server, socket, input, output, id) {
	CommandHandler.CommandHandler.call(this, server.database, socket, input, output);
	this.id = id;
}
util.inherits(Client, CommandHandler.CommandHandler);


Client.prototype._log = function(message) {
	var args = ["[Client %d] " + message, this.id];
	for (var i = 1; i < arguments.length; i++) {
		args.push(arguments[i]);
	}
	log.info.apply(log, args);
}

Client.prototype._logDebug = function(message) {
	var args = ["[Client %d DEBUG] " + message, this.id];
	for (var i = 1; i < arguments.length; i++) {
		args.push(arguments[i]);
	}
	log.debug.apply(log, args);
}

Client.prototype._logError = function(message) {
	var args = ["[Client %d ERROR] " + message, this.id];
	for (var i = 1; i < arguments.length; i++) {
		args.push(arguments[i]);
	}
	log.error.apply(log, args);
}


Client.prototype.initialize = function() {
	this.readNextCommand();
}

exports.Client = Client;
exports.DISCONNECTED = CommandHandler.DISCONNECTED;
