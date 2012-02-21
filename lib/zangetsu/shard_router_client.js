/**
 * Represents a connection from a normal client to a shard router.
 */

var util           = require('util');
var Client         = require('./client.js').Client;
var CommandHandler = require('./command_handler.js');
var log            = require('./default_log.js').log;
var ShardedDatabase = require('./sharded_database.js').Database;
var Group    = require('./group.js');

function ShardRouterClient(server, socket, input, output, mode, id) {
	var shardedDatabase = new ShardedDatabase(server.database);
	this.router = server.database;
	Client.call(this, {database: shardedDatabase}, socket, input, output, mode);
}
util.inherits(ShardRouterClient, Client);

ShardRouterClient.prototype._handleCommand_listLocks = function(command, callback) {
	this._writeJSON({ status: 'ok', result: this.router.listLocks()});
	callback();
}

ShardRouterClient.prototype._handleCommand_giveLock = function(command, callback) {
	var self = this;
	if (getType(command.group) != 'string') {
		this._disconnectWithError("Expecting a 'group' string field");
		return;
	} else if (getType(command.identifier) != 'string') {
		this._disconnectWithError("Expecting a 'identifier' string field");
		return;
	} else if (!Group.validateGroupName(command.group)) {
		this._disconnectWithError("Invalid group name");
		return;
	} else if (getType(command.dayTimestamp) != 'number') {
		this._disconnectWithError("Expecting a 'dayTimestamp' number field");
		return;
	}

	this.router.giveLock(command.identifier, command.group, command.dayTimestamp, function(locked) {
		if(locked) {
			self._writeJSON({ status: 'ok', result: locked});
		} else {
			self._writeJSON({ status: 'denied', message: "Lock request denied because higher priority lock existed"});
		}
		callback();
	});
}

ShardRouterClient.prototype._handleCommand_releaseLock = function(command, callback) {
	if (getType(command.group) != 'string') {
		this._disconnectWithError("Expecting a 'group' string field");
		return;
	} else if (getType(command.identifier) != 'string') {
		this._disconnectWithError("Expecting a 'identifier' string field");
		return;
	} else if (!Group.validateGroupName(command.group)) {
		this._disconnectWithError("Invalid group name");
		return;
	} else if (getType(command.dayTimestamp) != 'number') {
		this._disconnectWithError("Expecting a 'dayTimestamp' number field");
		return;
	} else if (command.result) {
		if(getType(command.result.command) != 'string') {
			this._disconnectWithError("Expecting a 'command' string field in the result");
		}
		//TODO some more validation on commands
	}
	this.router.releaseLock(command.group, command.dayTimestamp, command.result);
	this._writeJSON({ status: 'ok' });
	callback();
}

exports.ShardRouterClient = ShardRouterClient;
exports.DISCONNECTED = CommandHandler.DISCONNECTED;
