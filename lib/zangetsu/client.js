var util     = require('util');
var events   = require('events');
var Database = require('./database.js');
var Group    = require('./group.js');
var CRC32    = require('./crc32.js').CRC32;
var Utils    = require('./utils.js');
var IOUtils  = require('./io_utils.js');
var parseJsonObjectFromStream = IOUtils.parseJsonObjectFromStream;
var logInput                  = IOUtils.logInput;
var min                       = Utils.min;
var getType                   = Utils.getType;


const RECEIVING_COMMAND  = 0,
      RECEIVING_ADD_DATA = 1,
      DISCONNECTED       = 2;

const NO_COMMAND  = 0,
      ADD_COMMAND = 1,
      GET_COMMAND = 2;


function Client(server, socket, input, output, id) {
	events.EventEmitter.call(this);
	this.server   = server;
	this.socket   = socket;
	this.input    = input;
	this.output   = output;
	this.id       = id;
	this.database = server.database;
	
	this.state  = RECEIVING_COMMAND;
	this.commandBuffer = '';
	this.operationsInProgress = 0;
	this.results = {};
	this.addBuffers = [];
	this.addChecksum = new CRC32();
}
util.inherits(Client, events.EventEmitter);


Client.prototype._disconnectWithError = function(message) {
	if (this.connected()) {
		IOUtils.disconnectWithError(this.socket, message);
		this.state = DISCONNECTED;
	}
}

Client.prototype._disconnect = function() {
	if (this.connected()) {
		this.socket.destroy();
		this.state = DISCONNECTED;
	}
}

Client.prototype._log = function(message) {
	var args = ["[Client %d] " + message, this.id];
	for (var i = 1; i < arguments.length; i++) {
		args.push(arguments[i]);
	}
	console.log.apply(console, args);
}

Client.prototype._logDebug = function(message) {
	var args = ["[Client %d DEBUG] " + message, this.id];
	for (var i = 1; i < arguments.length; i++) {
		args.push(arguments[i]);
	}
	console.log.apply(console, args);
}

Client.prototype._logError = function(message) {
	var args = ["[Client %d ERROR] " + message, this.id];
	for (var i = 1; i < arguments.length; i++) {
		args.push(arguments[i]);
	}
	console.error.apply(console, args);
}

Client.prototype._write = function(buf, callback) {
	return this.output.write(buf, callback);
}

Client.prototype._writeJSON = function(buf, callback) {
	return this.output.writeJSON(buf, callback);
}

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

Client.prototype.onData = function(data) {
	var consumed = 0, found = false;
	var i, command, result;
	
	while (consumed < data.length && this.connected()) {
		switch (this.state) {
		case RECEIVING_COMMAND:
			try {
				logInput(this.commandBuffer);
				result = parseJsonObjectFromStream(this.commandBuffer, data, consumed);
			} catch (e) {
				this._disconnectWithError(
					"Cannot parse command " +
					JSON.stringify(this.commandBuffer) +
					": " + e.message);
				return consumed;
			}
			consumed = result.pos;
			if (result.done) {
				command = result.object;
				this.commandBuffer = '';
				
				if (getType(command.command) != 'string') {
					disconnectClientWithError(
						"Command must have a 'command' string field");
					return consumed;
				}
				
				if (this._onClientCommand(command)) {
					logInput("onClientClient assumes control");
					return consumed;
				}
			} else {
				this.commandBuffer += result.slice;
			}
			break;
			
		case RECEIVING_ADD_DATA:
			consumed += this._onClientAddData(data.slice(consumed));
			return consumed;
		
		default:
			throw 'BUG: unknown client state ' + this.state;
		}
	}
	
	return consumed;
}

Client.prototype._onClientCommand = function(command) {
	var self = this;
	
	if (command.command == 'add') {
		if (getType(command.group) != 'string') {
			this._disconnectWithError("Expecting a 'group' string field");
			return false;
		} else if (!Group.validateGroupName(command.group)) {
			this._disconnectWithError("Invalid group name");
			return false;
		} else if (getType(command.timestamp) != 'number') {
			this._disconnectWithError("Expecting a 'timestamp' number field");
			return false;
		} else if (getType(command.size) != 'number') {
			this._disconnectWithError("Expecting a 'size' number field");
			return false;
		} else if (getType(command.opid) != 'number') {
			this._disconnectWithError("Expecting an 'opid' number field");
			return false;
		} else if (command.size > Database.MAX_SIZE) {
			this._disconnectWithError("Size may not be larger than " +
				Database.MAX_SIZE_DESCRIPTION);
			return false;
		} else if (this.results[command.opid]) {
			this._disconnectWithError("The given opid is already given");
			return false;
		}
		this.currentCommand = command;
		this.currentCommand.remaining = command.size;
		this.state = RECEIVING_ADD_DATA;
		return false;
	
	} else if (command.command == 'results') {
		if (command.discard) {
			this.results = {};
			return false;
		}
		
		function writeResults() {
			self._writeJSON({ status: 'ok', results: self.results });
			self.results = {};
		}
		
		if (this.operationsInProgress == 0) {
			writeResults();
			return false;
		} else {
			/* Block until all background operations are ready. */
			this.input.pause();
			this.once('operationsDrained', function() {
				if (self.connected()) {
					writeResults();
					self.input.resume();
				}
			});
			return true;
		}
		
	} else if (command.command == 'get') {
		if (getType(command.group) != 'string') {
			this._disconnectWithError("Expecting a 'group' string field");
			return false;
		} else if (getType(command.timestamp) != 'number') {
			this._disconnectWithError("Expecting a 'timestamp' number field");
			return false;
		} else if (getType(command.offset) != 'string') {
			this._disconnectWithError("Expecting an 'offset' string field");
			return false;
		}
		
		this.input.pause();
		var dayTimestamp = parseInt(command.timestamp / 60 / 60 / 24);
		this.database.get(command.group, dayTimestamp, command.offset, function(err, data) {
			if (!self.connected()) {
				return;
			} else if (err == 'not-found') {
				self._writeJSON({ status: 'not-found' });
			} else if (err) {
				self._disconnectWithError("Cannot get requested data: " +
					err.message);
				return;
			} else {
				self._writeJSON({ status: 'ok', size: data.length });
				self._write(data, function() {
					if (self.connected()) {
						self.input.resume();
					}
				});
			}
		});
		return true;
		
	} else if (command.command == 'remove') {
		if (getType(command.group) != 'string') {
			this._disconnectWithError("Expecting a 'group' string field");
			return false;
		} else if (command.timestamp && getType(command.timestamp) != 'number') {
			this._disconnectWithError("The 'timestamp' field, if given, must be a number");
			return false;
		}
		
		var done = false;
		
		function deleted(err) {
			done = true;
			if (!self.connected()) {
				return;
			} else if (err) {
				self._writeJSON({ status: 'error',
					message: err.message });
			} else {
				self._writeJSON({ status: 'ok' });
			}
		}
		
		if (command.timestamp) {
			var dayTimestamp = parseInt(command.timestamp / 60 / 60 / 24);
			this.database.remove(command.group, dayTimestamp, deleted);
		} else {
			this.database.remove(command.group, undefined, deleted);
		}
		console.assert(done);
		return false;
		
	} else if (command.command == 'ping') {
		this._writeJSON({ status: 'ok' });
		return false;
		
	} else {
		this._disconnectWithError("Unknown command");
		return false;
	}
}

Client.prototype._onClientAddData = function(data) {
	var self = this;
	var consumed = 0;
	var done, slice, filename;
	
	consumed = min(this.currentCommand.remaining, data.length);
	this.currentCommand.remaining -= consumed;
	done = this.currentCommand.remaining == 0;
	slice = data.slice(0, consumed);
	this.addBuffers.push(slice);
	// Update checksum here instead of in Database.add() for better cache locality.
	this.addChecksum.update(slice);
	
	if (done) {
		this.incOperations();
		opid = this.currentCommand.opid;
		
		function added(err, offset) {
			if (!self.connected()) {
				return;
			} else if (err) {
				self._disconnectWithError("I/O error: " + err.message);
				return;
			}
			
			self.results[opid] = { status: 'ok', offset: offset };
			self.decOperations(); // Can invoke callbacks.
			if (self.connected()) {
				self.input.resume();
				self.state = RECEIVING_COMMAND;
			}
		}
		
		var group = this.currentCommand.group;
		var timestamp = this.currentCommand.timestamp;
		var buffers = this.addBuffers;
		var checksumBuffer = this.addChecksum.toBuffer();
		
		this.addChecksum.finalize();
		this.currentCommand = undefined;
		this.addBuffers = [];
		this.addChecksum.reset();
		/* Data hasn't been written to disk yet so stop
		 * accepting client data until we're done.
		 */
		this.input.pause();
		
		this.database.add(group, parseInt(timestamp / 60 / 60 / 24),
			buffers, checksumBuffer, added);
	}
	
	return consumed;
}

exports.Client = Client;
