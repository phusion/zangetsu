var util     = require('util');
var events   = require('events');
var Database = require('./database.js');
var Group    = require('./group.js');
var CRC32    = require('./crc32.js').CRC32;
var Utils    = require('./utils.js');
var IOUtils  = require('./io_utils.js');
var readData       = IOUtils.readData;
var readJsonObject = IOUtils.readJsonObject;
var logInput       = IOUtils.logInput;
var getType        = Utils.getType;


// CommandHandler modes. Some commands are disabled, depending on the chosen mode.
const
	// Allow all commands.
	MODE_NORMAL = exports.MODE_NORMAL = 0,
	// Allow all commands but return an error on data-modifying commands (e.g. 'add').
	MODE_SLAVE  = exports.MODE_SLAVE  = 1;

// States
const
	IDLING              = 0,
	WAITING_FOR_COMMAND = 1,
	PROCESSING_COMMAND  = 2,
	CLOSING             = 3,
	DISCONNECTED        = 4;


function CommandHandler(database, socket, input, output, mode) {
	events.EventEmitter.call(this);
	this.database = database;
	this.socket   = socket;
	this.input    = input;
	this.output   = output;
	this.mode     = mode;
	
	this.state = IDLING;
	this.operationsInProgress = 0;
	this.results = {};
	this.addChecksum = new CRC32();
}
util.inherits(CommandHandler, events.EventEmitter);


CommandHandler.prototype._disconnectWithError = function(message) {
	if (this.connected()) {
		this._logError(message);
		IOUtils.disconnectWithError(this.socket, message);
		this.state = DISCONNECTED;
	}
}

CommandHandler.prototype._disconnect = function(callback) {
	if (this.connected()) {
		this.state = DISCONNECTED;
		if (this.socket.bufferSize > 0) {
			var self = this;
			this.socket.once('drain', function() {
				self.socket.destroy();
				if (callback) {
					callback();
				}
			});
		} else {
			this.socket.destroy();
			if (callback) {
				process.nextTick(callback);
			}
		}
	}
}

CommandHandler.prototype._readJsonObject = function(callback) {
	var self = this;
	readJsonObject(this.input, function(err, object) {
		if (err) {
			if (!err.isIoError) {
				self.socket.emit('error', err);
				self._disconnectWithError("Invalid JSON data.");
			}
		} else if (self.connected()) {
			if (object) {
				callback(object);
			} else {
				self._disconnect();
			}
		}
	});
}

CommandHandler.prototype._write = function(buf, callback) {
	return this.output.write(buf, callback);
}

CommandHandler.prototype._writeJSON = function(buf, callback) {
	return this.output.writeJSON(buf, callback);
}

CommandHandler.prototype._startClosing = function() {
	var self = this;
	console.assert(this.state == IDLING ||
		this.state == WAITING_FOR_COMMAND ||
		this.state == PROCESSING_COMMAND);
	this.state = CLOSING;
	if (this.socket.bufferSize == 0) {
		this.state = DISCONNECTED;
		self.socket.destroy();
		process.nextTick(function() {
			self.emit('closed');
		});
	} else {
		this.socket.once('drain', function() {
			self.state = DISCONNECTED;
			self.socket.destroy();
			self.emit('closed');
		})
	}
}

CommandHandler.prototype.connected = function() {
	return !this.socket.destroyed && (
		this.state == IDLING ||
		this.state == WAITING_FOR_COMMAND ||
		this.state == PROCESSING_COMMAND
	);
}

CommandHandler.prototype.incOperations = function() {
	this.operationsInProgress++;
}

CommandHandler.prototype.decOperations = function() {
	console.assert(this.operationsInProgress > 0);
	this.operationsInProgress--;
	if (this.operationsInProgress == 0) {
		var self = this;
		process.nextTick(function() {
			self.emit('operationsDrained');
		});
	}
}

CommandHandler.prototype.requestClose = function(callback) {
	if (!this.connected()) {
		if (callback) {
			process.nextTick(callback);
		}
		return;
	}

	switch (this.state) {
	case IDLING:
	case WAITING_FOR_COMMAND:
		this._startClosing();
		if (callback) {
			this.once('closed', callback);
		}
		break;
	
	case PROCESSING_COMMAND:
		this.closeRequested = true;
		if (callback) {
			this.once('closed', callback);
		}
		break;

	case CLOSING:
		if (callback) {
			this.once('closed', callback);
		}
		break;

	case DISCONNECTED:
		if (callback) {
			process.nextTick(callback);
		}
		break;
	
	default:
		console.assert(false);
	}
}

CommandHandler.prototype.readNextCommand = function() {
	console.assert(this.state == IDLING);
	this.state = WAITING_FOR_COMMAND;

	if (this.closeRequested) {
		this._startClosing();
		return;
	}

	var self = this;
	readJsonObject(this.input, function(err, object) {
		if (err) {
			if (!err.isIoError) {
				self._disconnectWithError(err);
			}
			// else: already handled by onError/onClose handlers.
		} else if (object) {
			if (getType(object.command) != 'string') {
				self._disconnectWithError(
					"Command must have a 'command' string field");
			} else {
				self.state = PROCESSING_COMMAND;
				self.processCommand(object, function() {
					self.readNextCommand();
				});
			}
		}
	});
}

CommandHandler.prototype.processCommand = function(command, callback) {
	this.state = PROCESSING_COMMAND;
	this.emit('processCommand', command);
	this._logDebug('Processing command: %j', command);
	var handler = this['_handleCommand_' + command.command];
	if (handler) {
		var self = this;
		handler.call(this, command, function() {
			self.state = IDLING;
			callback();
		});
	} else {
		this._disconnectWithError("Unknown command " + command.command);
	}
}

CommandHandler.prototype._handleCommand_add = function(command, callback) {
	if (getType(command.group) != 'string') {
		this._disconnectWithError("Expecting a 'group' string field");
	} else if (!Group.validateGroupName(command.group)) {
		this._disconnectWithError("Invalid group name");
	} else if (getType(command.timestamp) != 'number') {
		this._disconnectWithError("Expecting a 'timestamp' number field");
	} else if (getType(command.size) != 'number') {
		this._disconnectWithError("Expecting a 'size' number field");
	} else if (getType(command.opid) != 'number') {
		this._disconnectWithError("Expecting an 'opid' number field");
	} else if (command.size > Database.MAX_SIZE) {
		this._disconnectWithError("Size may not be larger than " +
			Database.MAX_SIZE_DESCRIPTION);
	} else if (this.results[command.opid]) {
		this._disconnectWithError("The given opid is already used");
	} else if (this.mode != MODE_NORMAL) {
		this._disconnectWithError("This command is not allowed because the server is in slave mode");
	}

	var self = this;
	var buffers = [];

	readData(this.input, command.size, function(err, data, done) {
		if (err || !data) {
			return;
		}

		buffers.push(data);
		// Update checksum here instead of in Database.add() for better cache locality.
		self.addChecksum.update(data);

		if (done) {
			var checksumBuffer, options;

			self.incOperations();
			self.addChecksum.finalize();
			checksumBuffer = self.addChecksum.toBuffer();
			self.addChecksum.reset();

			if (command.corrupted) {
				options = { corrupted: true };
			}

			self.database.add(
				command.opid,
				command.group,
				parseInt(command.timestamp / 60 / 60 / 24),
				buffers,
				checksumBuffer,
				options,
				function(err, offset) {
					if (!self.connected()) {
						return;
					} else if (err) {
						self._disconnectWithError("I/O error: " + err.message);
						return;
					}
					
					self.results[command.opid] = { status: 'ok', offset: offset };
					self.decOperations();
				}
			);

			callback();
		}
	});
}

CommandHandler.prototype._handleCommand_results = function(command, callback) {
	var self = this;

	if (command.discard) {
		this.results = {};
		callback();
		return;
	}
	
	function writeResults() {
		self._writeJSON({ status: 'ok', results: self.results });
		self.results = {};
	}
	
	if (this.operationsInProgress == 0) {
		writeResults();
		callback();
	} else {
		this.once('operationsDrained', function() {
			if (self.connected()) {
				writeResults();
				callback();
			}
		});
	}
}

CommandHandler.prototype._handleCommand_get = function(command, callback) {
	var self = this;

	if (getType(command.group) != 'string') {
		this._disconnectWithError("Expecting a 'group' string field");
	} else if (getType(command.timestamp) != 'number') {
		this._disconnectWithError("Expecting a 'timestamp' number field");
	} else if (getType(command.offset) != 'number') {
		this._disconnectWithError("Expecting an 'offset' number field");
	}
	
	var dayTimestamp = parseInt(command.timestamp / 60 / 60 / 24);
	var offset = parseInt(command.offset);
	this.database.get(command.group, dayTimestamp, offset, function(err, record) {
		if (!self.connected()) {
			return;
		} else if (err) {
			self._disconnectWithError("Cannot get requested data: " +
				err.message);
		} else {
			self._writeJSON({
				status: 'ok',
				size: record.dataSize,
				corrupted: record.corrupted
			});
			self._write(record.data, function(err) {
				if (self.connected() && !err) {
					callback();
				}
			});
		}
	});
}

CommandHandler.prototype._handleCommand_remove = function(command, callback) {
	if (getType(command.group) != 'string') {
		this._disconnectWithError("Expecting a 'group' string field");
	} else if (command.timestamp && getType(command.timestamp) != 'number') {
		this._disconnectWithError("The 'timestamp' field, if given, must be a number");
	} else if (this.mode != MODE_NORMAL) {
		this._disconnectWithError("This command is not allowed because the server is in slave mode");
	}
	
	var self = this;
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
	callback();
}

CommandHandler.prototype._handleCommand_removeOne = function(command, callback) {
	if (getType(command.group) != 'string') {
		this._disconnectWithError("Expecting a 'group' string field");
	} else if (getType(command.dayTimestamp) != 'number') {
		this._disconnectWithError("Expecting a 'dayTimestamp' number field");
	} else if (this.mode != MODE_NORMAL) {
		this._disconnectWithError("This command is not allowed because the server is in slave mode");
	}
	
	var self = this;
	var done = false;
	this.database.removeOne(command.group, command.dayTimestamp, function(err) {
		done = true;
		if (!self.connected()) {
			return;
		} else if (err) {
			self._writeJSON({ status: 'error',
				message: err.message });
		} else {
			self._writeJSON({ status: 'ok' });
		}
	});
	console.assert(done);
	callback();
}

CommandHandler.prototype._handleCommand_getToc = function(command, callback) {
	this._writeJSON(this.database.toTocFormat());
	callback();
}

CommandHandler.prototype._handleCommand_ping = function(command, callback) {
	var self = this;

	function perform() {
		self._writeJSON({ status: 'ok' });
		callback();
	}
	
	if (command.sleep) {
		setTimeout(perform, command.sleep);
	} else {
		perform();
	}
}

exports.CommandHandler = CommandHandler;
exports.DISCONNECTED = DISCONNECTED;
