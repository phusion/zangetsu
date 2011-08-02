/**
 * Code implementing replication from a master server.
 * Mixin for Server.
 */

var net = require('net');
var SocketInputWrapper  = require('./socket_input_wrapper.js');
var SocketOutputWrapper = require('./socket_output_wrapper.js');
var Constants       = require('./constants.js');
var CommandHandler  = require('./command_handler.js');
var Utils           = require('./utils.js');
var IOUtils         = require('./io_utils.js');
var log             = require('./default_log.js').log;

var parseJsonObjectFromStream = IOUtils.parseJsonObjectFromStream;


exports.startAsSlave = function(host, port, publicHostName, masterHost, masterPort) {
	this._startAsSlave(publicHostName, masterHost, masterPort, function(server) {
		server.listen(port, host);
	});
}

exports.startAsSlaveWithFD = function(fd, publicHostName, masterHost, masterPort) {
	this._startAsSlave(publicHostName, masterHost, masterPort, function(server) {
		server.listenFD(fd);
	});
}

exports._startAsSlave = function(publicHostName, masterHost, masterPort, initialize) {
	console.assert(this.server === undefined);

	var self = this;
	var masterSocket, input, output;
	var masterSocketError;
	var commandHandler;
	var commandBuffer;
	var jsonHandler;

	function receiveJSON(callback) {
		if (masterSocketError) {
			callback(masterSocketError);
		} else {
			jsonHandler = callback;
			input.resume();
		}
	}

	function onData(data) {
		try {
			result = parseJsonObjectFromStream(commandBuffer, data, 0);
		} catch (e) {
			return reconnectToMaster("Cannot parse command " +
				JSON.stringify(commandBuffer) +
				": " + e.message);
		}
		if (result.done) {
			commandBuffer = '';
			var handler = jsonHandler;
			jsonHandler = undefined;
			input.pause();
			handler(undefined, result.object);
			return result.pos;
		} else {
			commandBuffer += result.slice;
			return data.length;
		}
	}

	function onClose(err) {
		if (jsonHandler) {
			var handler = jsonHandler;
			jsonHandler = undefined;
			handler(new Error('Connection with master closed unexpectedly'));
		}
	}

	function connectToMaster(timeout) {
		masterSocket  = new net.Socket();
		commandBuffer = '';
		setTimeout(function() {
			masterSocket = new net.Socket();
			input        = new SocketInputWrapper.SocketInputWrapper(masterSocket);
			output       = new SocketOutputWrapper.SocketOutputWrapper(masterSocket);
			input.onData = onData;

			commandHandler = new CommandHandler.CommandHandler(self.database,
				masterSocket, input, output);
			commandHandler._log = function(message) {
				var args = ["[Replication] " + message];
				for (var i = 1; i < arguments.length; i++) {
					args.push(arguments[i]);
				}
				console.log.apply(console, args);
			}
			commandHandler._logDebug = function(message) {
				var args = ["[Replication] " + message];
				for (var i = 1; i < arguments.length; i++) {
					args.push(arguments[i]);
				}
				console.log.apply(console, args);
			}
			commandHandler._logError = function(message) {
				var args = ["[Replication ERROR] " + message];
				for (var i = 1; i < arguments.length; i++) {
					args.push(arguments[i]);
				}
				console.error.apply(console, args);
			}


			self._logReplicationActivity("Connecting to master %s:%d",
				masterHost, masterPort);
			masterSocket.once('error', function(err) {
				onConnect(err);
			});
			masterSocket.connect(masterPort, masterHost, onConnect);
		}, timeout);
	}

	function disconnectFromMaster() {
		masterSocket.destroySoon();
		masterSocket   = undefined;
		masterSocketError = undefined;
		input          = undefined;
		output         = undefined;
		commandHandler = undefined;
		commandBuffer  = '';
		jsonHandler    = undefined;
	}

	function reconnectToMaster(reason, err) {
		if (err) {
			reason += " (" + err.message + ")";
		}
		reason += "; retrying in 5 seconds";
		self._logReplicationActivity("%s", reason);
		disconnectFromMaster();
		connectToMaster(5000);
	}

	function onEof() {
		var error = new Error('Connection with master closed unexpectedly');
		if (jsonHandler) {
			var handler = jsonHandler;
			jsonHandler = undefined;
			handler(error);
		} else {
			masterSocketError = error;
		}
	}

	function onConnect(err) {
		if (err) {
			return reconnectToMaster("Cannot connect to master", err);
		}

		masterSocket.on('end', onEof);
		masterSocket.on('error', onEof);

		self._logReplicationActivity("Connection to master established; handshaking...");
		receiveJSON(function(err, reply) {
			if (err) {
				return reconnectToMaster(
					"Cannot handshake with master",
					err);
			}

			self._logReplicationActivity("Master sent handshake reply: %s",
				JSON.stringify(reply));
			if (reply.protocolMajor != Constants.PROTOCOL_MAJOR) {
				return reconnectToMaster("Master server speaks unsupported major protocol " +
					reply.protocolMajor + "; expecting " + Constants.PROTOCOL_MAJOR);
			}
			if (reply.protocolMinor > Constants.PROTOCOL_MINOR) {
				return reconnectToMaster("Master server speaks unsupported minor protocol " +
					reply.protocolMinor + "; expecting <= " + Constants.PROTOCOL_MINOR);
			}
			if (reply.role != "master") {
				return reconnectToMaster("The server we connected to is not a master");
			}

			output.writeJSON({ identity: 'replica-slave' }, function(err) {
				if (err) {
					return reconnectToMaster(
						"Cannot handshake with master",
						err);
				}
				receiveJSON(function(err, details) {
					if (err) {
						return reconnectToMaster(
							"Cannot handshake with master",
							err);
					}

					self._logReplicationActivity("Master sent handshake reply: %s",
						JSON.stringify(details));
					if (details.status == 'ok') {
						handshakeWithMasterDone();
					} else {
						reconnectToMaster(
							"Unknown handshake reply from master (" +
							JSON.stringify(details) + ")");
					}
				});
			});
		});
	}

	function handshakeWithMasterDone(masterHostName, masterPort) {
		self._logReplicationActivity("Done handshaking with master");

		masterSocket.removeListener('end', onEof);
		masterSocket.removeListener('error', onEof);
		input.resume();

		commandHandler.on('command', function(command) {
			self._logReplicationActivity("Replication command: %s",
				JSON.stringify(command));
		});

		input.onData = function(data) {
			return commandHandler.onData(data);
		}
		
		masterSocket.on('close', function() {
			reconnectToMaster("Master closed the connection");
		});
	}

	this.server = net.createServer(function(socket) {
		this._onNewClientSocket(socket);
	}.bind(this));
	initialize(this.server);

	var addr   = this.server.address();
	this.role  = Constants.ROLE_SLAVE;
	this.state = Constants.SS_READY;
	this.host  = Utils.determinePublicHostName(addr.address, publicHostName);
	this.port  = addr.port;
	this.database.start();
	connectToMaster(0);
}

exports._logReplicationActivity = function(message) {
	var args = ["[Replication] " + message];
	for (var i = 1; i < arguments.length; i++) {
		args.push(arguments[i]);
	}
	log.info.apply(log, args);
}
