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


exports.startAsSlave = function(host, port, publicHostName, masterHost, masterPort) {
	console.assert(this.server === undefined);

	var self = this;
	var masterSocket, input, output;
	var commandHandler;
	var status;

	function readJsonObject(options, callback) {
		if (typeof(options) == 'function') {
			callback = options;
			options  = {};
		}
		IOUtils.readJsonObject(input, function(err, object) {
			if (err) {
				reconnectToMaster("Could not read from master while " + status, err);
			} else if (object) {
				callback(object);
			} else {
				if (options.allowEof) {
					callback();
				} else {
					reconnectToMaster("Connection with master closed unexpectedly while " + status);
				}
			}
		});
	}

	function onConnectionClosed() {
		reconnectToMaster('Connection with master closed unexpectedly');
	}

	function onConnectError(err) {
		onConnect(err);
	}

	function connectToMaster(timeout) {
		masterSocket  = new net.Socket();
		setTimeout(function() {
			masterSocket = new net.Socket();
			input        = new SocketInputWrapper.SocketInputWrapper(masterSocket);
			output       = new SocketOutputWrapper.SocketOutputWrapper(masterSocket);

			masterSocket.allowHalfOpen = false;

			commandHandler = new CommandHandler.CommandHandler(self.database,
				masterSocket, input, output);
			commandHandler._log = function(message) {
				var args = ["[Replication] " + message];
				for (var i = 1; i < arguments.length; i++) {
					args.push(arguments[i]);
				}
				log.info.apply(log, args);
			}
			commandHandler._logDebug = function(message) {
				var args = ["[Replication] " + message];
				for (var i = 1; i < arguments.length; i++) {
					args.push(arguments[i]);
				}
				log.log.apply(log, args);
			}
			commandHandler._logError = function(message) {
				var args = ["[Replication] " + message];
				for (var i = 1; i < arguments.length; i++) {
					args.push(arguments[i]);
				}
				log.error.apply(log, args);
			}


			self._logReplicationActivity("Connecting to master %s:%d",
				masterHost, masterPort);
			masterSocket.once('error', onConnect);
			masterSocket.connect(masterPort, masterHost, onConnect);
		}, timeout);
	}

	function disconnectFromMaster() {
		masterSocket.removeAllListeners();
		masterSocket.destroySoon();
		masterSocket      = undefined;
		masterSocketError = undefined;
		input             = undefined;
		output            = undefined;
		status            = undefined;
		commandHandler    = undefined;
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

	function onConnect(err) {
		if (err) {
			return reconnectToMaster("Cannot connect to master", err);
		}

		masterSocket.removeListener('error', onConnectError);
		masterSocket.on('close', onConnectionClosed);

		self._logReplicationActivity("Connection to master established; handshaking...");
		status = "handshaking with master";

		readJsonObject(function(reply) {
			self._logReplicationActivity("Master sent handshake reply: %j",
				reply);
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
				readJsonObject(function(details) {
					self._logReplicationActivity("Master sent handshake reply: %j",
						details);
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

	function handshakeWithMasterDone() {
		self._logReplicationActivity("Done handshaking with master");

		commandHandler.on('processCommand', function(command) {
			self._logReplicationActivity("Replication command: %j",
				command);
		});

		commandHandler.readNextCommand();
	}

	this.server = net.createServer(function(socket) {
		this._onNewClientSocket(socket);
	}.bind(this));
	this.server.listen(port, host, function() {
		if (!masterHost) {
			masterHost = 'localhost';
		}

		var addr   = self.server.address();
		self.role  = Constants.ROLE_SLAVE;
		self.state = Constants.SS_READY;
		self.host  = Utils.determinePublicHostName(addr.address, publicHostName);
		self.port  = addr.port;
		self.database.start();
		connectToMaster(0);
	});
}

exports._logReplicationActivity = function(message) {
	var args = ["[Replication] " + message];
	for (var i = 1; i < arguments.length; i++) {
		args.push(arguments[i]);
	}
	log.info.apply(log, args);
}
