/*
 *  Zangetsu - high-performance append-only database
 *  Copyright (C) 2011  Phusion
 *
 *  This program is free software: you can redistribute it and/or modify
 *  it under the terms of the GNU Affero General Public License as published by
 *  the Free Software Foundation, either version 3 of the License, or
 *  (at your option) any later version.
 *
 *  This program is distributed in the hope that it will be useful,
 *  but WITHOUT ANY WARRANTY; without even the implied warranty of
 *  MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 *  GNU Affero General Public License for more details.
 *
 *  You should have received a copy of the GNU Affero General Public License
 *  along with this program.  If not, see <http://www.gnu.org/licenses/>.
 */

var sys  = require('sys');
var fs   = require('fs');
var path = require('path');
var net  = require('net');
var util = require('util');
var SocketInputWrapper  = require('./socket_input_wrapper.js');
var SocketOutputWrapper = require('./socket_output_wrapper.js');
var HashSet       = require('./hashset.js');
var CommandHandler = require('./command_handler.js');
var ReplicaMember = require('./replica_member.js');
var Client        = require('./client.js');
var Database      = require('./database.js');
var Utils         = require('./utils.js');
var IOUtils       = require('./io_utils.js');

var logInput  = IOUtils.logInput;
var logOutput = IOUtils.logOutput;
var writeMessage        = IOUtils.writeMessage;
var disconnectWithError = IOUtils.disconnectWithError;
var parseJsonObject     = IOUtils.parseJsonObject;
var parseJsonObjectFromStream = IOUtils.parseJsonObjectFromStream;


const DEFAULT_PORT   = 6420;

const PROTOCOL_MAJOR = 1,
      PROTOCOL_MINOR = 0;

const UNKNOWN = 0,
      MASTER  = 1,
      SLAVE   = 2;

const UNINITIALIZED = 0,
      READY         = 2,
      CLOSED        = 3;


function Server(dbpath) {
	var self = this;
	this.dbpath = dbpath;
	this.replicaMembers = {};
	this.replicaMemberCount = 0;
	this.unidentifiedClients = new HashSet.HashSet();
	this.clients = {};
	this.clientCount = 1;
	this.lastClientId = 0;
	this.role = UNKNOWN;
	this.state = UNINITIALIZED;
	this.database = new Database.Database(dbpath);
	this.database.reload();
}

Server.prototype.startAsMaster = function(host, port, publicHostName) {
	this._startAsMaster(publicHostName, function(server) {
		server.listen(port, host);
	});
}

Server.prototype.startAsMasterWithFD = function(fd, publicHostName) {
	this._startAsMaster(publicHostName, function(server) {
		server.listenFD(fd);
	});
}

Server.prototype._startAsMaster = function(publicHostName, initialize) {
	console.assert(this.server === undefined);
	
	this.server = net.createServer(function(socket) {
		this._onNewClientSocket(socket);
	}.bind(this));
	initialize(this.server);
	
	var addr = this.server.address();
	this.role  = MASTER;
	this.state = READY;
	this.host  = Utils.determinePublicHostName(addr.address, publicHostName);
	this.port  = addr.port;
	this.database.start();
}

Server.prototype.close = function() {
	var self = this;
	this.state = CLOSED;
	this.server.once('close', function() {
		self.disconnectAllClients();
		self.database.close();
	});
	this.server.close();
}

Server.prototype.disconnectAllClients = function() {
	var unidentifiedClients = this.unidentifiedClients.values();
	var i, clientId, client;
	
	for (i = 0; i < unidentifiedClients.length; i++) {
		client = unidentifiedClients[i];
		client.close();
	}
	
	for (clientId in this.clients) {
		client = this.clients[clientId];
		client.close();
	}
}

Server.prototype.inspect = function() {
	var result = {
		dbpath: this.database.dbpath,
		role: (this.role == MASTER) ? 'MASTER' : 'SLAVE',
		replicaMemberCount: this.replicaMemberCount,
		clientCount: this.clientCount
	};
	return util.inspect(result);
}

Server.prototype.getRoleName = function() {
	if (this.role == MASTER) {
		return 'master';
	} else if (this.role == SLAVE) {
		return 'slave';
	} else {
		return 'unknown';
	}
}

Server.prototype.getStateName = function() {
	switch (this.state) {
	case UNINITIALIZED:
		return 'uninitialized';
	case READY:
		return 'ready';
	case CLOSED:
		return 'closed';
	default:
		throw new Error('BUG: unknown state ' + this.state);
	}
}

Server.prototype.getTopology = function() {
	var id, replicaMember;
	var topology = {
		replica_members: [{
			host: this.host,
			port: this.port,
			role: this.getRoleName(),
			state: this.getStateName()
		}]
	};
	
	for (id in this.replicaMembers) {
		replicaMember = this.replicaMembers[id];
		topology.replica_members.push({
			host: replicaMember.host,
			port: replicaMember.port,
			role: replicaMember.getRoleName(),
			state: replicaMember.getStateName()
		});
	}
	
	return toplogy;
}


/* New clients first go through a handshake process. If the handshake is
 * successful they will be identified as either a normal client or a replica
 * member client and added to the corresponding data structures.
 */
Server.prototype._onNewClientSocket = function(socket) {
	if (this.state != READY) {
		socket.destroy();
		return;
	}
	
	var self = this;
	var handshakeState = {
		socket: socket,
		input : new SocketInputWrapper.SocketInputWrapper(socket),
		output: new SocketOutputWrapper.SocketOutputWrapper(socket),
		buffer: '',
		id: this.lastClientId
	};
	var handshakeMessage = {
		protocolMajor: 1,
		protocolMinor: 0,
		serverName: "Zangetsu/1.0",
		host: this.host,
		port: this.port,
		role: this.getRoleName()
	};
	
	this.lastClientId++;
	console.log("[Client %d] Connected", handshakeState.id);
	socket.setNoDelay();
	socket.setKeepAlive(true);
	writeMessage(socket, handshakeMessage);
	
	handshakeState.input.onData = function(data) {
		return self._handshake(handshakeState, data);
	}
	handshakeState.equals = function(o) {
		return handshakeState === o;
	}
	handshakeState.hashCode = function() {
		return handshakeState.id;
	}
	handshakeState.close = function() {
		socket.destroy();
	}
	handshakeState.onSocketClose = function() {
		console.log("[Client %d] Connection closed before handshake done", handshakeState.id);
		self.unidentifiedClients.remove(handshakeState);
	}
	
	socket.on('close', handshakeState.onSocketClose);
	this.unidentifiedClients.add(handshakeState);
}

Server.prototype._handshake = function(state, data) {
	var consumed, reply;
	var found = false;
	
	// Consume everything until newline.
	for (consumed = 0; consumed < data.length && !found; consumed++) {
		if (data[consumed] == 10) {
			found = true;
		}
	}
	state.buffer += data.toString('utf8', 0, consumed);
	
	if (found) {
		/* Handshake complete: we got a complete line,
		 * which should contain JSON. Parse and process it.
		 */
		try {
			reply = parseJsonObject(state.buffer);
		} catch (e) {
			disconnectWithError(state.socket,
				'Command cannot be parsed: ' + e.message);
			return consumed;
		}
		
		var self = this;
		var client, replicaMember;
		
		state.socket.removeListener('close', state.onSocketClose);
		this.unidentifiedClients.remove(state);
		logInput(state.buffer);
		
		/* Now identify client and add it to corresponding data structures. */
		
		if (reply.identity == 'replica-member') {
			client = replicaMember = new ReplicaMember.ReplicaMember(this,
				state.socket, state.input, state.output, state.id);
			this.replicaMembers[state.id] = replicaMember;
			this.replicaMemberCount++;
			this._logClientConnectionActivity(replicaMember, "Identified as replica member");
			
			state.input.onData = function(data) {
				return replicaMember.onData(data);
			}
			state.socket.on('close', function() {
				replicaMember.state = ReplicaMember.DISCONNECTED;
				self._logClientConnectionActivity(replicaMember, "Connection closed");
				delete self.replicaMembers[replicaMember.id];
				self.replicaMemberCount--;
				console.assert(self.replicaMemberCount >= 0);
			});
			
			replicaMember.initialize();
			
		} else {
			client = new Client.Client(this, state.socket, state.input,
				state.output, state.id);
			this.clients[state.id] = client;
			this.clientCount++;
			this._logClientConnectionActivity(client, "Identified as regular client");
			
			state.input.onData = function(data) {
				return client.onData(data);
			}
			state.socket.on('close', function() {
				client.state = Client.DISCONNECTED;
				self._logClientConnectionActivity(client, "Connection closed");
				delete self.clients[client.id];
				self.clientCount--;
				console.assert(self.clientCount >= 0);
			});
			
			reply = { status: 'ok' };
			writeMessage(state.socket, reply);
		}
		
		state.socket.on('error', function(err) {
			if (err.code != 'ECONNRESET' && err.code != 'EPIPE') {
				self._logClientConnectionActivity(client,
					"Socket error: %s", err);
			}
			// Socket is automatically closed after error.
		});
	}
	
	return consumed;
}


Server.prototype.startAsSlave = function(host, port, publicHostName, masterHost, masterPort) {
	this._startAsSlave(publicHostName, masterHost, masterPort, function(server) {
		server.listen(port, host);
	});
}

Server.prototype.startAsSlaveWithFD = function(fd, publicHostName, masterHost, masterPort) {
	this._startAsSlave(publicHostName, masterHost, masterPort, function(server) {
		server.listenFD(fd);
	});
}

Server.prototype._startAsSlave = function(publicHostName, masterHost, masterPort, initialize) {
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
			
			
			self._logReplicationActivity("Connecting to master " + masterHost + ":" + masterPort);
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
		self._logReplicationActivity(reason);
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
			
			self._logReplicationActivity("Master sent handshake reply: " +
				JSON.stringify(reply));
			if (reply.protocolMajor != PROTOCOL_MAJOR) {
				return reconnectToMaster("Master server speaks unsupported major protocol " +
					reply.protocolMajor + "; expecting " + PROTOCOL_MAJOR);
			}
			if (reply.protocolMinor > PROTOCOL_MINOR) {
				return reconnectToMaster("Master server speaks unsupported minor protocol " +
					reply.protocolMinor + "; expecting <= " + PROTOCOL_MINOR);
			}
			if (reply.role != "master") {
				return reconnectToMaster("The server we connected to is not a master");
			}
			
			output.writeJSON({ identity: 'replica-member' }, function(err) {
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
					
					self._logReplicationActivity("Master sent handshake reply: " +
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
		
		input.onData = function(data) {
			return commandHandler.onData(data);
		}
		
		masterSocket.on('end', function() {
			reconnectToMaster("Master closed the connection");
		});
		masterSocket.on('error', function(err) {
			reconnectToMaster("Master socket error", err);
		});
		masterSocket.on('close', function() {
			reconnectToMaster("Master closed the connection");
		});
	}
	
	this.server = net.createServer(function(socket) {
		this._onNewClientSocket(socket);
	}.bind(this));
	initialize(this.server);
	
	var addr   = this.server.address();
	this.role  = SLAVE;
	this.state = READY;
	this.host  = Utils.determinePublicHostName(addr.address, publicHostName);
	this.port  = addr.port;
	this.database.start();
	connectToMaster(0);
}


Server.prototype._logClientConnectionActivity = function(client, message) {
	var args, i;
	if (client._log) {
		args = [message];
		for (i = 2; i < arguments.length; i++) {
			args.push(arguments[i]);
		}
		client._log.apply(client, args);
	} else {
		args = ["[Client %d] " + message, client.id];
		for (i = 2; i < arguments.length; i++) {
			args.push(arguments[i]);
		}
		console.log.apply(console, args);
	}
}

Server.prototype._logReplicationActivity = function(message) {
	console.log("[Replication] " + message);
}

exports.Server = Server;
exports.DEFAULT_PORT = DEFAULT_PORT;
exports.MASTER = MASTER;
exports.SLAVE  = SLAVE;
