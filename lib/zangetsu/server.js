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
var SocketInputWrapper = require('./socket_input_wrapper.js');
var ReplicaMember = require('./replica_member.js');
var Client        = require('./client.js');
var Database      = require('./database.js');
var Group         = require('./group.js');
var Utils         = require('./utils.js');
var IOUtils       = require('./io_utils.js');

var min       = Utils.min;
var logInput  = IOUtils.logInput;
var logOutput = IOUtils.logOutput;
var disconnectWithError = IOUtils.disconnectWithError;
var writeMessage    = IOUtils.writeMessage;
var getType         = IOUtils.getType;
var parseJsonObject = IOUtils.parseJsonObject;
var parseJsonObjectFromStream = IOUtils.parseJsonObjectFromStream;


const MASTER = 0,
      SLAVE  = 1;


function Server(dbpath) {
	var self = this;
	this.dbpath = dbpath;
	this.replicaMembers = {};
	this.replicaMemberCount = 0;
	this.clients = {};
	this.clientCount = 1;
	this.lastClientId = 0;
	this.role = MASTER;
	this.database = new Database.Database(dbpath);
	this.database.reload();
}

Server.prototype.listen = function(port) {
	var self = this;
	this.server = net.createServer(function(socket) {
		self._onNewClientSocket(socket);
	});
	this.server.listen(port);
}

Server.prototype.listenFD = function(fd) {
	var self = this;
	this.server = net.createServer(function(socket) {
		self._onNewClientSocket(socket);
	});
	this.server.listenFD(fd, 'tcp4');
}

Server.prototype.joinReplicaSet = function(memberHost, memberPort) {
	// TODO
}


/* New clients first go through a handshake process. If the handshake is
 * successful they will be identified as either a normal client or a replica
 * member client and added to the corresponding data structures.
 */
Server.prototype._onNewClientSocket = function(socket) {
	var self = this;
	var handshakeState = {
		socket: socket,
		input : new SocketInputWrapper.SocketInputWrapper(socket),
		buffer: '',
		id: this.lastClientId
	};
	var handshakeMessage = {
		protocolMajor: 1,
		protocolMinor: 0,
		serverName: "Zangetsu/1.0"
	};
	this.lastClientId++;
	console.log("[Client %d] Connected", handshakeState.id);
	socket.setNoDelay();
	socket.setKeepAlive(true);
	writeMessage(socket, handshakeMessage);
	handshakeState.input.onData = function(data) {
		return self._handshake(handshakeState, data);
	}
	socket.on('end', function() {
		socket.destroy();
	})
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
		
		state.socket.removeAllListeners('end');
		logInput(state.buffer);
		
		/* Now identify client and add it to corresponding data structures. */
		
		if (reply.identity == 'replica-member') {
			client = replicaMember = new ReplicaMember.ReplicaMember(this,
				state.socket, state.input, state.id);
			this.replicaMembers[state.id] = replicaMember;
			this.replicaMemberCount++;
			this._logClientConnectionActivity(replicaMember, "Identified as replica member");
			
			state.input.onData = function(data) {
				return replicaMember.onData(data);
			}
			state.socket.on('close', function() {
				self._logClientConnectionActivity(replicaMember, "Connection closed");
				delete self.replicaMembers[replicaMember.id];
				self.replicaMemberCount--;
				console.assert(self.replicaMemberCount >= 0);
			});
			replicaMember.initialize();
			
		} else {
			client = new Client.Client(state.socket, state.input, state.id);
			this.clients[state.id] = client;
			this.clientCount++;
			this._logClientConnectionActivity(client, "Identified as regular client");
			
			reply = { status: 'ok' };
			writeMessage(state.socket, reply);
			
			state.input.onData = function(data) {
				return self._onClientData(client, data);
			}
			state.socket.on('close', function() {
				client.state = Client.DISCONNECTED;
				self._logClientConnectionActivity(client, "Connection closed");
				delete self.clients[client.id];
				self.clientCount--;
				console.assert(self.clientCount >= 0);
			});
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


Server.prototype._onClientData = function(client, data) {
	var consumed = 0, found = false;
	var i, command, result;
	
	while (consumed < data.length && client.connected()) {
		switch (client.state) {
		case Client.RECEIVING_COMMAND:
			try {
				logInput(client.commandBuffer);
				result = parseJsonObjectFromStream(client.commandBuffer, data, consumed);
			} catch (e) {
				disconnectClientWithError(client,
					"Cannot parse command " +
					JSON.stringify(client.commandBuffer) +
					": " + e.message);
				return consumed;
			}
			consumed = result.pos;
			if (result.done) {
				command = result.object;
				client.commandBuffer = '';
				
				if (getType(command.command) != 'string') {
					disconnectClientWithError(client,
						"Command must have a 'command' string field");
					return consumed;
				}
				
				if (this._onClientCommand(client, command)) {
					logInput("onClientClient assumes control");
					return consumed;
				}
			} else {
				client.commandBuffer += result.slice;
			}
			break;
			
		case Client.RECEIVING_ADD_DATA:
			consumed += this._onClientAddData(client, data.slice(consumed));
			return consumed;
		
		default:
			throw 'BUG: unknown client state ' + client.state;
		}
	}
	
	return consumed;
}

Server.prototype._onClientCommand = function(client, command) {
	var self = this;
	if (command.command == 'add') {
		if (getType(command.group) != 'string') {
			disconnectClientWithError(client, "Expecting a 'group' string field");
			return false;
		} else if (!Group.validateGroupName(command.group)) {
			disconnectClientWithError(client, "Invalid group name");
			return false;
		} else if (getType(command.timestamp) != 'number') {
			disconnectClientWithError(client, "Expecting a 'timestamp' number field");
			return false;
		} else if (getType(command.size) != 'number') {
			disconnectClientWithError(client, "Expecting a 'size' number field");
			return false;
		} else if (getType(command.opid) != 'number') {
			disconnectClientWithError(client, "Expecting an 'opid' number field");
			return false;
		} else if (command.size > Database.MAX_SIZE) {
			disconnectClientWithError(client, "Size may not be larger than " +
				Database.MAX_SIZE_DESCRIPTION);
			return false;
		} else if (client.results[command.opid]) {
			disconnectClientWithError(client, "The given opid is already given");
			return false;
		}
		client.currentCommand = command;
		client.currentCommand.remaining = command.size;
		client.state = Client.RECEIVING_ADD_DATA;
		return false;
	
	} else if (command.command == 'results') {
		if (command.discard) {
			client.results = {};
			return false;
		}
		
		function writeResults() {
			writeMessage(client.socket, { status: 'ok', results: client.results });
			client.results = {};
		}
		
		if (client.operationsInProgress == 0) {
			writeResults();
			return false;
		} else {
			/* Block until all background operations are ready. */
			client.input.pause();
			client.once('operationsDrained', function() {
				writeResults();
				client.input.resume();
			});
			return true;
		}
		
	} else if (command.command == 'get') {
		if (getType(command.group) != 'string') {
			disconnectClientWithError(client, "Expecting a 'group' string field");
			return false;
		} else if (getType(command.timestamp) != 'number') {
			disconnectClientWithError(client, "Expecting a 'timestamp' number field");
			return false;
		} else if (getType(command.offset) != 'string') {
			disconnectClientWithError(client, "Expecting an 'offset' string field");
			return false;
		}
		
		client.input.pause();
		database.get(command.group, parseInt(command.timestamp / 60 / 60 / 24), command.offset,
			function(err, data)
		{
			if (err == 'not-found') {
				writeMessage(client.socket, { status: 'not-found' });
			} else if (err) {
				disconnectClientWithError(client, "Cannot get requested data: " +
					err.message);
				return;
			} else {
				writeMessage(client.socket, { status: 'ok', size: data.length });
				client.write(data, function() {
					client.input.resume();
				});
			}
		});
		return true;
		
	} else if (command.command == 'remove') {
		if (getType(command.group) != 'string') {
			disconnectClientWithError(client, "Expecting a 'group' string field");
			return false;
		} else if (command.timestamp && getType(command.timestamp) != 'number') {
			disconnectClientWithError(client, "The 'timestamp' field, if given, must be a number");
			return false;
		}
		
		var done = false;
		var paused = false;
		
		function deleted(err) {
			done = true;
			if (err) {
				writeMessage(client.socket, { status: 'error',
					message: err.message });
			} else {
				writeMessage(client.socket, { status: 'ok' });
			}
			if (paused) {
				client.input.resume();
			}
		}
		
		if (command.timestamp) {
			this.database.remove(command.group, timestamp / 60 / 60 / 24, deleted);
		} else {
			this.database.remove(command.group, undefined, deleted);
		}
		if (done) {
			return false;
		} else {
			client.input.pause();
			paused = true;
			return true;
		}
		
	} else if (command.command == 'ping') {
		writeMessage(client.socket, { status: 'ok' });
		return false;
		
	} else {
		disconnectClientWithError(client, "Unknown command");
		return false;
	}
}

Server.prototype._onClientAddData = function(client, data) {
	var self = this;
	var consumed = 0;
	var done, slice, filename;
	
	consumed = min(client.currentCommand.remaining, data.length);
	client.currentCommand.remaining -= consumed;
	done = client.currentCommand.remaining == 0;
	slice = data.slice(0, consumed);
	client.addBuffers.push(slice);
	// Update checksum here instead of in Database.add() for better cache locality.
	client.addChecksum.update(slice);
	
	if (done) {
		client.incOperations();
		opid = client.currentCommand.opid;
		
		function added(err, offset) {
			if (!client.connected()) {
				self._logClientConnectionActivity(client, "Client no longer connected");
				return;
			} else if (err) {
				disconnectClientWithError(client, "I/O error: " + err.message);
				return;
			}
			client.results[opid] = { status: 'ok', offset: offset };
			client.decOperations(); // Can invoke callbacks.
			if (client.connected()) {
				client.input.resume();
				client.state = Client.RECEIVING_COMMAND;
			}
		}
		
		var group = client.currentCommand.group;
		var timestamp = client.currentCommand.timestamp;
		var buffers = client.addBuffers;
		var checksumBuffer = client.addChecksum.toBuffer();
		
		client.addChecksum.finalize();
		client.currentCommand = undefined;
		client.addBuffers = [];
		client.addChecksum.reset();
		/* Data hasn't been written to disk yet so stop
		 * accepting client data until we're done.
		 */
		client.input.pause();
		
		this.database.add(group, parseInt(timestamp / 60 / 60 / 24),
			buffers, checksumBuffer, added);
	}
	
	return consumed;
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


function disconnectClientWithError(client, message) {
	disconnectWithError(client.socket, message);
	client.state = Client.DISCONNECTED;
}


exports.Server = Server;
exports.MASTER = MASTER;
exports.SLAVE  = SLAVE;
