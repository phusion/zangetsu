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
		output: new SocketOutputWrapper.SocketOutputWrapper(socket),
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
	handshakeState.onClose = function() {
		console.log("[Client %d] Connection closed before handshake done", handshakeState.id);
	}
	socket.on('close', handshakeState.onClose);
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
		
		state.socket.removeListener('close', state.onClose);
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
			
			reply = { status: 'ok' };
			writeMessage(state.socket, reply);
			
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

exports.Server = Server;
exports.MASTER = MASTER;
exports.SLAVE  = SLAVE;
