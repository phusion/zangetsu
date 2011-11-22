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
var Constants       = require('./constants.js');
var HashSet         = require('./hashset.js');
var CommandHandler  = require('./command_handler.js');
var ReplicaSlave    = require('./replica_slave.js');
var ReplicationMode = require('./replication_mode.js');
var Client          = require('./client.js');
var Database        = require('./database.js');
var Utils           = require('./utils.js');
var IOUtils         = require('./io_utils.js');
var log             = require('./default_log.js').log;

var logInput  = IOUtils.logInput;
var logOutput = IOUtils.logOutput;
var writeMessage        = IOUtils.writeMessage;
var disconnectWithError = IOUtils.disconnectWithError;
var parseJsonObject     = IOUtils.parseJsonObject;
var parseJsonObjectFromStream = IOUtils.parseJsonObjectFromStream;

exports.close = function() {
	var self = this;
	this.state = Constants.SS_CLOSED;
	this.server.once('close', function() {
		self.disconnectAllClients();
		self.database.close();
	});
	this.server.close();
}

exports.disconnectAllClients = function() {
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
	
	for (clientId in this.replicaSlaves) {
		client = this.replicaSlaves[clientId];
		client.close();
	}
}

exports.getStateName = function() {
	switch (this.state) {
	case Constants.SS_UNINITIALIZED:
		return 'uninitialized';
	case Constants.SS_READY:
		return 'ready';
	case Constants.SS_CLOSED:
		return 'closed';
	default:
		throw new Error('BUG: unknown state ' + this.state);
	}
}

/* New clients first go through a handshake process. If the handshake is
 * successful they will be identified as either a normal client or a replica
 * slave client and added to the corresponding data structures.
 */
exports._onNewClientSocket = function(socket) {
	if (this.state != Constants.SS_READY) {
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
	log.info("[Client %d] Connected", handshakeState.id);
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
		log.info("[Client %d] Connection closed before handshake done", handshakeState.id);
		self.unidentifiedClients.remove(handshakeState);
	}
	
	socket.on('close', handshakeState.onSocketClose);
	this.unidentifiedClients.add(handshakeState);
}

exports._handshake = function(state, data) {
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
		var client, replicaSlave;
		
		state.socket.removeListener('close', state.onSocketClose);
		this.unidentifiedClients.remove(state);
		logInput(state.buffer);
		
		/* Now identify client and add it to corresponding data structures. */
		
		if (reply.identity == 'replica-slave') {
			client = replicaSlave = new ReplicaSlave.ReplicaSlave(this,
				state.socket, state.input, state.output, state.id);
			this.replicaSlaves[state.id] = replicaSlave;
			this.replicaSlaveCount++;
			this._logClientConnectionActivity(replicaSlave, "Identified as replica slave");
			
			state.input.onData = function(data) {
				return replicaSlave.onData(data);
			}
			state.socket.on('close', function() {
				replicaSlave.state = ReplicaSlave.DISCONNECTED;
				self._logClientConnectionActivity(replicaSlave, "Connection closed");
				delete self.replicaSlaves[replicaSlave.id];
				self.replicaSlaveCount--;
				console.assert(self.replicaSlaveCount >= 0);
			});
			
			replicaSlave.initialize();
			
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

exports._logClientConnectionActivity = function(client, message) {
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
		log.info.apply(log, args);
	}
}
