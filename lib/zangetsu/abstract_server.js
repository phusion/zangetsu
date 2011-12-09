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

var net  = require('net');
var SocketInputWrapper  = require('./socket_input_wrapper.js');
var SocketOutputWrapper = require('./socket_output_wrapper.js');
var Constants       = require('./constants.js');
var CommandHandler  = require('./command_handler.js');
var ReplicaSlave    = require('./replica_slave.js');
var Client          = require('./client.js');
var Database        = require('./database.js');
var Utils           = require('./utils.js');
var IOUtils         = require('./io_utils.js');
var log             = require('./default_log.js').log;

var readJsonObject      = IOUtils.readJsonObject;
var writeMessage        = IOUtils.writeMessage;
var disconnectWithError = IOUtils.disconnectWithError;

exports.close = function() {
	var self = this;
	this.state = Constants.SS_CLOSED;
	self.disconnectAllClients(function() {
		self.server.close();
		self.database.close();
	});
}

exports.disconnectAllClients = function(callback) {
	var unidentifiedClients = this.unidentifiedClients.values();
	var i, clientId, client;
	var operations = 1;

	function decOperations() {
		console.assert(operations > 0);
		operations--;
		if (operations == 0 && callback) {
			callback();
		}
	}
	
	for (i = 0; i < unidentifiedClients.length; i++) {
		operations++;
		client = unidentifiedClients[i];
		client.requestClose(decOperations);
	}
	
	for (clientId in this.clients) {
		operations++;
		client = this.clients[clientId];
		client.requestClose(decOperations);
	}
	
	for (clientId in this.replicaSlaves) {
		operations++;
		client = this.replicaSlaves[clientId];
		client.requestClose(decOperations);
	}

	decOperations();
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
		id    : this.lastClientId
	};
	var handshakeMessage = {
		protocolMajor: Constants.PROTOCOL_MAJOR,
		protocolMinor: Constants.PROTOCOL_MINOR,
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
	handshakeState.requestClose = function(callback) {
		handshakeState.socket.removeListener('close', handshakeState.onSocketClose);
		self.unidentifiedClients.remove(handshakeState);
		socket.destroy();
		if (callback) {
			process.nextTick(callback);
		}
	}
	handshakeState.onSocketClose = function() {
		log.info("[Client %d] Connection closed before handshake done", handshakeState.id);
		self.unidentifiedClients.remove(handshakeState);
	}
	
	socket.on('close', handshakeState.onSocketClose);
	this.unidentifiedClients.add(handshakeState);

	readJsonObject(handshakeState.input, function(err, object) {
		if (object) {
			self._handleHandshakeReply(handshakeState, object);
		}
	});
}

exports._handleHandshakeReply = function(state, reply) {
	var self = this;
	var client, replicaSlave;
	
	state.socket.removeListener('close', state.onSocketClose);
	delete state.onSocketClose;
	this.unidentifiedClients.remove(state);
	
	/* Now identify client and add it to corresponding data structures. */
	
	if (reply.identity == 'replica-slave') {
		client = replicaSlave = new ReplicaSlave.ReplicaSlave(this,
			state.socket, state.input, state.output, state.id);
		replicaSlave.resultCheckThreshold = this.resultCheckThreshold;
		this.replicaSlaves[state.id] = replicaSlave;
		this.replicaSlaveCount++;
		this._logClientConnectionActivity(replicaSlave,
			"Identified as replica slave: %j",
			reply);
		
		state.input.onClose = function() {
			replicaSlave.state = ReplicaSlave.DISCONNECTED;
			self._logClientConnectionActivity(replicaSlave, "Connection closed");
			delete self.replicaSlaves[replicaSlave.id];
			self.replicaSlaveCount--;
			console.assert(self.replicaSlaveCount >= 0);
		}
		
	} else {
		client = new Client.Client(this, state.socket, state.input,
			state.output, state.id);
		this.clients[state.id] = client;
		this.clientCount++;
		this._logClientConnectionActivity(client,
			"Identified as regular client: %j",
			reply);
		
		state.input.onClose = function() {
			client.state = Client.DISCONNECTED;
			self._logClientConnectionActivity(client, "Connection closed");
			delete self.clients[client.id];
			self.clientCount--;
			console.assert(self.clientCount >= 0);
		}
		
		reply = { status: 'ok' };
		writeMessage(state.socket, reply);
	}
	
	state.input.onError = function(err) {
		if (err.code != 'ECONNRESET' && err.code != 'EPIPE') {
			self._logClientConnectionActivity(client,
				"Socket error: %s", err);
		}
		// Socket is automatically closed after error.
	}

	client.initialize();
}

exports._startAndReloadDatabase = function() {
	this.database.start();
	if (!this.database.clean) {
		log.emergency("Database directory contains a stale lock file (%s); " +
			"this probably indicates that the server was not cleanly shut down. " +
			"Recommendation: remove the lock file and quickly check the database " +
			"directory for errors by running " +
			"'zangetsu --dbpath=%s --quick-repair'.",
			this.database.getLockFileName(),
			this.dbpath);
		process.exit(1);
	}
	this.database.reload();
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
