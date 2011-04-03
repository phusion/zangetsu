var sys  = require('sys');
var fs   = require('fs');
var path = require('path');
var net  = require('net');
var ReplicaMember = require('./replica_member.js');
var Client        = require('./client.js');
var Database      = require('./database.js').Database;
var Group         = require('./group.js');

function Server(dbpath) {
	var self = this;
	this.dbpath = dbpath;
	this.replicaMembers = {};
	this.replicaMemberCount = 0;
	this.lastReplicaMemberId = 0;
	this.clients = {};
	this.clientCount = 0;
	this.lastClientId = 0;
	this.database = new Database(dbpath);
}

Server.prototype.listen = function(port, callback) {
	var self = this;
	this.database.reload(function(err) {
		if (err) {
			callback(err);
		} else {
			self.server = net.createServer(function(socket) {
				self._onNewClientSocket(socket);
			});
			self.server.listen(port);
			callback();
		}
	});
}

/* New clients first go through a handshake process. If the handshake is
 * successful they will be identified as either a normal client or a replica
 * member client and added to the corresponding data structures.
 */
Server.prototype._onNewClientSocket = function(socket) {
	var self = this;
	var handshakeState = {
		socket: socket,
		buffer: ''
	};
	var handshakeMessage = {
		protocolMajor: 1,
		protocolMinor: 0,
		serverName: "MosaDB/1.0"
	};
	socket.setNoDelay();
	socket.setKeepAlive(true);
	socket.write(JSON.stringify(handshakeMessage) + "\n");
	socket.on('data', function(data) {
		self._onClientHandshake(handshakeState, data);
	});
	socket.on('end', function() {
		socket.destroy();
	})
}

Server.prototype._onClientHandshake = function(state, data) {
	var i, reply;
	var found = false;
	
	// Consume everything until newline.
	for (i = 0; i < data.length && !found; i++) {
		if (data[i] == 10) {
			found = true;
		}
	}
	state.buffer += data.toString('utf8', 0, i);
	
	if (found) {
		/* Handshake complete: we got a complete line,
		 * which should contain JSON. Parse and process it.
		 */
		try {
			reply = parseJsonObject(state.buffer);
		} catch (e) {
			disconnectWithError(state.socket,
				'Command cannot be parsed: ' + e.message);
			return;
		}
		
		var self = this;
		var remainingData = data.slice(i);
		state.socket.removeAllListeners('data');
		state.socket.removeAllListeners('end');
		
		/* Now identify client and add it to corresponding data structures. */
		
		if (reply.identity == 'replica-member') {
			var replicaMember = new ReplicaMember.ReplicaMember(state.socket,
				this.lastReplicaMemberId);
			this.replicaMembers[this.lastReplicaMemberId] = replicaMember;
			this.replicaMemberCount++;
			this.lastReplicaMemberId++;
			
			reply = { result: 'ok' };
			writeMessage(state.socket, reply);
			state.socket.on('data', function(data) {
				self._onReplicaMemberData(replicaMember, data);
			});
			state.socket.on('close', function() {
				delete self.replicaMembers[replicaMember.id];
				self.replicaMemberCount--;
			});
			if (remainingData.length > 0) {
				this._onReplicaMemberData(replicaMember, remainingData);
			}
		} else {
			var client = new Client.Client(state.socket,
				this.lastClientId);
			this.clients[this.lastClientId] = client;
			this.clientCount++;
			this.lastClientId++;
			
			reply = { result: 'ok' };
			writeMessage(state.socket, reply);
			state.socket.on('data', function(data) {
				self._onClientData(client, data);
			});
			state.socket.on('close', function() {
				delete self.clients[client.id];
				self.clientCount--;
			});
			if (remainingData.length > 0) {
				this._onClientData(client, remainingData);
			}
		}
	}
}

Server.prototype._onReplicaMemberData = function(replicaMember, data) {
	sys.print("new replica member\n");
}

Server.prototype._onClientData = function(client, data) {
	var pos = 0, found = false;
	var i, command, result;
	
	while (pos < data.length && client.connected()) {
		switch (client.state) {
		case Client.RECEIVING_COMMAND:
			for (i = pos; i < data.length && !found; i++) {
				if (data[i] == 10) {
					found = true;
				}
			}
			client.commandBuffer += data.toString('utf8', pos, i);
			pos = i;
			if (found) {
				try {
					command = parseJsonObject(client.commandBuffer);
				} catch (e) {
					disconnectClientWithError(client,
						"Cannot parse command: " + e.message);
					return;
				}
				
				if (getType(command.command) != 'string') {
					disconnectClientWithError(client,
						"Command must have a 'command' string field");
					return;
				}
				
				client.commandBuffer = '';
				this._onClientCommand(client, command);
			}
			break;
			
		case Client.RECEIVING_ADD_DATA:
			this._onClientAddData(client, data.slice(pos));
			/* We stop processing here. _onClientAddData() will take
			 * care of changing state as well as processing the
			 * remaining socket data.
			 */
			return;
		
		default:
			throw 'BUG: unknown client state ' + client.state;
		}
	}
}

Server.prototype._onClientCommand = function(client, command) {
	if (command.command == 'add') {
		if (getType(command.group) != 'string') {
			disconnectClientWithError(client, "Expecting a 'group' string field");
			return;
		} else if (!Group.validateGroupName(command.group)) {
			disconnectClientWithError(client, "Invalid group name");
			return;
		} else if (getType(command.timestamp) != 'number') {
			disconnectClientWithError(client, "Expecting a 'timestamp' number field");
			return;
		} else if (getType(command.size) != 'number') {
			disconnectClientWithError(client, "Expecting a 'size' number field");
			return;
		} else if (command.size > 1024 * 1024) {
			disconnectClientWithError(client, "Size may not be larger than 1 MB");
			return;
		}
		client.currentCommand = command;
		client.currentCommand.remaining = command.size;
		client.state = Client.RECEIVING_ADD_DATA;
	
	} else if (command.command == 'get') {
		if (getType(command.group) != 'string') {
			disconnectClientWithError(client, "Expecting a 'group' string field");
			return;
		} else if (getType(command.timestamp) != 'number') {
			disconnectClientWithError(client, "Expecting a 'timestamp' number field");
			return;
		} else if (getType(command.key) != 'string') {
			disconnectClientWithError(client, "Expecting a 'key' string field");
			return;
		}
		
	} else if (command.command == 'delete') {
		
		
	} else if (command.command == 'ping') {
		writeMessage(client.socket, { result: 'ok' });
		
	} else {
		disconnectClientWithError(client, "Unknown command");
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
	client.addChecksum.update(slice);
	
	if (client.currentCommand.remaining < 0) {
		throw 'bug';
	} else if (done) {
		function added(err, offset) {
			if (!client.connected()) {
				return;
			} else if (err) {
				disconnectClientWithError(client, "I/O error: " + err.message);
				return;
			}
			
			writeMessage(client.socket, { status: 'ok', offset: offset });
			client.socket.resume();
			client.state = Client.RECEIVING_COMMAND;
			
			/* There was remaining socket data that isn't
			 * part of the 'add' command body, so process
			 * that now.
			 */
			if (consumed != data.length) {
				self._onClientData(client, data.slice(consumed));
			}
		}
		
		client.addChecksum.finalize();
		this.database.add(client.currentCommand.group,
			client.currentCommand.timestamp,
			client.addBuffers,
			client.addChecksum.toBuffer(),
			added);
		client.addBuffers = [];
		client.addChecksum.reset();
		/* Data hasn't been written to disk yet so stop
		 * accepting client data until we're done.
		 */
		client.socket.pause();
	}
	/* else: we've consumed all data */
}


function writeMessage(socket, object, callback) {
	var data = JSON.stringify(object);
	data += "\n";
	socket.write(data, callback);
}

function disconnectClientWithError(client, message) {
	disconnectWithError(client.socket, message);
	client.state = Client.DISCONNECTED;
}

function disconnectWithError(socket, message) {
	var reply = {
		result: 'error',
		message: message
	};
	writeMessage(socket, reply, function() {
		socket.destroy();
	});
	socket.setTimeout(20000, function() {
		socket.destroy();
	});
}

function min(a, b) {
	if (a < b) {
		return a;
	} else {
		return b;
	}
}

function parseJsonObject(str) {
	var object = JSON.parse(str);
	if (getType(object) != 'object') {
		throw new SyntaxError("Expected an object value, but got a " + getType(object));
	}
	return object;
}

function getType(value) {
	var typeName = typeof(value);
	if (typeName == 'object') {
		if (value) {
			if (value instanceof Array) {
				return 'array';
			} else {
				return typeName;
			}
		} else {
			return 'null';
		}
	} else {
		return typeName;
	}
}


exports.Server = Server;
new Server("db").listen(3000, function(err) {
	if (err) {
		sys.print(err, "\n");
	}
});
