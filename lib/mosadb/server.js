var sys  = require('sys');
var fs   = require('fs');
var path = require('path');
var net  = require('net');
var ReplicaMember = require('./replica_member.js').ReplicaMember;
var Client        = require('./client.js').Client;
var Toc           = require('./toc.js').Toc;

function Server(dbdir) {
	var self = this;
	this.dbdir = dbdir;
	this.replicaMembers = {};
	this.replicaMemberCount = 0;
	this.lastReplicaMemberId = 0;
	this.clients = {};
	this.clientCount = 0;
	this.lastClientId = 0;
	readToc(dbdir, function(err, toc) {
		self.toc = toc;
	});
}

Server.prototype.listen = function(port) {
	if (this.toc) {
		
	}
	this.server = net.createServer(function(socket) {
		self._onNewClientSocket(socket);
	});
}

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
		// We got a complete line, which should contain JSON.
		// Parse and process it.
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
		
		if (reply.identity == 'replica-member') {
			var replicaMember = new ReplicaMember(state.socket,
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
			var client = new Client(state.socket,
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
			
		case Client.RECEIVING_BODY:
			result = this._onClientCommandBody(client, data.slice(pos));
			pos += result.consumed;
			if (result.done && client.connected()) {
				client.state = Client.RECEIVING_COMMAND;
			}
			break;
		
		default:
			throw 'bug';
		}
	}
}

Server.prototype._onClientCommand = function(client, command) {
	if (command.command == 'add') {
		if (getType(command.group) != 'string') {
			disconnectClientWithError(client, "Expecting a 'group' string field");
			return;
		} else if (getType(command.timestamp) != 'number') {
			disconnectClientWithError(client, "Expecting a 'timestamp' number field");
			return;
		} else if (getType(command.size) != 'number') {
			disconnectClientWithError(client, "Expecting a 'size' number field");
			return;
		}
		client.currentCommand = command;
		client.currentCommand.remaining = command.size;
		client.state = Client.RECEIVING_BODY;
	
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

Server.prototype._onClientCommandBody = function(client, data) {
	var result = {
		consumed: 0,
		done: false
	};
	
	switch (client.currentCommand.command) {
	case 'add':
		result.consumed = min(client.currentCommand.remaining, data.length);
		client.currentCommand.remaining -= result.consumed;
		if (client.currentCommand < 0) {
			throw 'bug';
		}
		result.done = client.currentCommand == 0;
		
		determineFilename(client.currentCommand.group,
			client.currentCommand.timestamp);
		openFileWithCache("");
		
		return result;
		
	default:
		throw 'bug';
	}
}


function writeMessage(socket, object, callback) {
	socket.write(JSON.stringify(object) + "\n", callback);
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
//new Server().server.listen(3000);
var toc = new Toc("db")
toc.reload(function(err) {
	sys.print(err, "\n");
	sys.print(require('util').inspect(toc, true, 10), "\n");
});
