var util   = require('util');
var events = require('events');
var http = require('http');
var net = require('net');

var log                 = require('./default_log.js').log;
var TimeEntry           = require('./time_entry.js');
var Group               = require('./group.js');
var HashSet             = require('./hashset.js').HashSet;
var SocketOutputWrapper = require('./socket_output_wrapper.js').SocketOutputWrapper;
var SocketInputWrapper  = require('./socket_input_wrapper.js').SocketInputWrapper;
var ioutils             = require('./io_utils.js');

function Shard(configuration) {
	this.hostname = configuration.hostname;
	this.port = configuration.port;
	this.connected = false;
}

// Connect to the shard
Shard.prototype.connect = function(callback) {
	var self = this;
	self.socket = net.createConnection(this.port, this.hostname);
	self.onData = ioutils.readMessage(function(message) {
		self.shardInfo = message;
		var reply = {
			'identity' : 'shard-server'
		};
		self.output_socket.writeJSON(reply);
		self.onData = ioutils.readMessage(function(message) {
			self.connected = true;
			callback(message);
		});
	});

	self.socket.on('close', function() {
		self.connected = false;
	});

	self.input_socket = new SocketInputWrapper(this.socket, function(data) {
		return self.onData(data);
	});

	self.output_socket = new SocketOutputWrapper(this.socket);
}

Shard.prototype.add = function(groupName, timestamp, opid, size, buffers, callback) {
	var command = {
		command : 'add',
		group : groupName,
		timestamp : timestamp,
		opid : opid,
		size : size
	}

	this.performRequest(command, buffers, callback);
}

Shard.prototype.get = function(groupName, dayTimestamp, offset, callback) {
	var command = {
		command : 'get',
		group : groupName,
		timestamp : dayTimestamp,
		offset : offset
	}
	this.performRequest(command, [], callback);
}

// construct a getToc command, and then send it using performRequest
Shard.prototype.getTOC = function(callback) {
	var command = {'command' : 'getToc'};
	this.performRequest(command, [], callback);
}

// Gets the results of previous add operations
Shard.prototype.results = function(discard, callback) {
	var command = {'command' : 'results', 'discard': discard};
	this.performRequest(command, [], callback);
}

// Remove a group from the shard (with an optional timestamp)
Shard.prototype.remove = function(group, timestamp, callback) {
	var command = {'command' : 'remove', 'group': group};
	if (timestamp) {
		command.timestamp = timestamp;
	}
	this.performRequest(command, [], callback);
}

// Remove a day of a group from the shard
Shard.prototype.removeOne = function(group, dayTimestamp, callback) {
	var command = {'command' : 'removeOne',
	               'group': group,
	               'dayTimestamp' : dayTimestamp
	};
	this.performRequest(command, [], callback);
}

// Remove a day of a group from the shard
Shard.prototype.ping = function(callback) {
	var command = {'command' : 'ping'};
	this.performRequest(command, [], callback);
}

// Performs the command and runs the callback with this shard as first parameter
// and the resulting json object as the second.
// WARNING
// It is imperative that performRequest is not ran again before callback has fired.
// This means requests are required to be executed sequentially.
Shard.prototype.performRequest = function(command, buffers, callback, tries) {
	var self = this;
	// Try to connect 3 times
	if(tries == undefined) { tries = 3; }
	if(!this.connected && tries > 0) {
		this.connect(function() {
			self.performRequest(command, buffers, callback, tries - 1);
		});
		return;
	}

	self.output_socket.writeJSON(command);
	var write_state = {
		written : 0
	};
	var write_callback = function(err) {
		write_state.written += 1;
		if(write_state.written == buffers.length) {
			if(err) {
				callback({status: 'error', message: err.message, disconnect: true});
			}
			if(command.command == 'add') {
				callback({status: 'written', message: "data was written for command: " + command.opid});
			}
		}
	};

	for(buffer in buffers) {
		self.output_socket.write(buffers[buffer], write_callback);
	}

	var read_callback = function(message) {
		if(message.status == 'ok' && message.size) {
			self.onData = ioutils.readBinary(message.size, function(buffers) {
				callback(message, buffers);
			});
		} else {
			callback(message);
		}	
	}

	self.onData = ioutils.readMessage(read_callback);
}

// This shard is no longer in the shard config
// yet to decide what to do here, possibly we want to move data that is on
// this shard to other shards.
// Possibly afterwards we want to disconnect.
Shard.prototype.phaseOut = function() {
	// If we do, first recheck our toc by calling getToc
	// then move all data back to the cluster we're no longer a part of.
}

exports.Shard = Shard;
