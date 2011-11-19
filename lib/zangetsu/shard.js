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


function Shard(configuration) {
	this.hostname = configuration.hostname;
	this.port = configuration.port;
	this.connected = false;
}

// Connect to the shard
Shard.prototype.connect = function(callback) {
	var self = this;
	self.socket = net.createConnection(this.port, this.hostname);
	socket.on('connect', callback);
	socket.on('close', function() {
		self.connected = false;
	});
	self.input_socket = new SocketInputWrapper(this.socket, function(data) {
		return self.onData(data);
	});
}

Shard.prototype.add = function(groupName, dayTimestamp, opid, size, buffers, callback) {
	var command = {
		command : 'add',
		group : groupName,
		timestamp : dayTimestamp,
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
	this.performRequest(command, callback);
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
	}

	// We set the onData hook to be the callback that performRequest
	self.onData = function(data) {
		callback(self, data);
		// Clean self.onData up
		self.onData = function() {};
	};

	var output_socket = new SocketOutputWrapper(this.socket);
	output_socket.writeJSON(command);
	for(buffer in buffers) {
		output_socket.write(buffer);
	}
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
