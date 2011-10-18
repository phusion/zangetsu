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

Shard.prototype.connect(callback) {
	var self = this;
	self.socket = net.createConnection(this.port, this.hostname);
	socket.on('close', function() {
		self.connected = false;
	}
}

Shard.prototype.getTOC = function(callback) {
}

// Performs the command and runs the callback with this shard as first parameter
// and the resulting json object as the second.
Shard.prototype.performRequest = function(command, callback, tries) {
	var self = this;
	if(tries == undefined) { tries = 3; }

	if(!this.connected && tries < 1) {
		this.connect(function() {
			self.performRequest(command, callback, tries - 1);
		});
	}

	var input_socket = new SocketInputWrapper(this.socket, function(data) {
		var result = JSON.parse(data);
		callback(self, result);
	});

	var output_socket = new SocketOutputWrapper(this.socket);
	output_socket.writeJSON(command);
}

exports.Shard = Shard;
