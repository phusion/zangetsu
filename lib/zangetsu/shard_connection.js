/**
 * Represents a shard from the point of view of a shard router.
 */

var util   = require('util');
var events = require('events');
var http   = require('http');
var net    = require('net');

var SocketInputWrapper  = require('./socket_input_wrapper.js').SocketInputWrapper;
var SocketOutputWrapper = require('./socket_output_wrapper.js').SocketOutputWrapper;
var log                 = require('./default_log.js').log;
var IOUtils             = require('./io_utils.js');

var readData       = IOUtils.readData;
var readJsonObject = IOUtils.readJsonObject;


const NOT_CONNECTED = 0,
      CONNECTING    = 1,
      IDLE          = 2,
      WORKING       = 3;


function ShardConnection(configuration) {
	events.EventEmitter.call(this);
	this.hostname = configuration.hostname;
	this.port     = configuration.port;
	this.state    = NOT_CONNECTED;
	this.identifier = configuration.hostname + ':' + configuration.port;
	this.queuedRequests = [];
}
util.inherits(ShardConnection, events.EventEmitter);

ShardConnection.prototype.add = function(groupName, timestamp, opid, size, buffers, callback) {
	var command = {
		command   : 'add',
		group     : groupName,
		timestamp : timestamp,
		opid      : opid,
		size      : size,
		buffers   : buffers
	}
	this.performRequest(command, callback);
}

ShardConnection.prototype.get = function(groupName, dayTimestamp, offset, callback) {
	var command = {
		command  : 'get',
		group    : groupName,
		timestamp: dayTimestamp * 60 * 60 * 24,
		offset   : offset
	}
	this.performRequest(command, callback);
}

// construct a getToc command, and then send it using performRequest
ShardConnection.prototype.getTOC = function(callback) {
	var command = { command: 'getToc' };
	this.performRequest(command, callback);
}

// Gets the results of previous add operations
ShardConnection.prototype.results = function(callback) {
	var command = { command: 'results'};
	this.performRequest(command, callback);
}

// Remove a group from the shard (with an optional timestamp)
ShardConnection.prototype.remove = function(group, timestamp, callback) {
	var command = { command: 'remove', group: group};
	if (timestamp) {
		command.timestamp = timestamp;
	}
	this.performRequest(command, callback);
}

// Remove a day of a group from the shard
ShardConnection.prototype.removeOne = function(group, dayTimestamp, callback) {
	var command = {
		command: 'removeOne',
	    group: group,
	    dayTimestamp: dayTimestamp
	};
	this.performRequest(command, callback);
}

// Remove a day of a group from the shard
ShardConnection.prototype.ping = function(callback) {
	var command = { command: 'ping' };
	this.performRequest(command, callback);
}

// Performs the command and runs the callback with this shard as first parameter
// and the resulting json object as the second.
// WARNING
// It is imperative that performRequest is not ran again before callback has fired.
// This means requests are required to be executed sequentially.
ShardConnection.prototype.performRequest = function(command, callback) {
	console.assert(!command.buffers || command.buffers.length > 0);

	this.queuedRequests.push([command, callback]);
	switch (this.state) {
	case NOT_CONNECTED:
		this._connect();
		break;
	case IDLE:
		this._processNextRequest();
		break;
	case CONNECTING:
	case WORKING:
		// Don't do anything. Queue item will eventually be processed.
		break;
	default:
		console.assert(false);
		break;
	}
}

ShardConnection.prototype._connect = function() {
	console.assert(this.state == NOT_CONNECTED);
	var self = this;
	var timerId;

	this.state  = CONNECTING;
	this.socket = net.connect(this.port, this.hostname, onConnectSuccess);
	this.socket.once('error', onConnectError);
	timerId = setTimeout(onConnectTimeout, 5000);

	// TODO: on connection error or timeout, timeout all queued requests that are too old

	function onConnectSuccess() {
		clearTimeout(timerId);
		self.socket.removeListener('error', onConnectError);
		self.input = new SocketInputWrapper(self.socket);
		self.output = new SocketOutputWrapper(self.socket);

		self._readJsonObject(function(message) {
			var reply = { identity: 'shard-router' };
			self.output.writeJSON(reply);
			self._readJsonObject(function(message) {
				log.notice("Connected to shard %s:%d",
					self.hostname, self.port);
				self.state = IDLE;
				if (self.queuedRequests.length > 0) {
					self._processNextRequest();
				}
			});
		});
	}

	function onConnectError(err) {
		clearTimeout(timerId);
		delete self.socket;
		log.warning("Cannot connect to shard %s:%d: %s; retrying in 5 seconds",
			self.hostname, self.port, err.message);
		setTimeout(function() {
			self.state = NOT_CONNECTED;
			self._connect();
		}, 5000);
	}

	function onConnectTimeout() {
		self.socket.removeAllListeners();
		delete self.socket;
		log.warning("Cannot connect to shard %s:%d: connection timeout; retrying...",
			self.hostname, self.port);
		self.state = NOT_CONNECTED;
		self._connect();
	}
}

ShardConnection.prototype._disconnect = function() {
	this.state = NOT_CONNECTED;
	this.socket.destroySoon();
	delete this.socket;
	delete this.input;
	delete this.output;
}

ShardConnection.prototype._processNextRequest = function() {
	console.assert(this.state == IDLE);
	console.assert(this.queuedRequests.length > 0);

	var self     = this;
	var request  = this.queuedRequests.shift();
	var command  = request[0];
	var callback = request[1];
	var buffers  = command.buffers;
	var i;

	delete command.buffers;
	this.state = WORKING;
	this.output.writeJSON(command, buffers ? undefined : written);
	if (buffers) {
		for (i = 0; i < buffers.length; i++) {
			this.output.write(buffers[i],
				(i == buffers.length - 1) ? written : undefined);
		}
	}

	function written(err) {
		if (err) {
			self._disconnect();
			done({
				status : 'error',
				message: 'Could not write to the shard server: ' + err.message
			});
		} else if (command.command == 'add') {
			// 'add' command does not usually send a status reply.
			done();
		} else {
			readJsonObject(self.input, function(err, message) {
				if (err) {
					self._disconnect();
					if (err.isIoError) {
						done({
							status : 'error',
							message: 'Could not read from the shard server: ' + err.message
						});
					} else {
						done({
							status : 'error',
							message: 'The shard server sent invalid JSON: ' + err.message
						});
					}
				} else if (message) {
					receivedStatusReply(message);
				} else {
					self._disconnect();
					done({
						status : 'error',
						message: 'The shard server unexpectedly closed the connection'
					});
				}
			});
		}
	}

	function receivedStatusReply(message) {
		if (message.status == 'ok' && command.command == 'get') {
			// 'get' command is followed by payload data.
			buffers = [];
			readData(self.input, message.size, function(err, buffer, isLast) {
				if (err) {
					self._disconnect();
					done({
						status : 'error',
						message: 'Could not read from the shard server: ' + err.message
					});
				} else if (buffer) {
					buffers.push(buffer);
					if (isLast) {
						done(message, buffers);
					}
				} else {
					self._disconnect();
					done({
						status : 'error',
						message: 'The shard server unexpectedly closed the connection'
					});
				}
			});
		} else {
			if (message.status == 'error' && message.disconnect) {
				self._disconnect();
			}
			done(message);
		}
	}

	function done() {
		callback.apply(null, arguments);
		if (self.state == WORKING) {
			self.state = IDLE;
			if (self.queuedRequests.length > 0) {
				self._processNextRequest();
			} else {
				self.emit('idle');
			}
		}
	}
}

ShardConnection.prototype._readJsonObject = function(callback) {
	var self = this;
	var handler = function(err, object) {
		if(err) {
			//TODO what to do on error?
			log.error('Connect Error');
		} else {
			callback(object);
		}
	}
	readJsonObject(self.input, handler);
}

// This shard is no longer in the shard config
// yet to decide what to do here, possibly we want to move data that is on
// this shard to other shards.
// Possibly afterwards we want to disconnect.
ShardConnection.prototype.phaseOut = function() {
	// If we do, first recheck our toc by calling getToc
	// then move all data back to the cluster we're no longer a part of.
}

exports.ShardConnection = ShardConnection;
