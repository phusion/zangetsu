/**
 * Represents another router from the point of view of a shard router.
 */

var util   = require('util');
var events = require('events');
var http = require('http');
var net = require('net');

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

function ShardRouterConnection(configuration) {
	events.EventEmitter.call(this);
	this.hostname = configuration.hostname;
	this.port = configuration.port;
	this.identifier = this.hostname + ':' + this.port;
	this.state    = NOT_CONNECTED;
	this.queuedRequests = [];
}

//TODO must we also send which shardserver we are?
// Supported commands
// listLocks -- lists all locks that router thinks it owns
// giveLock  -- requests the router to cede a lock
// releaseLock -- tells the router the lock can be released

ShardConnection.prototype.listLocks = function(callback) {
	var command = {
		command   : 'listLocks'
	}
	this.performRequest(command, callback);
}

ShardConnection.prototype.lock = function(group, dayTimestamp, callback) {
	var command = {
		command   : 'giveLock',
		group     : group,
		dayTimestamp : dayTimestamp
	}
	this.performRequest(command, callback);
}

ShardConnection.prototype.releaseLock = function(callback) {
	var command = {
		command : 'releaseLock',
		group : group,
		dayTimestamp : dayTimestamp,
		result : result
	}
	this.performRequest(command, callback);
}

util.inherits(ShardRouterConnection, events.EventEmitter);

ShardRouterConnection.prototype.performRequest = function(command, callback) {
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

ShardRouterConnection.prototype._connect = function() {
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
				log.notice("Connected to shard-router %s:%d",
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
		log.warning("Cannot connect to shard-router %s:%d: %s; retrying in 5 seconds",
			self.hostname, self.port, err.message);
		setTimeout(function() {
			self.state = NOT_CONNECTED;
			self._connect();
		}, 5000);
	}

	function onConnectTimeout() {
		self.socket.removeAllListeners();
		delete self.socket;
		log.warning("Cannot connect to shard-router %s:%d: connection timeout; retrying...",
			self.hostname, self.port);
		self.state = NOT_CONNECTED;
		self._connect();
	}
}

ShardRouterConnection.prototype._disconnect = function() {
	this.state = NOT_CONNECTED;
	this.socket.destroySoon();
	delete this.socket;
	delete this.input;
	delete this.output;
}

ShardRouterConnection.prototype._processNextRequest = function() {
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
				message: 'Could not write to the shard router: ' + err.message
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
							message: 'Could not read from the shard router: ' + err.message
						});
					} else {
						done({
							status : 'error',
							message: 'The shard router sent invalid JSON: ' + err.message
						});
					}
				} else if (message) {
					receivedStatusReply(message);
				} else {
					self._disconnect();
					done({
						status : 'error',
						message: 'The shard router unexpectedly closed the connection'
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
						message: 'Could not read from the shard router: ' + err.message
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
						message: 'The shard router unexpectedly closed the connection'
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

ShardRouterConnection.prototype._readJsonObject = function(callback) {
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

exports.ShardRouterConnection = ShardRouterConnection;
