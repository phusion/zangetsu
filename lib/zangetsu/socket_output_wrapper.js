/**
 * Guarantees that socket write callbacks are always called
 * even in the event of errors.
 */

function SocketOutputWrapper(socket) {
	this.socket = socket;
	this.callbacks = {};
	this.lastCallbackId = 0;
	
	var self = this;
	socket.on('error', function(err) {
		self.error = err;
	});
	socket.on('close', function() {
		var id;
		var callbacks = self.callbacks;
		self.callbacks = {};
		process.nextTick(function() {
			for (id in callbacks) {
				callbacks[id](self.error);
			}
		});
	});
}

SocketOutputWrapper.prototype.connected = function() {
	return !this.socket.destroyed;
}

SocketOutputWrapper.prototype.write = function(buf, callback) {
	if (callback) {
		var self = this;
		var id = this.lastCallbackId;
		this.lastCallbackId++;
		this.callbacks[id] = callback;
		return this.socket.write(buf, function(err) {
			if (self.callbacks[id]) {
				delete self.callbacks[id];
				callback(err);
			}
		});
	} else {
		return this.socket.write(buf);
	}
}

SocketOutputWrapper.prototype.writeJSON = function(object, callback) {
	var data = JSON.stringify(object);
	data += "\n";
	this.write(data, callback);
}

exports.SocketOutputWrapper = SocketOutputWrapper;
