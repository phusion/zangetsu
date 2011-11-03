/**
 * Provides buffered data events for sockets.
 *
 * == Synopsis
 *
 *   wrapper = new SocketInputWrapper(socket, function(data) {
 *      var numberOfBytesConsumed = doSomethingWith(data);
 *      return numberOfBytesConsumed;
 *   });
 *   
 *   wrapper.pause();
 *   
 *   wrapper.resume();
 *
 * == Description
 *
 * A socket only emits its 'data'
 * event once. If the application does not consume the entire buffer immediately
 * then it is responsible for storing the remaining unconsumed part of the buffer
 * and for pausing and resuming the socket accordingly. SocketInputWrapper
 * simplifies all that by doing all that for you.
 *
 * Wrap a SocketInputWrapper around a socket and provide a data handler callback.
 * The handler is called every time there is socket data. The handler must return
 * the number of bytes that it has actually consumed. If not everything has been
 * consumed, then the handler will be called with the remaining data in the next
 * tick.
 *
 * Since SocketInputWrapper handles pausing and resuming the socket for you, you
 * must not directly call pause() and resume() on the underlying socket. Instead,
 * use the pause() and resume() functions on SocketInputWrapper. These functions
 * work as expected: if you haven't consumed all data but you call pause() then
 * SocketInputWrapper won't call the handler again until you call resume(), which
 * causes the handler to be called in the next tick (not immediately).
 */

function SocketInputWrapper(socket, onData, onEnd, onError) {
	var self        = this;
	this.socket     = socket;
	this.onData     = onData;
	this.onEnd      = onEnd;
	this.onError    = onError;
	this.unconsumed = undefined;
	this.paused     = false;
	this.eof        = false;
	this.error      = undefined;
	this.resumeAfterProcessingEvents = false;
	this.nextTickInstalled = false;
	
	socket.on('data', function(data) {
		console.assert(!self.nextTickInstalled);
		console.assert(self.unconsumed === undefined);
		self.unconsumed = data;
		self._processEvents();
	});
	socket.on('end', function() {
		self.eof = true;
		if ((!self.unconsumed || self.unconsumed.length == 0) && !self.nextTickInstalled) {
			self.onEnd();
		}
	});
	socket.on('error', function(err) {
		self.error = err;
		if ((!self.unconsumed || self.unconsumed.length == 0) && !self.nextTickInstalled) {
			self.onError(err);
		}
	});
}

SocketInputWrapper.prototype.pause = function() {
	this.paused = true;
	this.resumeAfterProcessingEvents = false;
	this.socket.pause();
}

SocketInputWrapper.prototype.resume = function() {
	if (this.unconsumed) {
		this.resumeAfterProcessingEvents = true;
		this._processEventsInNextTick();
	} else {
		this.paused = false;
		this.socket.resume();
	}
}

SocketInputWrapper.prototype._processEventsInNextTick = function() {
	if (!this.nextTickInstalled) {
		var self = this;
		this.nextTickInstalled = true;
		process.nextTick(function() {
			self.nextTickInstalled = false;
			self._processEvents();
		});
	}
}

SocketInputWrapper.prototype._processEvents = function() {
	if ((this.paused && !this.resumeAfterProcessingEvents)
	 || (this.socket.destroyed)) {
		return;
	}
	
	if (this.unconsumed) {
		console.assert(this.unconsumed.length > 0);
		var consumed = this.onData(this.unconsumed);
		if (typeof(consumed) != 'number') {
			throw Error('The onData callback must return the number of bytes consumed.');
		}
		if (consumed == this.unconsumed.length) {
			this.unconsumed = undefined;
			if (this.eof) {
				this.onEnd();
			} else if (this.error) {
				this.onError(this.error);
			} else if (this.resumeAfterProcessingEvents) {
				this.paused = false;
				this.resumeAfterProcessingEvents = false;
				this.socket.resume();
			}
		} else {
			this.unconsumed = this.unconsumed.slice(consumed);
			if (!this.paused) {
				this.paused = true;
				this.resumeAfterProcessingEvents = true;
				this.socket.pause();
				this._processEventsInNextTick();
			} else if (this.resumeAfterProcessingEvents) {
				this._processEventsInNextTick();
			}
		}
	} else if (this.eof) {
		this.onEnd();
	} else if (this.error) {
		this.onError(this.error);
	}
}

exports.SocketInputWrapper = SocketInputWrapper;
