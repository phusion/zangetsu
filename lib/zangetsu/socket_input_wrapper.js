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

function SocketInputWrapper(socket, onData) {
	var self        = this;
	this.socket     = socket;
	this.onData     = onData;
	this.unconsumed = undefined;
	this.paused     = false;
	this.resumeAfterProcessingUnconsumed = false;
	
	socket.on('data', function(data) {
		console.assert(self.unconsumed === undefined);
		self.unconsumed = data;
		self._processUnconsumed();
	});
}

SocketInputWrapper.prototype.pause = function() {
	this.paused = true;
	this.resumeAfterProcessingUnconsumed = false;
	this.socket.pause();
}

SocketInputWrapper.prototype.resume = function() {
	if (this.unconsumed) {
		this.resumeAfterProcessingUnconsumed = true;
		this._processUnconsumedInNextTick();
	} else {
		this.paused = false;
		this.socket.resume();
	}
}

SocketInputWrapper.prototype._processUnconsumedInNextTick = function() {
	if (!this.nextTickInstalled) {
		var self = this;
		this.nextTickInstalled = true;
		process.nextTick(function() {
			self.nextTickInstalled = false;
			self._processUnconsumed();
		});
	}
}

SocketInputWrapper.prototype._processUnconsumed = function() {
	if (this.unconsumed === undefined || (this.paused && !this.resumeAfterProcessingUnconsumed)) {
		return;
	}
	console.assert(this.unconsumed.length > 0);
	
	var self = this;
	var consumed = this.onData(this.unconsumed);
	if (typeof(consumed) != 'number') {
		throw Error('The onData callback must return the number of bytes consumed.');
	}
	if (consumed == this.unconsumed.length) {
		this.unconsumed = undefined;
		if (this.resumeAfterProcessingUnconsumed) {
			this.paused = false;
			this.resumeAfterProcessingUnconsumed = false;
			this.socket.resume();
		}
	} else {
		this.unconsumed = this.unconsumed.slice(consumed);
		if (!this.paused) {
			this.paused = true;
			this.resumeAfterProcessingUnconsumed = true;
			this.socket.pause();
			this._processUnconsumedInNextTick();
		} else if (this.resumeAfterProcessingUnconsumed) {
			this._processUnconsumedInNextTick();
		}
	}
}

exports.SocketInputWrapper = SocketInputWrapper;
