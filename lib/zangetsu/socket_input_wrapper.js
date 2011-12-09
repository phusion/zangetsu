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
 * A socket only emits its 'data' event once. If the application does not consume
 * the entire buffer immediately then it is responsible for storing the remaining
 * unconsumed part of the buffer and for pausing and resuming the socket
 * accordingly. SocketInputWrapper simplifies all that by doing all that for you.
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

/**
 * Possible states for both the wrapper and the underlying socket.
 * They both have the same state.
 */
const
	/** The socket is open and is functiong normally. It may or may not
	 * be paused and the buffer may or may not contain data.
	 */
	LIVE       = 1,
	/** End-of-stream has been reached. In this state we might
	 * still have data in 'buffer', so the 'end' and 'close' events
	 * shouldn't be propagated until the buffer has been fully
	 * consumed.
	 */
    EOF        = 2,
    /** A read error has been encountered. In this state we might
     * still have data in 'buffer', so the 'error' and 'close' events
     * shouldn't be propagated until the buffer has been fully
     * consumed.
     */
    READ_ERROR = 3,
    /** Socket has been manually closed (by called destroy()). */
    CLOSED     = 4;

function SocketInputWrapper(socket, onData, onEnd, onError, onClose) {
	var self        = this;
	this.socket     = socket;

	this.onData     = onData;
	this.onEnd      = onEnd;
	this.onError    = onError;
	this.onClose    = onClose;

	/**
	 * The state for the wrapper and the underlying socket
	 * are always the same.
	 */
	this.state = LIVE;
	/**
	 * Whether this wrapper itself is paused.
	 * Invariant:
	 *    if paused:
	 *       socketPaused
	 * Equivalently:
	 *    if !socketPaused:
	 *       !paused
	 */
	this.paused = false;
	/**
	 * Whether the underlying socket is paused.
	 */
	this.socketPaused = false;
	/**
	 * A buffer containing data which is yet to be consumed.
	 * Invariant:
	 *    if buffer != undefined:
	 *       state == LIVE
	 *       buffer.length > 0
	 *       socketPaused
	 * Equivalently:
	 *    if !socketPaused || state != LIVE:
	 *       buffer == undefined
	 */
	this.buffer = undefined;
	/**
	 * If the underlying socket encountered a read error then the
	 * error code is stored here.
	 */
	this.error = undefined;
	this.nextTickInstalled = false;
	
	socket.on('data', function(data) {
		console.assert(self.state == LIVE);
		console.assert(!self.socketPaused);
		console.assert(self.buffer === undefined);
		console.assert(!self.paused);

		self.buffer = data;
		self._processBuffer();
	});

	socket.on('end', function() {
		console.assert(self.state == LIVE);
		console.assert(!self.socketPaused);
		console.assert(self.buffer === undefined);
		console.assert(!self.paused);
		
		self.state = EOF;
		if (self.onEnd) {
			self.onEnd();
		}
		if (self.onClose) {
			self.onClose();
		}
	});

	socket.on('error', function(err) {
		console.assert(self.state == LIVE);
		console.assert(!self.socketPaused);
		console.assert(self.buffer === undefined);
		console.assert(!self.paused);
		
		self.state = READ_ERROR;
		self.error = err;
		if (self.onError) {
			self.onError(err);
		}
		if (self.onClose) {
			self.onClose();
		}
	});
	
	socket.on('close', function() {
		if (self.state == EOF || self.state == READ_ERROR) {
			return;
		}
		console.assert(self.state == LIVE);

		self.state = CLOSED;
		if (self.onClose) {
			self.onClose();
		}
	});
}

SocketInputWrapper.prototype.pause = function() {
	if (this.state == LIVE && !this.paused) {
		this.paused = true;
		if (!this.socketPaused) {
			this.socketPaused = true;
			this.socket.pause();
		}
	}
}

SocketInputWrapper.prototype.resume = function() {
	if (this.state == LIVE && this.paused) {
		console.assert(this.socketPaused);
		
		this.paused = false;
		if (this.buffer) {
			this._processBufferInNextTick();
		} else {
			this.socketPaused = false;
			this.socket.resume();
		}
	}
}

SocketInputWrapper.prototype._processBufferInNextTick = function() {
	if (!this.nextTickInstalled) {
		var self = this;
		this.nextTickInstalled = true;
		process.nextTick(function() {
			self.nextTickInstalled = false;
			self._processBuffer();
		});
	}
}

SocketInputWrapper.prototype._processBuffer = function() {
	if (this.state == CLOSED) {
		return;
	}
	console.assert(this.state == LIVE);
	if (this.paused || !this.buffer || this.socket.destroyed) {
		return;
	}

	console.assert(this.buffer.length > 0);

	//console.log('SocketInputWrapper: before consumption: [' +
	//	this.buffer.toString('utf8') + ']');
	var consumed = this.onData(this.buffer);
	if (this.state == CLOSED) {
		return;
	}
	if (typeof(consumed) != 'number') {
		throw new Error('The onData callback must return the number of bytes consumed.');
	}
	if (consumed == this.buffer.length) {
		this.buffer = undefined;
		if (!this.paused && this.socketPaused) {
			this.socketPaused = false;
			this.socket.resume();
		}
	} else {
		this.buffer = this.buffer.slice(consumed);
		if (!this.socketPaused) {
			this.socketPaused = true;
			this.socket.pause();
		}
		if (!this.paused) {
			// Consume rest of the data in the next tick.
			this._processBufferInNextTick();
		}
	}
	//console.log('SocketInputWrapper: after consuming ' + consumed +
	//	' bytes: [' + (this.buffer || new Buffer(0)).toString('utf8') + ']');
}

exports.SocketInputWrapper = SocketInputWrapper;
