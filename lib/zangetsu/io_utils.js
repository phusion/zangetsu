var path  = require('path');
var fs    = require('fs');
var Utils = require('./utils.js');
var getType = Utils.getType;

exports.logInput = function(data) {
	//console.log("<<", JSON.stringify(data));
}

exports.logOutput = function(data) {
	//console.log(">>", JSON.stringify(data));
}

/**
 * Reads arbitrary binary data from the given SocketInputWrapper.
 * 'size' specifies the number of bytes one wants to read.
 *
 * The callback should accept three parameters: (error, data, done)
 *
 * This function will call the callback with a buffer that is up
 * to 'size' bytes large. It will keep calling the callback until
 * 'size' bytes have been read in total, or until a premature EOF
 * occurs, or until an error occurs. The 'done' parameter indicates
 * whether this is the last time the callback is called.
 *
 * For example, suppose that you call:
 *
 *   readData(100)
 *
 * This may result in the callback being called as follows:
 *
 *   callback(undefined, buffer with 50 bytes, false)
 *   callback(undefined, buffer with 10 bytes, false)
 *   callback(undefined, buffer with 40 bytes, true)
 *
 * If a premature EOF occurs then the callback is called with
 * (undefined, undefined, true). The SocketInputWrapper's onEnd
 * callback is also called, if any.
 *
 * If an error occurs then the callback is called with just an
 * error object. The SocketInputWrapper's onError callback is
 * also called, if any.
 */
exports.readData = function(input, size, callback) {
	var alreadyRead = 0;
	var oldOnData  = input.onData;
	var oldOnEnd   = input.onEnd;
	var oldOnError = input.onError;
	
	function resetCallbacks() {
		input.onData  = oldOnData;
		input.onEnd   = oldOnEnd;
		input.onError = oldOnError;
	}

	input.onData = function(data) {
		var consumed = Math.min(size - alreadyRead, data.length);
		alreadyRead += consumed;
		if (alreadyRead == size) {
			resetCallbacks();
			input.pause();
		}
		if (data.length > consumed) {
			// Only slice (and create an extra object on the heap)
			// when necessary.
			data = data.slice(0, consumed);
		}
		callback(undefined, data, alreadyRead == size);
		return consumed;
	}

	input.onEnd = function() {
		resetCallbacks();
		callback(undefined, undefined, true);
		if (oldOnEnd) {
			oldOnEnd();
		}
	}

	input.onError = function(err) {
		resetCallbacks();
		callback(err);
		if (oldOnError) {
			oldOnError();
		}
	}

	input.resume();
}

/**
 * Reads a line of JSON data from the given SocketInputWrapper,
 * parses it into a JSON object and calls the given callback with
 * the object.
 *
 * The callback must accept two parameters: (error, object)
 *
 * If a premature EOF occurred then the callback is called with
 * no error and no object, after which the SocketInputWrapper's
 * onEnd callback is called (if any).
 *
 * If an I/O read error occurred then the callback is called with
 * just the error, after which the SocketInputWrapper's onError
 * callback is called (if any). The error object will have the
 * attribute 'isIoError' set to true.
 *
 * If a non-I/O error occurred (e.g. the received JSON data is
 * malformed) then the callback is called with an error, but the
 * SocketInputWrapper's onError callback is NOT called. The error
 * object will not have an 'isIoError' attribute.
 *
 * If the socket is destroyed before the read operation finalizes
 * then the callback is never called.
 */
exports.readJsonObject = function(input, callback) {
	var buffer     = '';
	var oldOnData  = input.onData;
	var oldOnEnd   = input.onEnd;
	var oldOnError = input.onError;
	
	function resetCallbacks() {
		input.onData  = oldOnData;
		input.onEnd   = oldOnEnd;
		input.onError = oldOnError;
	}

	input.onData = function(data) {
		var consumed, object, error;
		var found = false;

		// Consume everything until newline.
		for (consumed = 0; consumed < data.length && !found; consumed++) {
			if (data[consumed] == 10) {
				found = true;
			}
		}
		buffer += data.toString('utf8', 0, consumed);
		if (found) {
			try {
				object = parseJsonObject(buffer);
			} catch (e) {
				error = e;
			}
			resetCallbacks();
			input.pause();
			callback(error, object);
			return consumed;
		}
	}

	input.onEnd = function() {
		resetCallbacks();
		callback();
		if (oldOnEnd) {
			oldOnEnd();
		}
	}
	
	input.onError = function(err) {
		err.isIoError = true;
		resetCallbacks();
		callback(err);
		if (oldOnError) {
			oldOnError();
		}
	}

	input.resume();
}

var writeMessage = exports.writeMessage = function(socket, object, callback) {
	var data = JSON.stringify(object);
	data += "\n";
	socket.write(data, callback);
	exports.logOutput(data);
}

exports.disconnectWithError = function(socket, message) {
	var reply = {
		status: 'error',
		message: message,
		disconnect: true
	};
	writeMessage(socket, reply, function() {
		socket.destroy();
	});
	socket.setTimeout(20000, function() {
		socket.destroy();
	});
}

var parseJsonObject = exports.parseJsonObject = function(str) {
	var object = JSON.parse(str);
	if (getType(object) != 'object') {
		throw new SyntaxError("Expected an object value, but got a " +
			getType(object));
	}
	return object;
}

var parseJsonObjectFromStream = exports.parseJsonObjectFromStream = function(buffer, data, start) {
	var found = false;
	var i, slice;
	
	for (i = start; i < data.length && !found; i++) {
		if (data[i] == 10) {
			found = true;
		}
	}
	slice = data.toString('utf8', start, i);
	if (found) {
		buffer += slice;
		return {
			done: true,
			pos: i,
			object: parseJsonObject(buffer)
		}
	} else {
		return {
			done: false,
			pos: i,
			slice: slice
		}
	}
}

exports.renameToHidden = function(filename, callback) {
	var dirname  = path.dirname(filename);
	var basename = path.basename(filename);
	var newFilename;
	
	for (i = 0; i < 1000; i++) {
		newFilename = path.join(dirname, "._" + basename + '-' + i);
		try {
			fs.renameSync(filename, newFilename);
			return newFilename;
		} catch (err) {
			if (err.code != 'ENOTEMPTY') {
				throw err;
			}
		}
		return
	}
	throw Error("Cannot find a suitable filename to rename to");
}

/** Converts a 32-bit unsigned integer into a 32-bit binary buffer, big endian encoding. */
exports.uintToBuffer = function(i, buf, start) {
	if (!buf) {
		buf = new Buffer(4);
	}
	if (!start) {
		start = 0;
	}
	buf[start + 0] = (i & 0xff000000) >> 24;
	buf[start + 1] = (i & 0xff0000) >> 16;
	buf[start + 2] = (i & 0xff00) >> 8;
	buf[start + 3] = (i & 0xff);
	return buf;
}

/** Converts a 32-bit binary buffer, big endian encoding, into a 32-bit unsigned integer. **/
exports.bufferToUint = function(buf, start) {
	if (!start) {
		start = 0;
	}
	return buf[start + 0] << 24 |
		buf[start + 1] << 16 |
		buf[start + 2] << 8 |
		buf[start + 3];
}
