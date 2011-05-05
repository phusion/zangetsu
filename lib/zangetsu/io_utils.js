var path  = require('path');
var fs    = require('fs');
var Utils = require('./utils.js');
var getType = Utils.getType;

exports.logInput = function(data) {
	//console.log("<<", data);
}

exports.logOutput = function(data) {
	//console.log(">>", data);
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
	slice = data.toString('utf8', start, i - start);
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