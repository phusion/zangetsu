var path = require('path');
var fs   = require('fs');

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

var getType = exports.getType = function(value) {
	var typeName = typeof(value);
	if (typeName == 'object') {
		if (value) {
			if (value instanceof Array) {
				return 'array';
			} else {
				return typeName;
			}
		} else {
			return 'null';
		}
	} else {
		return typeName;
	}
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
