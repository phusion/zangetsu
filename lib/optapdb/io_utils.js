exports.logInput = function(data) {
	//console.log("<<", data);
}

exports.logOutput = function(data) {
	//console.log(">>", data);
}

exports.writeMessage = function(socket, object, callback) {
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
	exports.writeMessage(socket, reply, function() {
		socket.destroy();
	});
	socket.setTimeout(20000, function() {
		socket.destroy();
	});
}

exports.getType = function(value) {
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

exports.parseJsonObject = function(str) {
	var object = JSON.parse(str);
	if (exports.getType(object) != 'object') {
		throw new SyntaxError("Expected an object value, but got a " +
			exports.getType(object));
	}
	return object;
}

exports.parseJsonObjectFromStream = function(buffer, data, start) {
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
			object: exports.parseJsonObject(buffer)
		}
	} else {
		return {
			done: false,
			pos: i,
			slice: slice
		}
	}
}
