var os = require('os');

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

exports.determinePublicHostName = function(bindAddress, givenPublicHostName) {
	if (!givenPublicHostName) {
		if (bindAddress == '0.0.0.0' || bindAddress == '::ffff:0.0.0.0') {
			return os.hostname();
		} else {
			return bindAddress;
		}
	} else {
		return givenPublicHostName;
	}
}
