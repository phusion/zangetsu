exports.max = function(a, b) {
	if (a > b) {
		return a;
	} else {
		return b;
	}
}

exports.min = function(a, b) {
	if (a < b) {
		return a;
	} else {
		return b;
	}
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
