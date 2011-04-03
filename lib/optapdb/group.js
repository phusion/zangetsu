var path = require('path');
var fs   = require('fs');
var TimeEntry = require('./time_entry.js').TimeEntry;

function Group(name, path) {
	this.name = name;
	this.path = path;
	this.timeEntryCount = 0;
	this.timeEntries = {};
}

Group.prototype.findOrCreateTimeEntry = function(dayTimestamp, callback) {
	var self = this;
	var timeEntry = this.timeEntries[dayTimestamp];
	if (timeEntry) {
		callback(undefined, timeEntry);
	} else {
		var timePath = path.join(this.path, dayTimestamp);
		fs.mkdir(timePath, 0755, function(err) {
			if (err && err.code != 'EEXIST') {
				callback(err);
			} else if (!self.timeEntries[dayTimestamp]) {
				self.timeEntryCount++;
				timeEntry = new TimeEntry(dayTimestamp, timePath, 0);
				self.timeEntries[dayTimestamp] = timeEntry;
				callback(undefined, timeEntry);
			}
		});
	}
}

function validateGroupNameChar(ch) {
	return (ch >= 'a' && ch <= 'z')
		|| (ch >= 'A' && ch <= 'Z')
		|| (ch >= '0' && ch <= '9')
		|| ch == '_'
		|| ch == '-'
		|| ch == '.';
}

exports.validateGroupName = function(name) {
	if (name.length == 0 || name[0] == '.') {
		return false;
	}
	for (var i = 0; i < name.legth; i++) {
		if (!validateGroupNameChar(name[i])) {
			return false;
		}
	}
	return true;
}

exports.Group = Group;
