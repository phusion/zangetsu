var path = require('path');
var fs   = require('fs');
var TimeEntry = require('./time_entry.js').TimeEntry;

function Group(name, path) {
	/*** Public read-only ***/
	this.name = name;
	this.path = path;
	this.timeEntryCount = 0;
	this.timeEntries = {};
	
	/*** Private ***/
	
	/* Whether this Group is closed. */
	this.closed = false;
}

Group.prototype.close = function(callback) {
	var counter = this.timeEntryCount;
	var timeEntries = this.timeEntries;
	this.timeEntryCount = 0;
	this.timeEntries = {};
	this.closed = true;
	
	for (dayTimestamp in timeEntries) {
		timeEntries[dayTimestamp].close(function() {
			counter--;
			if (counter == 0 && callback) {
				callback();
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
