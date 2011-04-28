var path          = require('path');
var fs            = require('fs');
var child_process = require('child_process');
var TimeEntry     = require('./time_entry.js').TimeEntry;
var IOUtils       = require('./io_utils.js');

function Group(name, path) {
	/****** Public read-only ******/
	
	this.name = name;
	this.path = path;
	this.timeEntryCount = 0;
	this.timeEntries = {};
	
	/****** Private ******/
	
	/* Whether this Group is closed. */
	this.closed = false;
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

Group.prototype.remove = function(dayTimestamp, callback) {
	var self = this;
	var newFilename;
	
	if (dayTimestamp) {
		var dayTimestampsToRemove = [];
		var dirsToRemove = [];
		var dts, i, timeEntry;
		
		// We want to delete all time entries earlier than 'dayTimestamp'.
		for (dts in this.timeEntries) {
			if (parseInt(dts) < dayTimestamp) {
				dayTimestampsToRemove.push(dts);
			}
		}
		
		function deleteNextDir() {
			var dir = dirsToRemove.pop();
			if (dir) {
				var child = child_process.spawn('rm', ['-rf', dir]);
				child.stdin.end();
				child.stdout.pipe(process.stdout);
				child.stderr.pipe(process.stderr);
				child.on('exit', deleteNextDir);
			}
		}
		
		// Synchronously rename each time entry directory to a hidden name,
		// then delete them in the background.
		for (i = 0; i < dayTimestampsToRemove.length; i++) {
			dts = dayTimestampsToRemove[i];
			timeEntry = this.timeEntries[dts];
			try {
				newFilename = IOUtils.renameToHidden(timeEntry.path);
			} catch (err) {
				// If anything goes wrong we initiate removal of
				// those time entries that have already been
				// successfully renamed.
				deleteNextDir();
				callback(err);
				return;
			}
			timeEntry.close();
			this.timeEntryCount--;
			delete this.timeEntries[dts];
			dirsToRemove.push(newFilename);
		}
		
		deleteNextDir();
		callback();
		
	} else {
		try {
			newFilename = IOUtils.renameToHidden(this.path);
		} catch (err) {
			callback(err);
			return;
		}
		this.close();
		child_process.spawn('rm', ['-rf', newFilename]);
		callback();
	}
}

exports.Group = Group;
