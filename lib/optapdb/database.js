var fs     = require('fs');
var path   = require('path');
var util   = require('util');
var events = require('events');
var TimeEntry     = require('./time_entry.js');
var Group         = require('./group.js');

function Database(dbpath) {
	events.EventEmitter.call(this);
	this.dbpath = dbpath;
	this.groupCount = 0;
	this.groups = {};
}
util.inherits(Database, events.EventEmitter);

Database.prototype.reload = function() {
	// TODO: clear stale renames upon reloading
	
	var groupDirs = fs.readdirSync(this.dbpath);
	var i, groupName, groupPath, stats, timeEntryDirs, group,
		j, timePath, dayTimestamp, dataPath, stream, timeEntry;
	var newState = {
		groupCount: 0,
		groups: {}
	};
	
	for (i = 0; i < groupDirs.length; i++) {
		if (groupDirs[i][0] == '.') {
			continue;
		}
		
		groupName = groupDirs[i];
		groupPath = path.join(this.dbpath, groupDirs[i]);
		stats = fs.statSync(groupPath);
		if (!stats.isDirectory()) {
			continue;
		}
		
		timeEntryDirs = fs.readdirSync(groupPath);
		group = new Group.Group(groupName, groupPath);
		newState.groupCount++;
		newState.groups[groupName] = group;
		
		for (j = 0; j < timeEntryDirs.length; j++) {
			if (timeEntryDirs[j][0] == '.' || !looksLikeTimestamp(timeEntryDirs[j])) {
				continue;
			}
			
			timePath = path.join(groupPath, timeEntryDirs[j]);
			stats = fs.statSync(timePath);
			if (!stats.isDirectory()) {
				continue;
			}
			
			dayTimestamp = parseInt(timeEntryDirs[j]);
			dataPath = path.join(timePath, "data");
			try {
				stats = fs.statSync(dataPath);
			} catch (err) {
				if (err.code == 'ENOENT') {
					try {
						stream = openDataFile(dataPath);
					} catch (err) {
						throw new Error("Cannot create data file " +
							dataPath + ": " + err);
					}
					timeEntry = new TimeEntry.TimeEntry(dayTimestamp,
						timePath, stream, 0);
					group.timeEntryCount++;
					group.timeEntries[dayTimestamp] = timeEntry;
				} else {
					throw new Error("Cannot stat data file " +
						dataPath + ": " + err.message);
				}
				continue;
			}
			
			if (!stats.isFile()) {
				throw new Error("Data file " + dataPath + " is not a file");
			}
			try {
				stream = openDataFile(dataPath);
			} catch (err) {
				throw new Error("Cannot create data file " +
					dataPath + ": " + err);
			}
			timeEntry = new TimeEntry.TimeEntry(dayTimestamp,
				timePath, stream, stats.size);
			group.timeEntryCount++;
			group.timeEntries[dayTimestamp] = timeEntry;
		}
	}
	
	var groupName;
	for (groupName in this.groups) {
		this.groups[groupName].close();
	}
	this.groupCount = newState.groupCount;
	this.groups = newState.groups;
}

Database.prototype.findNonexistantGroups = function(toc) {
	var groupName, result = [];
	for (groupName in toc) {
		if (!this.groups[groupName]) {
			result.push(groupName);
		}
	}
	return result;
}

Database.prototype.findOrCreateGroup = function(groupName) {
	if (!Group.validateGroupName(groupName)) {
		throw new Error('Invalid group name');
	}
	
	var group = this.groups[groupName];
	if (!group) {
		var groupPath = path.join(this.dbpath, groupName);
		try {
			fs.mkdirSync(groupPath, 0755);
		} catch (err) {
			if (err.code != 'EEXIST') {
				throw err;
			}
		}
		group = new Group.Group(groupName, groupPath);
		this.groups[groupName] = group;
	}
	return group;
}

Database.prototype.findOrCreateTimeEntry = function(groupName, dayTimestamp) {
	var group = this.findOrCreateGroup(groupName);
	var timeEntry = group.timeEntries[dayTimestamp];
	if (!timeEntry) {
		var timePath = path.join(this.dbpath, groupName, dayTimestamp + "");
		var dataPath = path.join(timePath, "data");
		var size;
		
		try {
			fs.mkdirSync(timePath, 0700);
			size = 0;
		} catch (err) {
			if (err.code == 'EEXIST') {
				try {
					size = fs.statSync(dataPath).size;
				} catch (err) {
					if (err.code == 'ENOENT') {
						size = 0;
					} else {
						throw err;
					}
				}
			} else {
				throw err;
			}
		}
		
		var stream = openDataFile(dataPath);
		group.timeEntryCount++;
		timeEntry = new TimeEntry.TimeEntry(dayTimestamp,
			timePath, stream, size);
		group.timeEntries[dayTimestamp] = timeEntry;
	}
	return timeEntry;
}

Database.prototype.get = function(groupName, dayTimestamp, offset, callback) {
	var timeEntry;
	try {
		timeEntry = this.findOrCreateTimeEntry(groupName, dayTimestamp);
	} catch (err) {
		callback(err);
		return;
	}
	return timeEntry.get(offset, callback);
}

Database.prototype.add = function(groupName, dayTimestamp, buffers, checksumBuffer, callback) {
	var timeEntry;
	try {
		timeEntry = this.findOrCreateTimeEntry(groupName, dayTimestamp);
	} catch (err) {
		callback(err);
		return;
	}
	return timeEntry.add(buffers, checksumBuffer, callback);
}

Database.prototype.remove = function(groupName, dayTimestamp, callback) {
	console.assert(Group.validateGroupName(groupName));
	var group = this.groups[groupName];
	if (!group) {
		callback();
		return;
	}
	
	var self = this;
	if (!dayTimestamp) {
		this.groupCount--;
		delete this.groups[groupName];
	}
	group.remove(dayTimestamp, function() {
		self.emit('remove', groupName, dayTimestamp);
		callback();
	});
}

function looksLikeTimestamp(name) {
	return name[0] >= '0' && name[0] <= '9' && parseInt(name) == name;
}

function openDataFile(filename, callback) {
	return fs.createWriteStream(filename, {
		flags: 'a+',
		encoding: null,
		mode: 0600,
		fd: fs.openSync(filename, 'a+', 0600)
	});
}

exports.Database = Database;
exports.MAX_SIZE = TimeEntry.MAX_SIZE;
exports.MAX_SIZE_DESCRIPTION = TimeEntry.MAX_SIZE_DESCRIPTION;
