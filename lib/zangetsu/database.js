var fs     = require('fs');
var path   = require('path');
var util   = require('util');
var events = require('events');
var TimeEntry     = require('./time_entry.js');
var Group         = require('./group.js');
var HashSet       = require('./hashset.js').HashSet;

const MAX_UNEVICTED  = 1024 * 1024 * 64;
const SYNC_AND_EVICT_INTERVAL = 10000;


function Database(dbpath) {
	events.EventEmitter.call(this);
	this.dbpath = dbpath;
	this.groupCount = 0;
	this.groups = {};
	this.lastObjectId = 0;
	
	this.writeLocks = 0;
	this.writeLockReleaseCallbacks = [];
	this.unevictedSize = 0;
	this.unevictedTimeEntries = new HashSet();
}
util.inherits(Database, events.EventEmitter);


Database.prototype._verifyInvariants = function() {
	console.assert(this.writeLocks >= 0);
	console.assert(this.unevictedSize >= 0);
}

Database.prototype._incWriteLockCount = function() {
	this.writeLocks++;
}

Database.prototype._decWriteLockCount = function() {
	console.assert(this.writeLocks > 0);
	this.writeLocks--;
	if (this.writeLocks == 0) {
		var callbacks = this.writeLockReleaseCallbacks;
		this.writeLockReleaseCallbacks = [];
		for (var i = 0; i < callbacks.length; i++) {
			callbacks[i]();
		}
	}
}


Database.prototype.start = function() {
	console.assert(this.flushTimerId === undefined);
	this.syncAndEvictTimerId = setInterval(this.syncAndEvict.bind(this),
		SYNC_AND_EVICT_INTERVAL);
}

Database.prototype.close = function() {
	var groupName;
	clearInterval(this.syncAndEvictTimerId);
	delete this.syncAndEvictTimerId;
	for (groupName in this.groups) {
		this.groups[groupName].close();
	}
	this.groupCount = 0;
	this.groups = {};
	this.unevictedSize = 0;
	this.unevictedTimeEntries.clear();
}

Database.prototype.reload = function() {
	// TODO: clear stale renames upon reloading
	
	var groupDirs = fs.readdirSync(this.dbpath);
	var i, groupName, groupPath, stats, timeEntryDirs, group,
		j, timePath, dayTimestamp, dataPath, stream, timeEntry;
	var newState = {
		groupCount: 0,
		groups: {},
		lastObjectId: 0
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
					timeEntry = new TimeEntry.TimeEntry(this,
						newState.lastObjectId, dayTimestamp,
						timePath, stream, 0);
					newState.lastObjectId++;
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
			timeEntry = new TimeEntry.TimeEntry(this, newState.lastObjectId,
				dayTimestamp, timePath, stream, stats.size);
			newState.lastObjectId++;
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
	this.lastObjectId = newState.lastObjectId;
	this.unflushedSize = 0;
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
		timeEntry = new TimeEntry.TimeEntry(this, this.lastObjectId,
			dayTimestamp, timePath, stream, size);
		this.lastObjectId++;
		group.timeEntries[dayTimestamp] = timeEntry;
	}
	return timeEntry;
}

Database.prototype.findTimeEntry = function(groupName, dayTimestamp) {
	var group = this.groups[groupName];
	if (group) {
		return group.timeEntries[dayTimestamp];
	}
}

Database.prototype.get = function(groupName, dayTimestamp, offset, callback) {
	var timeEntry = this.findTimeEntry(groupName, dayTimestamp);
	if (timeEntry) {
		return timeEntry.get(offset, callback);
	} else {
		callback('not-found');
	}
}

Database.prototype.add = function(groupName, dayTimestamp, buffers, checksumBuffer, callback) {
	var timeEntry;
	try {
		timeEntry = this.findOrCreateTimeEntry(groupName, dayTimestamp);
	} catch (err) {
		callback(err);
		return;
	}
	
	var self = this;
	
	function doAdd(err) {
		timeEntry.add(buffers, checksumBuffer, function(err, offset, size, buffers) {
			timeEntry.decWriteOperations();
			timeEntry._verifyInvariants();
			if (err) {
				callback(err);
			} else {
				self.emit('add', groupName, dayTimestamp, offset, size, buffers);
				callback(undefined, offset, size);
			}
		});
	}
	
	timeEntry.incWriteOperations();
	if (this.writeLocks > 0) {
		this.writeLockReleaseCallbacks.push(doAdd);
	} else {
		doAdd();
	}
}

Database.prototype.remove = function(groupName, dayTimestamp, callback) {
	console.assert(Group.validateGroupName(groupName));
	console.assert(dayTimestamp === undefined || looksLikeTimestamp(dayTimestamp));
	
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
	group.remove(dayTimestamp, function(err) {
		if (err) {
			callback(err);
		} else {
			self.emit('remove', groupName, dayTimestamp);
			callback();
		}
	});
}

Database.prototype.removeOne = function(groupName, dayTimestamp, callback) {
	console.assert(Group.validateGroupName(groupName));
	console.assert(looksLikeTimestamp(dayTimestamp));
	
	var group = this.groups[groupName];
	if (!group) {
		callback();
		return;
	}
	
	group.removeOne(dayTimestamp, function(err) {
		if (err) {
			callback(err);
		} else {
			self.emit('removeOne', groupName, dayTimestamp);
			callback();
		}
	});
}

Database.prototype.syncAndEvict = function() {
	var self = this;
	var timeEntries = this.unevictedTimeEntries.values();
	if (timeEntries.length == 0) {
		return;
	}
	
	var i;
	var counter = 0;
	var evictingSize = this.unevictedSize;
	
	this.unevictedTimeEntries.clear();
	this.unevictedSize = 0;
	this._incWriteLockCount();
	
	function done() {
		self._decWriteLockCount();
		self._verifyInvariants();
		console.log("Done evicting.");
	}
	
	function decCounter() {
		console.assert(counter > 0);
		counter--;
		if (counter == 0) {
			done();
		}
	}
	
	function doSyncAndEvict(timeEntry) {
		timeEntry.sync(function(err) {
			if (timeEntry.isClosed() || err) {
				decCounter();
			} else {
				timeEntry.evict(decCounter);
			}
		});
	}
	
	for (i = 0; i < timeEntries.length; i++) {
		if (!timeEntries[i].isClosed()) {
			counter++;
		}
	}
	for (i = 0; i < timeEntries.length; i++) {
		if (!timeEntries[i].isClosed()) {
			doSyncAndEvict(timeEntries[i]);
		}
	}
	
	if (counter == 0) {
		done();
	} else if (evictingSize < 1024 * 1024) {
		console.log("Evicting %s KB over %d entries...",
			(evictingSize / 1024).toFixed(2),
			timeEntries.length);
	} else {
		console.log("Evicting %s MB over %d entries...",
			(evictingSize / 1024 / 1024).toFixed(2),
			timeEntries.length);
	}
}

Database.prototype.registerDataGrowth = function(timeEntry, size) {
	console.assert(timeEntry.database === this);
	console.assert(!timeEntry.closed && !timeEntry.closing);
	this.unevictedTimeEntries.add(timeEntry);
	this.unevictedSize += size;
	if (this.unevictedSize >= MAX_UNEVICTED) {
		var self = this;
		self.syncAndEvict();
	}
}

function looksLikeTimestamp(name) {
	return name == parseInt(name) + '';
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
