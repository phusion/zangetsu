var fs     = require('fs');
var path   = require('path');
var util   = require('util');
var events = require('events');
var log           = require('./default_log.js').log;
var TimeEntry     = require('./time_entry.js');
var Group         = require('./group.js');
var HashSet       = require('./hashset.js').HashSet;
var log           = require('./default_log.js').log;


/* Error codes. Make sure that these do not overlap with the error codes in time_entry.js! */
exports.ERR_NOT_FOUND    = 10;


const NORMAL  = 0,
      LOCKING = 1,
      LOCKED  = 2;

const MAX_UNEVICTED  = 1024 * 1024 * 64;
const SYNC_AND_EVICT_INTERVAL = 10000;


function Database(dbpath) {
	events.EventEmitter.call(this);
	this.dbpath = dbpath;
	this.groupCount = 0;
	this.groups = {};
	this.lastObjectId = 0;
	
	this.state = NORMAL;
	this.addOperations = 0;
	this.waitingLockers = [];
	this.waitingAddOperations = [];
	this.syncAndEvictInProgress = false;
	
	this.unevictedSize = 0;
	this.unevictedTimeEntries = new HashSet();
}
util.inherits(Database, events.EventEmitter);


function createError(message, code) {
	var e = new Error(message);
	e.code = code;
	e.codeno = exports[code];
	console.assert(e.codeno != undefined);
	return e;
}

Database.prototype._verifyInvariants = function() {
	console.assert(this.waitingLockers.length >= 0);
	console.assert(this.waitingAddOperations.length >= 0);
	console.assert(this.unevictedSize >= 0);
	console.assert(!( this.state == NORMAL ) || ( this.waitingLockers.length == 0 ));
	console.assert(!( this.state == NORMAL ) || ( this.waitingAddOperations.length == 0 ));
	console.assert(!( this.state == LOCKING ) || ( this.waitingLockers.length > 0 ));
}

Database.prototype._checkClean = function() {
	var stat;
	try {
		stat = fs.statSync(this.getLockFileName());
		return false;
	} catch (e) {
		if (e.code == 'ENOENT') {
			return true;
		} else {
			throw e;
		}
	}
}


Database.prototype.getLockFileName = function() {
	return this.dbpath + '/lock';
}

Database.prototype.start = function() {
	console.assert(this.flushTimerId === undefined);
	this.clean = this._checkClean();
	this.syncAndEvictTimerId = setInterval(this.syncAndEvict.bind(this),
		SYNC_AND_EVICT_INTERVAL);
	var fd = fs.openSync(this.getLockFileName(), 'w', 0644);
	fs.fsyncSync(fd);
	fs.closeSync(fd);
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
	fs.unlinkSync(this.getLockFileName());
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

Database.prototype._findOrCreateGroup = function(groupName) {
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

Database.prototype._findOrCreateTimeEntry = function(groupName, dayTimestamp) {
	var group = this._findOrCreateGroup(groupName);
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
		callback(createError('Time entry not found', 'ERR_NOT_FOUND'));
	}
}

Database.prototype.add = function(groupName, dayTimestamp, buffers, checksumBuffer, options, callback) {
	var timeEntry;

	if (typeof(options) == 'function') {
		callback = options;
		options = undefined;
	}

	try {
		timeEntry = this._findOrCreateTimeEntry(groupName, dayTimestamp);
	} catch (err) {
		callback(err);
		return;
	}

	/*
	 * Perform two concurrent operations:
	 * 1. Emit 'adding' and wait until all listeners
	 *    have reported that they're done.
	 * 2. Store the data on local disk and wait until
	 *    it's done.
	 *
	 * The callback is called after both operations are done.
	 */
	
	var self = this;
	var concurrentOperationsDone = 0;
	var result;

	function callAddingListeners() {
		var listeners = self.listeners('adding').length;
		if (listeners > 0) {
			var counter = 0;
			self.emit('adding', timeEntry, groupName, dayTimestamp, buffers, function() {
				console.assert(counter <= listeners);
				counter++;
				if (counter == listeners) {
					concurrentOperationDone();
				}
			});
		} else {
			concurrentOperationDone();
		}
	}

	function storeOnLocalDisk(callback) {
		if (self.state == LOCKING || self.state == LOCKED) {
			self.waitingAddOperations.push(performStoringOnLocalDisk);
		} else {
			performStoringOnLocalDisk();
		}
	}

	function concurrentOperationDone() {
		console.assert(concurrentOperationsDone <= 2);
		concurrentOperationsDone++;
		if (concurrentOperationsDone == 2) {
			allConcurrentOperationsDone();
		}
	}

	function allConcurrentOperationsDone() {
		var err     = result.err;
		var offset  = result.offset;
		var size    = result.size;
		var buffers = result.buffers;
		
		self.unevictedTimeEntries.add(timeEntry);
		self.unevictedSize += size;
		if (self.unevictedSize >= MAX_UNEVICTED) {
			self.syncAndEvict();
			console.assert(self.state == LOCKING);
			console.assert(self.waitingLockers.length > 0);
		}
		self.addOperations--;
		timeEntry.decWriteOperations();
		timeEntry._verifyInvariants();
		self._verifyInvariants();
		
		if (self.addOperations == 0 && self.state == LOCKING) {
			console.assert(self.waitingLockers.length > 0);
			self.state = LOCKED;
			var cb = self.waitingLockers.shift();
			cb();
		}
		
		if (err) {
			callback(err);
		} else {
			callback(undefined, offset, size);
		}
	}
	
	function reallyAdd() {
		timeEntry.add(buffers, checksumBuffer, options, function(err, offset, size, buffers) {
			console.assert(self.addOperations > 0);
			console.assert(self.state == NORMAL || self.state == LOCKING);
			
			self.emit('added', timeEntry, groupName, dayTimestamp, offset, size, buffers);
			result = {
				err    : err,
				offset : offset,
				size   : size,
				buffers: buffers
			}
			concurrentOperationDone();
		});
	}
	
	function performStoringOnLocalDisk() {
		console.assert(self.state == NORMAL);
		self.addOperations++;
		if (self.addSleepTime !== undefined) {
			setTimeout(reallyAdd, self.addSleepTime);
		} else {
			reallyAdd();
		}
	}

	timeEntry.incWriteOperations();
	callAddingListeners();
	storeOnLocalDisk();
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
	
	var self = this;
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
	
	if (this.syncAndEvictInProgress) {
		return;
	}
	this.syncAndEvictInProgress = true;
	this.lock(function() {
		var timeEntries = self.unevictedTimeEntries.values();
		if (timeEntries.length == 0) {
			self.syncAndEvictInProgress = false;
			return self.unlock();
		}
		
		var i;
		var counter = 0;
		var evictingSize = self.unevictedSize;
		
		self.unevictedTimeEntries.clear();
		self.unevictedSize = 0;
		
		function done() {
			self.syncAndEvictInProgress = false;
			log.debug("Done evicting.");
			self.unlock();
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
			log.debug("Evicting %s KB over %d entries...",
				(evictingSize / 1024).toFixed(2),
				timeEntries.length);
		} else {
			log.debug("Evicting %s MB over %d entries...",
				(evictingSize / 1024 / 1024).toFixed(2),
				timeEntries.length);
		}
	});
}

Database.prototype.lock = function(callback) {
	if (this.state == LOCKING || this.state == LOCKED) {
		this.waitingLockers.push(callback);
	} else {
		console.assert(this.state == NORMAL);
		if (this.addOperations > 0) {
			this.state = LOCKING;
			this.waitingLockers.push(callback);
		} else {
			this.state = LOCKED;
			callback();
		}
	}
}

Database.prototype.unlock = function() {
	console.assert(this.state == LOCKED);
	console.assert(this.addOperations == 0);
	var callback = this.waitingLockers.shift();
	if (callback) {
		this._verifyInvariants();
		callback();
	} else {
		this.state = NORMAL;
		var callbacks = this.waitingAddOperations;
		var origLength = callbacks.length;
		this.waitingAddOperations = [];
		while (callbacks.length > 0) {
			callback = callbacks.shift();
			callback();
		}
		console.assert(this.addOperations == origLength);
		console.assert(this.state == NORMAL);
		this._verifyInvariants();
	}
}

Database.prototype.toTocFormat = function() {
	var toc = {};
	var groupName;
	var group, groupInToc;
	var dayTimestamp;
	var timeEntry, timeEntryInToc;
	
	for (groupName in this.groups) {
		group = this.groups[groupName];
		groupInToc = toc[groupName] = {};
		for (dayTimestamp in group.timeEntries) {
			timeEntry = group.timeEntries[dayTimestamp];
			timeEntryInToc = groupInToc[dayTimestamp] = {};
			timeEntryInToc.size = timeEntry.writtenSize;
		}
	}
	
	return toc;
}

Database.prototype.calculateRecordSize = function(buffers) {
	return TimeEntry.calculateRecordSize(buffers);
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

exports.NORMAL  = NORMAL;
exports.LOCKING = LOCKING;
exports.LOCKED  = LOCKED;
