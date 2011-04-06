var fs   = require('fs');
var path = require('path');
var child_process = require('child_process');
var TimeEntry = require('./time_entry.js');
var Group     = require('./group.js');
var BufferUtils = require('./buffer_utils.js');

const DATA_FILE_HEADER  = new Buffer("ET");
const CRC32_BINARY_SIZE = 4;

function Database(dbpath) {
	this.dbpath = dbpath;
	this.groupCount = 0;
	this.groups = {};
	this.ntimeEntriesBeingCreated = 0;
	this.reloading = false;
}

Database.prototype.reload = function(callback) {
	var state = {
		self: this,
		aborted: false,
		operations: 1,
		groupCount: 0,
		groups: {}
	};
	
	function decOperations() {
		if (!state.aborted) {
			state.operations--;
			if (state.operations == 0) {
				state.self.groupCount = state.groupCount;
				state.self.groups = state.groups;
				state.self.reloading = false;
				callback();
			}
		}
	}
	
	function scanTimeDirectory(dayTimestamp, timePath, group, err, stats) {
		if (state.aborted) {
			return;
		} else if (err) {
			state.aborted = true;
			callback(new Error("Cannot stat directory " + timePath + ": " + err));
			return;
		}
		
		if (stats.isDirectory()) {
			var dataPath = path.join(timePath, "data");
			state.operations++;
			fs.stat(dataPath, function(err, stats) {
				var timeEntry, stream;
				if (state.aborted) {
					return;
				} else if (err) {
					if (err.code == 'ENOENT') {
						try {
							stream = openDataFile(dataPath);
						} catch (err) {
							state.aborted = true;
							callback(new Error("Cannot create data file " +
								dataPath + ": " + err));
							return;
						}
						timeEntry = new TimeEntry.TimeEntry(dayTimestamp,
							timePath, stream, 0);
						group.timeEntryCount++;
						group.timeEntries[dayTimestamp] = timeEntry;
						decOperations();
					} else {
						state.aborted = true;
						callback(new Error("Cannot stat data file " +
							dataPath + ": " + err.message));
					}
				} else if (!stats.isFile()) {
					state.aborted = true;
					callback(new Error("Data file " + dataPath + " is not a file"));
				} else {
					try {
						stream = openDataFile(dataPath);
					} catch (err) {
						state.aborted = true;
						callback(new Error("Cannot create data file " +
							dataPath + ": " + err));
						return;
					}
					timeEntry = new TimeEntry.TimeEntry(dayTimestamp,
						timePath, stream, stats.size);
					group.timeEntryCount++;
					group.timeEntries[dayTimestamp] = timeEntry;
					decOperations();
				}
			});
		}
		decOperations();
	}
	
	function scanGroupDirectory(groupPath, err, stats) {
		if (state.aborted) {
			return;
		} else if (err) {
			state.aborted = true;
			callback(new Error("Cannot stat " + groupPath + ": " + err));
			return;
		}
		
		if (stats.isDirectory()) {
			state.operations++;
			fs.readdir(groupPath, function(err, files) {
				if (state.aborted) {
					return;
				} else if (err) {
					state.aborted = true;
					callback(new Error("Cannot read directory " +
						groupPath + ": " + err));
					return;
				}
				
				var groupName = path.basename(groupPath);
				var group = new Group.Group(groupName, groupPath);
				state.groupCount++;
				state.groups[groupName] = group;
				
				var i, timePath;
				for (i = 0; i < files.length; i++) {
					if (files[i][0] == '.' || !looksLikeTimestamp(files[i])) {
						continue;
					}
					timePath = path.join(groupPath, files[i]);
					state.operations++;
					fs.stat(timePath, scanTimeDirectory.bind(state.this,
						parseInt(files[i]), timePath, group));
				}
				decOperations();
			});
		}
		decOperations();
	}
	
	this.reloading = true;
	this.operations++;
	fs.readdir(this.dbpath, function(err, files) {
		if (err) {
			callback(new Error("Cannot read directory " + state.self.dbpath +
				": " + err.message));
			return;
		}
		
		var i, groupPath;
		for (i = 0; i < files.length; i++) {
			if (files[i][0] == '.') {
				continue;
			}
			
			groupPath = path.join(state.self.dbpath, files[i]);
			state.operations++;
			fs.stat(groupPath, scanGroupDirectory.bind(state.self, groupPath));
		}
		decOperations();
	});
}

Database.prototype.findOrCreateGroup = function(groupName) {
	if (this.reloading) {
		// I don't want to write code for handling concurrency issues.
		// Just don't support this until there's a need for live reload.
		throw Error("Cannot perform this operation while reloading");
	} else if (!Group.validateGroupName(groupName)) {
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

Database.prototype.add = function(groupName, dayTimestamp, buffers, checksumBuffer, callback) {
	console.assert(checksumBuffer.length == CRC32_BINARY_SIZE);
	var timeEntry = this.findOrCreateTimeEntry(groupName, dayTimestamp);
	var flushed, i, totalSize = 0;
	var prevOffset = timeEntry.dataFileSize;
	
	for (i = 0; i < buffers.length; i++) {
		totalSize += buffers[i].length;
	}
	
	function written(err) {
		if (err) {
			callback(err);
		} else {
			callback(undefined, prevOffset);
		}
	}
	
	var buf = new Buffer(DATA_FILE_HEADER.length + CRC32_BINARY_SIZE);
	DATA_FILE_HEADER.copy(buf);
	BufferUtils.uintToBuffer(totalSize, buf, DATA_FILE_HEADER.length);
	flushed = timeEntry.stream.write(buf);
	timeEntry.dataFileSize += buf.length;
	console.assert(!flushed, "Code assumes that file I/O is never flushed immediately");
	
	flushed = timeEntry.stream.write(checksumBuffer,
		(buffers.length == 0) ? written : undefined);
	timeEntry.dataFileSize += checksumBuffer.length;
	console.assert(!flushed, "Code assumes that file I/O is never flushed immediately");
	
	for (i = 0; i < buffers.length; i++) {
		flushed = timeEntry.stream.write(buffers[i],
			(i == buffers.length - 1) ? written : undefined);
		timeEntry.dataFileSize += buffers[i].length;
		console.assert(!flushed, "Code assumes that file I/O is never flushed immediately");
	}
}

Database.prototype.remove = function(groupName, dayTimestamp, callback) {
	console.assert(Group.validateGroupName(groupName));
	var group = this.groups[groupName];
	if (!group) {
		callback();
		return;
	}
	
	var self = this;
	var newFilename;
	
	if (dayTimestamp) {
		var timeEntry = group.timeEntries[dayTimestamp];
		if (!timeEntry) {
			callback();
			return;
		}
		group.timeEntryCount--;
		delete group.timeEntries[dayTimestamp];
		timeEntry.destroy();
		try {
			newFilename = renameToHidden(timeEntry.path);
		} catch (err) {
			callback(err);
			return;
		}
		
	} else {
		this.groupCount--;
		delete this.groups[groupName];
		group.destroy();
		try {
			newFilename = renameToHidden(group.path);
		} catch (err) {
			callback(err);
			return;
		}
	}
	
	child_process.spawn('rm', ['-rf', newFilename]);
	callback();
}

function looksLikeTimestamp(name) {
	return name[0] >= '0' && name[0] <= '9' && parseInt(name) == name;
}

function openDataFile(filename, callback) {
	var fd = fs.openSync(filename, 'a', 0600);
	fs.close(fd);
	return fs.createWriteStream(filename, { flags: 'a', encoding: null, mode: 0600 });
}

function renameToHidden(filename, callback) {
	var dirname = path.dirname(filename);
	var basename = path.basename(filename);
	var newFilename;
	
	for (i = 0; i < 1000; i++) {
		newFilename = path.join(dirname, "._" + basename + i);
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
	// TODO: clear stale renames upon reloading
	throw Error("Cannot find a suitable filename to rename to");
}

exports.Database = Database;
