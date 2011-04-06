/*
 * Each data file in the database consists of an array of entry records
 * with the following format:
 *
 *   Magic        2 bytes             Always equal to the value "ET"
 *   Data size    4 bytes             In big endian format.
 *   CRC32        4 bytes             Checksum of the data.
 *   Data         'Data size' bytes   The actual data.
 */

var fs   = require('fs');
var path = require('path');
var child_process = require('child_process');
var TimeEntry     = require('./time_entry.js');
var Group         = require('./group.js');
var BufferUtils   = require('./buffer_utils.js');
var CRC32         = require('./crc32.js');

const MAX_SIZE = 1024 * 1024;
const MAX_SIZE_DESCRIPTION = "1 MB";

const ENTRY_MAGIC       = new Buffer("ET");
const SIZE_ENTRY_SIZE   = 4;
const CRC32_BINARY_SIZE = 4;
const HEADER_SIZE       = ENTRY_MAGIC.length + SIZE_ENTRY_SIZE + CRC32_BINARY_SIZE;

function Database(dbpath) {
	this.dbpath = dbpath;
	this.groupCount = 0;
	this.groups = {};
	this.ntimeEntriesBeingCreated = 0;
	this.reloading = false;
	this.checksummer = new CRC32.CRC32();
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

Database.prototype.get = function(groupName, dayTimestamp, offset, callback) {
	var timeEntry;
	try {
		timeEntry = this.findOrCreateTimeEntry(groupName, dayTimestamp);
	} catch (err) {
		callback(err);
		return;
	}
	
	if (timeEntry.dataFileSize < offset + HEADER_SIZE) {
		// Invalid offset.
		callback('not-found');
	} else {
		// Preallocate slightly larger buffer so that we don't have to allocate two
		// buffers in case the entry is < ~4 KB. The '- 3 * 8' is to account for
		// malloc overhead so that a single allocation fits in a single page.
		console.assert(HEADER_SIZE <= 1024 * 4 - 3 * 8);
		var self = this;
		var buf = new Buffer(1024 * 4 - 3 * 8);
		timeEntry.incFdUsage();
		fs.read(timeEntry.stream.fd, buf, 0, HEADER_SIZE, offset, function(err, bytesRead) {
			var i, size, storedChecksum;
			
			if (err) {
				timeEntry.decFdUsage();
				callback(err);
				return;
			} else if (bytesRead != HEADER_SIZE) {
				// Invalid offset or corrupt file.
				timeEntry.decFdUsage();
				callback('not-found');
				return;
			}
			
			// Check magic.
			for (i = 0; i < ENTRY_MAGIC.length; i++) {
				if (buf[i] != ENTRY_MAGIC[i]) {
					// Invalid offset or corrupt file.
					timeEntry.decFdUsage();
					callback('not-found');
					return;
				}
			}
			
			// Check size.
			size = BufferUtils.bufferToUint(buf, ENTRY_MAGIC.length);
			if (size > MAX_SIZE) {
				// Probably corrupt file..
				timeEntry.decFdUsage();
				callback('not-found');
				return;
			}
			
			storedChecksum = BufferUtils.bufferToUint(buf,
				ENTRY_MAGIC.length + SIZE_ENTRY_SIZE);
			
			// TODO: prevent TimeEntry.destroy() from closing
			// file descriptor until done.
			if (size > buf.length) {
				buf = new Buffer(size);
			}
			fs.read(timeEntry.stream.fd, buf, 0, size, offset + HEADER_SIZE,
				function(err, bytesRead)
			{
				timeEntry.decFdUsage();
				
				if (err) {
					callback(err);
					return;
				} else if (bytesRead != size) {
					// What's going on?
					callback('not-found');
					return;
				}
				
				self.checksummer.reset();
				self.checksummer.update(buf, 0, size);
				self.checksummer.finalize();
				
				if (self.checksummer.value == storedChecksum) {
					if (buf.length != size) {
						buf = buf.slice(0, size);
					}
					callback(undefined, buf);
				} else {
					// Probably corrupt file.
					callback('not-found');
				}
			});
		});
	}
}

Database.prototype.add = function(groupName, dayTimestamp, buffers, checksumBuffer, callback) {
	console.assert(checksumBuffer.length == CRC32_BINARY_SIZE);
	var timeEntry = this.findOrCreateTimeEntry(groupName, dayTimestamp);
	var flushed, i, totalSize = 0;
	var prevOffset = timeEntry.dataFileSize;
	
	for (i = 0; i < buffers.length; i++) {
		totalSize += buffers[i].length;
	}
	console.assert(totalSize < MAX_SIZE);
	
	function written(err) {
		if (err) {
			callback(err);
		} else {
			callback(undefined, prevOffset);
		}
	}
	
	var buf = new Buffer(HEADER_SIZE);
	ENTRY_MAGIC.copy(buf);
	BufferUtils.uintToBuffer(totalSize, buf, ENTRY_MAGIC.length);
	checksumBuffer.copy(buf, ENTRY_MAGIC.length + SIZE_ENTRY_SIZE);
	flushed = timeEntry.stream.write(buf);
	timeEntry.dataFileSize += HEADER_SIZE;
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
		var dayTimestampsToRemove = [];
		var dirsToRemove = [];
		var dts, i, timeEntry;
		
		for (dts in group.timeEntries) {
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
		
		for (i = 0; i < dayTimestampsToRemove.length; i++) {
			dts = dayTimestampsToRemove[i];
			timeEntry = group.timeEntries[dts];
			try {
				newFilename = renameToHidden(timeEntry.path);
			} catch (err) {
				deleteNextDir();
				callback(err);
				return;
			}
			timeEntry.destroy();
			group.timeEntryCount--;
			delete group.timeEntries[dts];
			dirsToRemove.push(newFilename);
		}
		
		deleteNextDir();
		callback();
		
	} else {
		try {
			newFilename = renameToHidden(group.path);
		} catch (err) {
			callback(err);
			return;
		}
		this.groupCount--;
		delete this.groups[groupName];
		group.destroy();
		child_process.spawn('rm', ['-rf', newFilename]);
		callback();
	}
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

function renameToHidden(filename, callback) {
	var dirname = path.dirname(filename);
	var basename = path.basename(filename);
	var newFilename;
	
	for (i = 0; i < 1000; i++) {
		newFilename = path.join(dirname, "._" + basename + '-' + i);
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
exports.MAX_SIZE = MAX_SIZE;
exports.MAX_SIZE_DESCRIPTION = MAX_SIZE_DESCRIPTION;
