var fs   = require('fs');
var path = require('path');
var TimeEntry = require('./time_entry.js');
var Group     = require('./group.js');
var BufferUtils = require('./buffer_utils.js');

const DATA_FILE_HEADER  = new Buffer("ET");
const CRC32_BINARY_SIZE = 4;

function Database(dbpath) {
	this.dbpath = dbpath;
	this.groupCount = 0;
	this.groups = {};
	this.ngroupsBeingCreated = 0;
	this.groupsBeingCreated = {};
	this.ntimeEntriesBeingCreated = 0;
	this.reloading = false;
}

Database.prototype.reload = function(callback) {
	if (this.ngroupsBeingCreated > 0 || this.ntimeEntriesBeingCreated > 0) {
		// I don't want to write code for handling concurrency issues.
		// Just don't support this until there's a need for live reload.
		throw Error("Cannot reload while groups or time entries are being created");
	}
	
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
						state.operations++;
						createDataFile(dataPath, function(err) {
							if (state.aborted) {
								return;
							} else if (err) {
								state.aborted = true;
								callback(new Error("Cannot create data file " +
									dataPath + ": " + err));
								return;
							}
							
							stream = fs.createWriteStream(dataPath,
								{ flags: 'a', encoding: null, mode: 0600 });
							timeEntry = new TimeEntry.TimeEntry(dayTimestamp,
								timePath, stream, 0);
							group.timeEntryCount++;
							group.timeEntries[dayTimestamp] = timeEntry;
							decOperations();
						});
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
					stream = fs.createWriteStream(dataPath,
						{ flags: 'a', encoding: null, mode: 0600 });
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

Database.prototype.findOrCreateGroup = function(groupName, callback) {
	if (this.reloading) {
		// I don't want to write code for handling concurrency issues.
		// Just don't support this until there's a need for live reload.
		throw Error("Cannot perform this operation while reloading");
	} else if (!Group.validateGroupName(groupName)) {
		callback(new Error('Invalid group name'));
		return;
	}
	
	var group = this.groups[groupName];
	if (group) {
		callback(undefined, group);
	} else if (this.groupsBeingCreated[groupName]) {
		/* Group is already being created. Let the given callback
		 * be called when the group is done creating.
		 */
		this.groupsBeingCreated[groupName].push(callback);
	} else {
		var groupPath = path.join(this.dbpath, groupName);
		var self = this;
		
		this.groupsBeingCreated[groupName] = [];
		this.groupsBeingCreated[groupName].push(callback);
		this.ngroupsBeingCreated++;
		
		fs.mkdir(groupPath, 0755, function(err) {
			var i, callbacks = self.groupsBeingCreated[groupName];
			delete self.groupsBeingCreated[groupName];
			self.ngroupsBeingCreated--;
			
			if (err && err.code != 'EEXIST') {
				for (i = 0; i < callbacks.length; i++) {
					callbacks[i](err);
				}
			} else {
				self.groupCount++;
				group = new Group.Group(groupName, groupPath);
				self.groups[groupName] = group;
				for (i = 0; i < callbacks.length; i++) {
					callbacks[i](undefined, group);
				}
			}
		});
	}
}

Database.prototype.findOrCreateTimeEntry = function(groupName, dayTimestamp, callback) {
	var self = this;
	this.findOrCreateGroup(groupName, function(err, group) {
		if (err) {
			callback(err);
			return;
		}
		
		var timeEntry = group.timeEntries[dayTimestamp];
		if (timeEntry) {
			callback(undefined, timeEntry);
		} else if (group.timeEntriesBeingCreated[dayTimestamp]) {
			group.timeEntriesBeingCreated[dayTimestamp].push(callback);
		} else {
			var timePath = path.join(self.dbpath, groupName, dayTimestamp + "");
			
			group.timeEntriesBeingCreated[dayTimestamp] = [];
			group.timeEntriesBeingCreated[dayTimestamp].push(callback);
			self.ntimeEntriesBeingCreated++;
			
			fs.mkdir(timePath, 0700, function(err) {
				var i, callbacks, dataPath;
				
				if (err && err.code != 'EEXIST') {
					callbacks = group.timeEntriesBeingCreated[dayTimestamp];
					delete group.timeEntriesBeingCreated[dayTimestamp];
					self.ntimeEntriesBeingCreated--;
					for (i = 0; i < callbacks.length; i++) {
						callbacks[i](err);
					}
					return;
				}
				
				dataPath = path.join(timePath, "data");
				createDataFile(dataPath, function(err) {
					callbacks = group.timeEntriesBeingCreated[dayTimestamp];
					delete group.timeEntriesBeingCreated[dayTimestamp];
					self.ntimeEntriesBeingCreated--;
					
					if (err) {
						for (i = 0; i < callbacks.length; i++) {
							callbacks[i](err);
						}
						return;
					}
					
					var stream = fs.createWriteStream(dataPath,
						{ flags: 'a', encoding: null, mode: 0600 });
					group.timeEntryCount++;
					timeEntry = new TimeEntry.TimeEntry(dayTimestamp,
						timePath, stream, 0);
					group.timeEntries[dayTimestamp] = timeEntry;
					for (i = 0; i < callbacks.length; i++) {
						callbacks[i](undefined, timeEntry);
					}
				});
			});
		}
	});
}

Database.prototype.add = function(groupName, timestamp, buffers, checksumBuffer, callback) {
	console.assert(checksumBuffer.length == CRC32_BINARY_SIZE);
	this.findOrCreateTimeEntry(groupName, timestamp / 60 / 60 / 24, function(err, timeEntry) {
		if (err) {
			callback(err);
			return;
		}
		
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
	})
}

function looksLikeTimestamp(name) {
	return name[0] >= '0' && name[0] <= '9' && parseInt(name) == name;
}

function createDataFile(filename, callback) {
	fs.open(filename, 'a', 0600, function(err, fd) {
		if (err) {
			callback(err);
		} else {
			fs.close(fd);
			callback();
		}
	});
}

exports.Database = Database;
