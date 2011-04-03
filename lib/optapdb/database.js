var fs   = require('fs');
var path = require('path');
var TimeEntry = require('./time_entry.js');
var Group     = require('./group.js');

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
				var timeEntry;
				if (state.aborted) {
					return;
				} else if (err) {
					if (err.code == 'ENOENT') {
						timeEntry = new TimeEntry.TimeEntry(dayTimestamp,
							timePath, 0);
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
					timeEntry = new TimeEntry.TimeEntry(dayTimestamp,
						timePath, stats.size);
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
			var timePath = path.join(this.dbpath, groupName, dayTimestamp);
			var self = this;
			
			group.timeEntriesBeingCreated[dayTimestamp] = [];
			group.timeEntriesBeingCreated[dayTimestamp].push(callback);
			this.ntimeEntriesBeingCreated++;
			
			fs.mkdir(timePath, 0700, function(err) {
				var i, callbacks = group.timeEntriesBeingCreated[dayTimestamp];
				delete group.timeEntriesBeingCreated[dayTimestamp];
				self.ntimeEntriesBeingCreated--;
				
				if (err && err.code != 'EEXIST') {
					for (i = 0; i < callbacks.length; i++) {
						callbacks[i](err);
					}
				} else {
					group.timeEntryCount++;
					timeEntry = new TimeEntry.TimeEntry(dayTimestamp, timePath, 0);
					group.timeEntries[dayTimestamp] = timEntry;
					for (i = 0; i < callbacks.length; i++) {
						callbacks[i](undefined, timeEntry);
					}
				}
			});
		}
	});
}

function looksLikeTimestamp(name) {
	return name[0] >= '0' && name[0] <= '9' && parseInt(name) == name;
}

exports.Database = Database;
