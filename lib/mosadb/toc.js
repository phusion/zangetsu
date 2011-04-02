var fs   = require('fs');
var path = require('path');
var TimeEntry = require('./time_entry.js').TimeEntry;
var Group     = require('./group.js').Group;

function Toc(dbpath) {
	this.dbpath = dbpath;
}

Toc.prototype.reload = function(callback) {
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
				callback();
			}
		}
	}
	
	function scanTimeDirectory(dayTimestamp, timePath, group, err, stats) {
		if (state.aborted) {
			return;
		} else if (err) {
			state.aborted = true;
			callback("Cannot stat directory " + timePath + ": " + err);
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
						timeEntry = new TimeEntry(dayTimestamp, timePath, 0);
						group.timeEntries[dayTimestamp] = timeEntry;
						decOperations();
					} else {
						state.aborted = true;
						callback("Cannot stat data file " + dataPath + ": " + err.message);
					}
				} else {
					timeEntry = new TimeEntry(dayTimestamp, timePath, stats.size);
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
			callback("Cannot stat " + groupPath + ": " + err);
			return;
		}
		
		if (stats.isDirectory()) {
			state.operations++;
			fs.readdir(groupPath, function(err, files) {
				if (state.aborted) {
					return;
				} else if (err) {
					state.aborted = true;
					callback("Cannot read directory " + groupPath + ": " + err);
					return;
				}
				
				var groupName = path.basename(groupPath);
				var group = new Group(groupName, groupPath);
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
	
	this.operations++;
	fs.readdir(this.dbpath, function(err, files) {
		if (err) {
			callback("Cannot read directory " + state.self.dbpath + ": " + err.message);
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

function looksLikeTimestamp(name) {
	return name[0] >= '0' && name[0] <= '9' && parseInt(name) == name;
}

exports.Toc = Toc;
