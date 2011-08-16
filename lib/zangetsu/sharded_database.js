var util   = require('util');
var events = require('events');
var TimeEntry     = require('./time_entry.js');
var Group         = require('./group.js');
var HashSet       = require('./hashset.js').HashSet;

function Database() {
	events.EventEmitter.call(this);

	this.groupCount = 0;
	this.groups = {};
	this.lastObjectId = 0;
	
	this.addOperations = 0;
	this.waitingAddOperations = [];
}
util.inherits(Database, events.EventEmitter);

Database.prototype.start = function() {
	console.assert(this.flushTimerId === undefined);
}

Database.prototype.close = function() {
}

Database.prototype.reload = function() {
}

Database.prototype.findTimeEntry = function(groupName, dayTimestamp) {
}

Database.prototype.get = function(groupName, dayTimestamp, offset, callback) {
	callback('not-found');
}

Database.prototype.add = function(groupName, dayTimestamp, buffers, checksumBuffer, callback) {
	callback();
}

Database.prototype.remove = function(groupName, dayTimestamp, callback) {
	callback();
}

Database.prototype.removeOne = function(groupName, dayTimestamp, callback) {
	callback();
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


function looksLikeTimestamp(name) {
	return name == parseInt(name) + '';
}

exports.Database = Database;
