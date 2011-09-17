var util   = require('util');
var events = require('events');
var fs     = require('fs');

var log           = require('./default_log.js').log;
var TimeEntry     = require('./time_entry.js');
var Group         = require('./group.js');
var HashSet       = require('./hashset.js').HashSet;

function Database() {
	events.EventEmitter.call(this);

	this.groups = {};
	this.shards = [];

	this.configure();

	// Monitor shard configuration
	fs.watchFile("config/shards.json", {}, this.configure);
}

util.inherits(Database, events.EventEmitter);

Database.prototype.configure = function() {
	var filename = "config/shards.json";
	try {
		var fileContents = fs.readFileSync(filename,'utf8');
		var shards = JSON.parse(fileContents).shards;
		if ( shards != this.shards) {
			this.shardsChanged(shards);
		}
	} catch(err) {
		log.notice('[ShardServer] Could not read config/shards.json\n' + err.description);
	}
}

Database.prototype.start = function() {
	console.assert(this.flushTimerId === undefined);
}

Database.prototype.shardsChanged = function(shards) {
	// establish new shards
	// get their TOC's
	// possibly rebalance
	this.shards = shards;
}


Database.prototype.close = function() {
}

Database.prototype.reload = function() {
}

Database.prototype.findTimeEntry = function(groupName, dayTimestamp) {
}

Database.prototype.get = function(groupName, dayTimestamp, offset, callback) {
	// lookup shard id('s) in TOC
	// forward to shard
	callback('not-found');
}

Database.prototype.add = function(groupName, dayTimestamp, buffers, checksumBuffer, callback) {
	// look up on which shard the file is for this data
	// if the file does not yet exist
	// put data in round robin order on a shard (is random more secure?)
	// log data entry in TOC with shard id
	// key = groupname + dayTimeStamp
	// if TOC.has? key
	//   TOC[key].shard.add data
	// else
	//   shard = hash(key) % shards
	//   shard.add data
	//   TOC[key].shard = shard
	// end
	callback();
}

Database.prototype.remove = function(groupName, dayTimestamp, callback) {
	// voor remove lock aanvragen op de file zodat de volgorde klopt met write acties
	// opletten dat locks niet gehouden worden door gecrashte instances
	callback();
}

Database.prototype.removeOne = function(groupName, dayTimestamp, callback) {
	callback();
}

Database.prototype.rebalance = function(callback) {
	// total_size = toc.total_size
	// ideal = total_size / shards
	// over = shards.select{|s| s.size > ideal}
	// shorted = shards.select{|s| s.size < ideal}
	// over.each do |shard|
	//   to_distribute = shard.size - ideal
	//   data = // selecteer to_distribute aan data uit shard
	//   shorted.each do |sh|
	//     to_receive = ideal - sh.size
	//     sh.add // selecteer to_receive uit data
	//   end
	// end
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
