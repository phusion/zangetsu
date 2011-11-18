var util   = require('util');
var events = require('events');
var fs     = require('fs');

var log           = require('./default_log.js').log;
var TimeEntry     = require('./time_entry.js');
var Group         = require('./group.js');
var HashSet       = require('./hashset.js').HashSet;
var Shard         = require('./shard.js').Shard;

function Database(config_file) {
	events.EventEmitter.call(this);

	this.groups = {};
	this.shard_configuration = [];
	this.shards = {};
	this.toc = {};
	this.toc.groups = {};


	if(config_file == undefined) {
		config_file = "./config/shards.json";
	}
	this.configure(config_file);

	var database = this;

	// Monitor shard configuration
	//fs.watchFile("./config/shards.json", {}, function() { database.configure()});
}

util.inherits(Database, events.EventEmitter);

Database.prototype.configure = function(filename) {
	try {
		var fileContents = fs.readFileSync(filename,'utf8');
		var shards = JSON.parse(fileContents).shards.sort(shardSorter);
		//log.notice('[ShardServer] Read configuration file');
		var i;
		var different = !(shards.length == this.shard_configuration.length);
		for(i = 0; i < shards.length && (!different); i++) {
			different = !(shards[i].hostname == this.shard_configuration[i].hostname)	
		}
		if (different) {
			this.shardsChanged(shards);
		}
	} catch(err) {
		log.notice('[ShardServer] Could not read config/shards.json\n' + err.message);
	}
}

function shardSorter(a,b) {
	if (a.hostname < b.hostname) {
		return -1;
	} if (a.hostname > b.hostname) {
		return 1;
	} else {
		return 0;
	}
}

Database.prototype.start = function() {
	console.assert(this.flushTimerId === undefined);
}

Database.prototype.shardsChanged = function(shards) {
	var shard;
	var i;
	for(i = 0; i < shards.length; i++) {
		shard = this.shards[shards[i].hostname];
		if (shard == undefined) {
			shard = new Shard(shards[i]);
			shards[shards[i].hostname] = shard;
			// get their TOC's
		}
	}
	// phase out rest of the shards using same index
	for(i; i < this.shard_configuration.length; i++) {
		shard = this.shards[shards[i].hostname];
		shard.phaseOut(); // somehow mark that new data shouldn't be added to this shard.
	}
	// possibly rebalance
	// log.notice('[ShardServer] Shard configuration changed to ' + shards.length + ' nodes.');
	this.shard_configuration = shards;
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

Database.prototype.addToToc = function(shard, toc) {
	var group, groupName;
	var dayTimestamp;
	var timeEntry;
	shard.total_size = 0;
	for (groupName in toc.groups) {
		if(this.toc.groups[groupName] == undefined) {
			this.toc.groups[groupName] = {};
		}

		group = this.toc.groups[groupName];
		
		for(dayTimestamp in toc.groups[groupName]) {
			timeEntry = toc.groups[groupName][dayTimestamp];
			group[dayTimestamp] = timeEntry;
			timeEntry.shard = shard;
			shard.total_size += timeEntry.size;
		}
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

function looksLikeTimestamp(name) {
	return name == parseInt(name) + '';
}

exports.Database = Database;
