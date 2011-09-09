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

//
// er moet een locks tabel zijn voor alle zangetsu instances
// er moet een lock negotiation protocol zijn
// mocht er een shardserver down gaan dan moeten de locks van die server
// moeten kunnen worden vrijgegeven


function looksLikeTimestamp(name) {
	return name == parseInt(name) + '';
}

exports.Database = Database;
