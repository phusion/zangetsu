var util   = require('util');
var events = require('events');
var fs     = require('fs');

var log           = require('./default_log.js').log;
var TimeEntry     = require('./time_entry.js');
var Group         = require('./group.js');
var HashSet       = require('./hashset.js').HashSet;
var Shard         = require('./shard.js').Shard;
var ShardServerProxy = require('./shard_server_proxy.js').ShardServerProxy;

function Database(configFile, hostname) {
	events.EventEmitter.call(this);

	this.groups = {};
	this.shardConfiguration = [];
	this.shardServerConfiguration = [];
	this.shards = {};
	this.shardServers = {};
	this.shardServersPhasingOut = {};
	this.shardsPhasingOut = {};
	this.toc = {};
	this.toc.groups = {};
	this.lockTable = {};


	if(configFile == undefined) {
		this.configFile = "./config/shards.json";
	} else {
		this.configFile = configFile;
	}

	if(hostname == undefined) {
		this.hostname = "localhost";
	} else {
		this.hostname = hostname;
	}

	this.configure();

	var database = this;

	// Monitor shard configuration
	//fs.watchFile("./config/shards.json", {}, function() { database.configure()});
}

util.inherits(Database, events.EventEmitter);

Database.prototype.configure = function() {
	try {
		var fileContents = fs.readFileSync(this.configFile, 'utf8');
		var newConfig = JSON.parse(fileContents);
		var oldShardsString = JSON.stringify(this.shardConfiguration);
		var newShards = newConfig.shards.sort(shardSorter);
		var newShardsString = JSON.stringify(newShards);
		if (oldShardsString !== newShardsString) {
			this.shardsChanged(newShards);
		}
		var oldShardServersString = JSON.stringify(this.shardServerConfiguration);
		var newShardServers = newConfig.shardServers.sort(shardSorter);
		var newShardServersString = JSON.stringify(newShardServers);
		if (oldShardServersString !== newShardServersString) {
			this.shardServersChanged(newShardServers);
		}
	} catch(err) {
		log.notice('[ShardServer] Could not read ' + this.configFile + "\n" + err.message);
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
			this.addShard(shards[i]);
		}
	}
	// phase out rest of the shards using same index
	for(i; i < this.shardConfiguration.length; i++) {
		shard = this.shards[this.shardConfiguration[i].hostname];
		this.removeShard(shard);
	}
	// possibly rebalance
	this.shardConfiguration = shards;
}

Database.prototype.shardServersChanged = function(servers) {
	var server;
	var i;
	for(i = 0; i < servers.length; i++) {
		server = this.shardServers[servers[i].hostname];
		if (server == undefined) {
			this.addShardServer(servers[i]);
		}
	}
	// phase out rest of the servers using same index
	for(i; i < this.shardServerConfiguration.length; i++) {
		server = this.shardServers[this.shardServerConfiguration[i].hostname];
		this.removeShardServer(server);
	}
	// possibly rebalance
	this.shardServerConfiguration = servers;
}

Database.prototype.addShardServer = function(description) {
	var server = new ShardServerProxy(description);
	this.shardServers[description.hostname] = server;
	// get their TOC's
}

Database.prototype.removeShardServer = function(server) {
	delete this.shardServers[server.hostname];
	this.shardServersPhasingOut[server.hostname] = server;
	//server.phaseOut();
}

Database.prototype.addShard = function(description) {
	var shard = new Shard(description);
	this.shards[description.hostname] = shard;
	// get their TOC's
}

Database.prototype.removeShard = function(shard) {
	delete this.shards[shard.hostname];
	this.shardsPhasingOut[shard.hostname] = shard;
	shard.phaseOut();
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
	//  vraag lock aan alle shardservers
	//   shard = hash(key) % shards
	//   shard.add data
	//   TOC[key].shard = shard
	//   release locks aan shardservers
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

Database.prototype.acquireLock = function(group, dayTimestamp, callback) {
	var key = group + '/' + dayTimestamp;
	if(this.lockTable[key] == undefined) {
		this.lockTable[key] = {hostname: this.hostname, callbacks: [callback]};
		for(hostname in this.shardServers) {
			this.shardServers[hostname].acquireLock(group, dayTimestamp);
		}	
	} else {
		this.lockTable[key].callbacks.push(callback);
	}
}

Database.prototype.giveLock = function(group, dayTimestamp) {
	
}

Database.prototype.releaseLock = function(group, dayTimestamp) {
	
}

function looksLikeTimestamp(name) {
	return name == parseInt(name) + '';
}

exports.Database = Database;
