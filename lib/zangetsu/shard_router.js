var util   = require('util');
var events = require('events');
var fs     = require('fs');

var log           = require('./default_log.js').log;
var TimeEntry     = require('./time_entry.js');
var Group         = require('./group.js');
var HashSet       = require('./hashset.js').HashSet;
var ShardConnection = require('./shard_connection.js').ShardConnection;
var ShardServerProxy = require('./shard_server_proxy.js').ShardServerProxy;

function Router(configFile) {
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
	this.working = [];
	this.workId = -1;
	this.waiting = [];

	if(configFile == undefined) {
		this.configFile = "./config/shards.json";
	} else {
		this.configFile = configFile;
	}

	this.configure();
	this.monitorLocks();
}

Router.prototype.start = function(host, portname) {
	this.identifier = host + ":" + portname;
}

Router.prototype.configure = function() {
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

Router.prototype.getWorkKey = function() {
	this.workId += 1;
	this.working.push(this.workId);
	return this.workId;
}

Router.prototype.doneWorking = function(key) {
	var index = this.working.indexOf(key);
	if(index == -1) { return; } // double doneWorking call?
	this.working.splice(key, key);

	var waiter;
	var newWaiting = [];
	for(w in this.waiting) {
		waiter = this.waiting[w];
		if(waiter.waitingFor.indexOf(key) > -1) {
			waiter.waitingFor.splice(key, key);
		}
		if(waiter.waitingFor.length == 0) {
			waiter.callback();
		} else {
			newWaiting.push(waiter);
		}
	}
	this.waiting = newWaiting;
}

Router.prototype.whenWorkingFinished = function(callback) {
	if(this.working.length == 0) {
		callback();
	} else {
		this.waiting.push({ waitingFor: this.working.splice(), callback: callback});
	}
}

function shardSorter(a,b) {
	a.identifier = a.hostname + ':' + a.port;
	b.identifier = b.hostname + ':' + b.port;
	if (a.identifier < b.identifier) {
		return -1;
	} if (a.identifier > b.identifier) {
		return 1;
	} else {
		return 0;
	}
}

// If a lock is held too long query if the lock is still needed,
// if not, release the lock.
Router.prototype.monitorLocks = function() {
	var server;
	var listLocksMemo = {};
	var currentTime = new Date().getTime();

	var self = this;
	var checkRelease = function(list, key) {
		var found = false;
		for(item in list) {
			if (list[item] == key) {
				found = true;
			}
		}
		if(!found) {
			key = key.split('/');
			self.releaseLock(key[0], key[1]);
		}
	}

	for(key in this.lockTable) {
		// if the lock is held for over 20 seconds
		if(currentTime - this.lockTable[key].time > 20000) {
			server = this.lockTable[key].identifier;
			// if memoized, then check the memo
			if(listLocksMemo[server]) {
				checkRelease(listLocksMemo[server], key);
			} else {
				// query the server, and then check the resulting locks
				this.shardServers[server].listLocks(function(locks) {
					listLocksMemo[server] = locks;
					checkRelease(locks, key);
				});
			}
		}
	}
	var self = this;
	setTimeout(function() {self.monitorLocks()}, 2000);
}

Router.prototype.shardsChanged = function(shards) {
	var shard;
	var i;
	for(i = 0; i < shards.length; i++) {
		shard = this.shards[shards[i].identifier];
		if (shard == undefined) {
			this.addShard(shards[i]);
		}
	}
	// phase out rest of the shards using same index
	for(i; i < this.shardConfiguration.length; i++) {
		shard = this.shards[this.shardConfiguration[i].identifier];
		this.removeShard(shard);
	}
	// possibly rebalance
	this.shardConfiguration = shards;
}

Router.prototype.shardServersChanged = function(servers) {
	var server;
	var i;
	for(i = 0; i < servers.length; i++) {
		server = this.shardServers[servers[i].identifier];
		if (server == undefined) {
			this.addShardServer(servers[i]);
		}
	}
	// phase out rest of the servers using same index
	for(i; i < this.shardServerConfiguration.length; i++) {
		server = this.shardServers[this.shardServerConfiguration[i].identifier];
		this.removeShardServer(server);
	}
	// possibly rebalance
	this.shardServerConfiguration = servers;
}

Router.prototype.addShardServer = function(description) {
	var server = new ShardServerProxy(description);
	this.shardServers[server.identifier] = server;
	// get their TOC's
}

Router.prototype.removeShardServer = function(server) {
	delete this.shardServers[server.identifier];
	this.shardServersPhasingOut[server.identifier] = server;
	//server.phaseOut();
}

Router.prototype.addShard = function(description) {
	var shard = new ShardConnection(description);
	this.shards[description.identifier] = shard;
	// get their TOC's
}

Router.prototype.removeShard = function(shard) {
	delete this.shards[shard.identifier];
	this.shardsPhasingOut[shard.identifier] = shard;
	shard.phaseOut();
}

Router.prototype.rebalance = function(callback) {
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

Router.prototype.addToToc = function(shard, toc) {
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

Router.prototype.toTocFormat = function() {
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


// Returns wether a file is locked
Router.prototype.isLocked = function(group, dayTimestamp) {
	var key = group;
	if(dayTimestamp) {
		key = group + '/' + dayTimestamp;
	}
	return this.lockTable[group] != undefined || this.lockTable[key] != undefined;
}

// Returns the lock object
Router.prototype.lockObject = function(group, dayTimestamp) {
	var key = group;
	if(dayTimestamp) {
		key = group + '/' + dayTimestamp;
	}
	var owner = this.lockTable[group];
	if(!owner) {
		owner = this.lockTable[key];
	}
	return owner;
}

// Other shard router asks for a lock
Router.prototype.giveLock = function(identifier, group, dayTimestamp) {
	var self = this;
	var key = group;
	if(dayTimestamp) {
		key = group + '/' + dayTimestamp;
	}
	if(this.isLocked(group, dayTimestamp)) {
		var otherPriority = this.shardServers[this.lockObject(group, dayTimestamp).identifier].priority;
		if (otherPriority > this.shardServers[identifier].priority) {
			return; // Deny request
		} else {
			this.lockObject(group, dayTimestamp).identifier = identifier;
		}
	} else {
		this.lockTable[key] = {identifier: identifier, callbacks: []};
	}
	this.whenWorkingFinished(function() {
		self.shardServers[identifier].giveLock(key);
	});
}

Router.prototype.releaseLock = function(group, dayTimestamp, result) {
	var self = this;
	var key = group;
	if(dayTimestamp) {
		key = group + '/' + dayTimestamp;
	}
	if(result) {
		switch(result.command) {
			case "add":
				var shard = self.shards[result.shard];
				var timeEntry = {shard: shard};
				if(self.toc[result.group] == undefined) {
					self.toc[result.group] = {};
				}
				self.toc[result.group][result.dayTimestamp] = timeEntry;
				break;
			case "remove":
				if(result.timestamp) {
					var group = self.toc[result.group];
					for(timeStamp in group) {
						if(timeStamp < (result.timestamp / 60 / 60 / 24)) {
							delete group[timeStamp];
						}
					}
				} else {
					delete self.toc[result.group];
				}
				break;
			case "removeOne":
				delete self.toc[result.group][result.dayTimestamp];
				break;
		}
	}
	if(this.lockTable[key] == undefined) { return; } // out of order?
	for(c in this.lockTable[key].callbacks) {
		this.lockTable[key].callbacks[c]();
	}
	delete this.lockTable[key];
}

Router.prototype.lock = function(group, dayTimestamp, callback) {
	var self = this;
	// It should not lock if the file is already locked
	if(this.isLocked(group, dayTimestamp)) {
		this.lockObject(group, dayTimestamp).callbacks.push(callback);
	} else {
		var key = group;
		if(dayTimestamp) {
			key = group + '/' + dayTimestamp;
		}
		this.lockTable[key] = {identifier: this.identifier,
			                   callbacks: [callback],
							   affirmed: [],
							   time: new Date().getTime()
		};
		// it should request all nodes for a lock on the file
		for(identifier in this.shardServers) {
			this.shardServers[identifier].lock(group, dayTimestamp);
		}
		this.whenWorkingFinished(function() {
			self.receiveLock(self.identifier, group, dayTimestamp);
		});
	}
}

// Shard router gives us a lock
Router.prototype.receiveLock = function(identifier, group, dayTimestamp) {
	var key = group;
	if(dayTimestamp) {
		key = group + '/' + dayTimestamp;
	}
	if(this.lockTable[key] == undefined) { return; } // out of order?
	var affirmed = this.lockTable[key].affirmed;
	affirmed.push(identifier);
	var done = true;
	for(host in this.shardServers) {
		done = done && (affirmed.indexOf(host) > -1);
	}
	if(done) {
		// We got the lock so we get to execute the callback!
		this.unlock(group, dayTimestamp, this.lockTable[key].callbacks.shift()(true));
	}
}

Router.prototype.listLocks = function(identifier) {
	var locks = [];
	for(k in this.lockTable) {
		if(this.lockTable[k].identifier == this.identifier) {
			locks.push(k);
		}
	}
	this.shardServers[identifier].replyLocks(locks);
}

Router.prototype.unlock = function(group, dayTimestamp, result) {
	var key = group;
	if(dayTimestamp) {
		key = group + '/' + dayTimestamp;
	}
	for(h in this.shardServers) {
		this.shardServers[h].releaseLock(key, result);
	}
	this.releaseLock(group, dayTimestamp, result);
}

exports.ShardRouter = Router;
