var util   = require('util');
var events = require('events');
var fs     = require('fs');

var log           = require('./default_log.js').log;
var TimeEntry     = require('./time_entry.js');
var Group         = require('./group.js');
var HashSet       = require('./hashset.js').HashSet;
var ShardConnection = require('./shard_connection.js').ShardConnection;
var ShardServerProxy = require('./shard_server_proxy.js').ShardServerProxy;

function Database(router) {
	events.EventEmitter.call(this);
	this.router = router;
	this.shardConnections = {};
	this.addingShards = {};
}

util.inherits(Database, events.EventEmitter);

Database.prototype.onConnection = function(identifier) {
	if(this.shardConnections[identifier] == undefined) {
		// create a new connection based on the router's connection
		this.shardConnections[identifier] = new ShardConnection(this.router.shards[identifier]);	
	}
	return this.shardConnections[identifier];
}

Database.prototype.findTimeEntry = function(groupName, dayTimestamp) {
	//
}

Database.prototype.get = function(groupName, dayTimestamp, offset, callback) {
	var group = this.router.toc[groupName];
	var notFound = false;
	if(group == undefined) {
		notFound = true
	} else {
		var entry = group[dayTimestamp];
		if(entry == undefined) {
			notFound = true
		} else {
			var shardId = entry.shard.identifier;
			this.onConnection(shardId).get(groupName, dayTimestamp, offset, callback);
		}
	}
	if (notFound) {
		// disconnect with error.
	}
}

Database.prototype.add = function(opid, groupName, dayTimestamp, buffers, checksumBuffer, options, callback) {
	var self = this;
	if (typeof(options) == 'function') {
		callback = options;
		options = undefined;
	}
	// look up on which shard the file is for this data
	// if the file does not yet exist
	// put data in round robin order on a shard (is random more secure?)
	// log data entry in TOC with shard id
	// key = groupname + '/' + dayTimeStamp
	var key = groupName + '/' + dayTimestamp;
	var exists = function() {
		return self.router.toc[groupName] && self.router.toc[groupName][dayTimestamp];
	}

	var add = function() {
		var size = 0;
		for(b in buffers) {
			size += buffers[b].length;
		}

		var shardId = self.router.toc[groupName][dayTimestamp].shard.identifier;
		self.onConnection(shardId).add(groupName, 3600 * 24 * dayTimestamp, opid, size, buffers, write_callback);
		// Add shard to list (set) of shards we issued add requests to
		self.addingShards[shardId] = true;
	}

	var write_callback = function(err) {
		if(err) {
			// TODO something bad happened, let client know
		} else {
			callback();
		}
	}

	if(exists()) {
		if(self.router.isLocked(groupName, dayTimestamp)) {
			// it's locked so we have to queue up
			self.router.lockObject(groupName, dayTimestamp).callbacks.push(
				function() {
					self.add(opid, groupName, dayTimestamp, buffers, checksumBuffer, options, callback);
				}
			);
		} else {
			// it exists and it isn't locked so we can just add without a problem
			add();
		}
	} else {
		self.router.lock(groupName, dayTimestamp, function(gotLock) {
			if(gotLock) {
				// we've got the lock, lets make the file
				var shardList = [];
				for(shardId in self.router.shards) {
					shardList.push(self.router.shards[shardId]);
				}
				// pick a random shard
				var shard = shardList[Math.floor(Math.random() * shardList.length)];

				self.router.toc[groupName] = {};
				self.router.toc[groupName][dayTimestamp] = {shard: shard};
				add();	
			} else {
			   // we don't own the lock, someone else got before us	
				if(exists()) {
					// the file exists, someone created it for us
					add();
				} else {
					// someone deleted the file, we must reacquire the lock before we can do anything
					self.add(opid, groupName, dayTimestamp, buffers, checksumBuffer, options, callback);
				}
			}
		});
	}
}

// Fetch the results
Database.prototype.results = function(callback) {
	var self = this;
	// call results on all relevant shards
	var results = {};
	var toGo = 0;
	for(s in self.addingShards) {
		toGo += 1;
	}

	var state = {toGo : toGo};

	var processResults = function(newResults) {
		for(opid in newResults) {
			results[opid] = newResults[opid];
		}
	}

	var done = function() {
		self.addingShards = {};
		callback(results);
	}

	for(shard in self.addingShards) {
		self.onConnection(shard).results(function(newResults) {
			processResults(newResults);
			state.toGo -= 1;
			if (state.toGo == 0) {
				done();
			}
		});
	}
}

Database.prototype.remove = function(groupName, timestamp, callback) {
	var self = this;
	this.router.lock(groupName, undefined, function(gotLock) {
		if(gotLock) {
			// we got the lock, lets remove the files by sending
			// the remove command to every shard.
			for(shard in self.router.shards) {
				self.onConnection(shard).remove(groupName, timestamp, callback);
			}
		} else {
			// we didn't get the lock, lets try again
			self.remove(groupName, timestamp, callback);
		}
	});
	// voor remove lock aanvragen op de file zodat de volgorde klopt met write acties
	callback();
}

Database.prototype.removeOne = function(groupName, dayTimestamp, callback) {
	var self = this;
	var shard = function() {
		var shardId;
		var group = self.router.toc[groupName];
		if(group != undefined) {
			var entry = group[dayTimestamp];
			if(entry != undefined) {
				shardId = entry.shard.identifier;
			}
		}
		return shardId;
	}

	if(!shard()) {
		callback(); //disconnect with error
		return;
	}
	this.router.lock(groupName, dayTimestamp, function(gotLock) {
		if(gotLock) {
			// we got the lock, lets remove the file.
			var shardId = shard();
			self.onConnection(shardId).removeOne(groupName, dayTimestamp, callback);
		} else {
			// we didn't get the lock, lets try again
			self.removeOne(groupName, dayTimestamp, callback);
		}
	});
	// voor remove lock aanvragen op de file zodat de volgorde klopt met write acties
	callback();
}

exports.Database = Database;
