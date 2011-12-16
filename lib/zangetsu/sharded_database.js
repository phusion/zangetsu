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
	// lookup shard id('s) in TOC
	// forward to shard
	callback('not-found');
}

Database.prototype.add = function(groupName, dayTimestamp, buffers, checksumBuffer, options, callback) {
	var self = this;
	// look up on which shard the file is for this data
	// if the file does not yet exist
	// put data in round robin order on a shard (is random more secure?)
	// log data entry in TOC with shard id
	// key = groupname + '/' + dayTimeStamp
	var key = groupName + '/' + dayTimestamp;
	var exists = function() {
		return self.toc[groupName] && self.toc[groupName][dayTimestamp];
	}

	var add = function() {
		var opid = self.nextOpid();
		var size = 11;
		var shardId = self.toc[groupName][dayTimestamp].shard.identifier;
		self.onConnection(shardId).add(groupName, 3600 * 24 * dayTimestamp, opid, size, buffers, write_callback);
		// Add shard to list (set) of shards we issued add requests to
		self.addingShards[shardId] = true;
	}

	var write_callback = function(err) {
		if(err) {
			// TODO something bad happened, let client know
		}
	}

	if(exists()) {
		// it exists so we can just add without a problem
		add();
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
					self.add(groupName, dayTimestamp, buffers, checksumBuffer, options, callback);
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
	var state = {toGo : self.addingShards.length};

	for(shard in self.addingShards) {
		shard.results(function(newResults) {
			processResults(newResults);
			state.toGo -= 1;
			if (state.toGo == 0) {
				done();
			}
		});
	}

	var processResults = function(newResults) {
		for(opid in newResults) {
			results[opid] = newResults[opid];
		}
	}

	var done = function() {
		self.addingShards = [];
		callback(results);
	}
}

Database.prototype.remove = function(groupName, dayTimestamp, callback) {
	// voor remove lock aanvragen op de file zodat de volgorde klopt met write acties
	// opletten dat locks niet gehouden worden door gecrashte instances
	callback();
}

Database.prototype.removeOne = function(groupName, dayTimestamp, callback) {
	callback();
}

exports.Database = Database;
