# encoding: binary
require File.expand_path(File.dirname(__FILE__) + "/spec_helper")

describe "Distributed locks" do
	before :each do
		Dir.mkdir 'tmp'
		Dir.mkdir 'tmp/db'
		@dbpath = 'tmp/db'
	end

	before :each do
		config = %Q{{ "shards" : [], "shardServers" : []}}
		File.open('tmp/config.json', 'w') {|f| f.write(config) }
	end

	after :each do
		File.delete('tmp/config.json')
		if @proc && !@proc.closed?
			@proc.close
		end
	end

	# Design:
	# When a file needs to be changed, a node requests the lock. It will
	# do so by sending a lock request to all registered shard servers.
	# When another shard server requests the same lock at the same time
	# with it will deny the request.
	# When a shard server has acknowledged a request, but gets a request
	# with a higher priority it will acknowledge it and replace the ownership.
	# When a shard server has requested a lock but receives a lock request
	# with higher authority it will change the ownership of the lock to that
	# and block its own operation until the lock is released.
	# When a node has acknowledged a lock it may not request the lock itself
	# until the lock has been released.
	# When a node is done with a lock it will announce the release to all
	# nodes.
	# When a node crashes that holds a lock other nodes will remove the node
	# from their acknowledgement tables and release the lock.
	# When a node recuperates from a crash it will not own any locks.
	# Nodes will at intervals check wether locks are still held.
	#
	# Freedom from Deadlock: Because a higher priority node always wins when
	# multiple nodes require the same lock no deadlock can occur.
	# Mutual exclusion: Because a lock has to be acknowledged by all other nodes
	# and a node cannot request a lock it has already acknowledged there can
	# be no situation wherein two nodes think they own the same lock.
	# Freedom from starvation: Not guaranteed for low-priority nodes. Can be
	# improved by adding a random (time-based) value to the priority.
	#
	# Correctness: No effort will be made to correct any incomplete actions
	# executed by zangetsu.

	describe "giveLock" do
		it "should acknowledge and register the lock" do
			@proc = async_eval_js %Q{
					var ShardedDatabase = require('zangetsu/sharded_database');
					var database = new ShardedDatabase.Database('tmp/config.json');
					var otherServer = {
						identifier: 'otherServer',
						giveLock: function(key) {
							console.log("locked");
						}
					}
					database.shardServers.otherServer = otherServer;
					database.giveLock(otherServer.identifier, "group", 1);
					console.log(database.lockTable["group/1"].identifier == otherServer.identifier);
			}
			eventually do
				@proc.output == "locked\ntrue\n"
			end
		end

		it "should deny when higher ranked and also requesting lock" do
			@proc = async_eval_js %Q{
					var ShardedDatabase = require('zangetsu/sharded_database');
					var database = new ShardedDatabase.Database('tmp/config.json');
					database.priority = 1;
					var otherServer = {
						identifier: 'otherServer',
						giveLock: function(key) {
							console.log("locked");
						},
						priority: 0
					}
					database.shardServers.otherServer = otherServer;
					database.shardServers[database.identifier] = database;
					database.lockTable["group/1"] = {identifier: database.identifier, callbacks: []};
					database.giveLock(otherServer.identifier, "group", 1);
					console.log(database.lockTable["group/1"].identifier == otherServer.identifier);
			}
			eventually do
				@proc.output == "false\n"
			end
		end

		it "should update lockTable with higher ranked server" do
			@proc = async_eval_js %Q{
					var ShardedDatabase = require('zangetsu/sharded_database');
					var database = new ShardedDatabase.Database('tmp/config.json');
					database.priority = 0;
					var otherServer = {
						identifier: 'otherServer',
						giveLock: function(key) {
							console.log("locked");
						},
						priority: 1
					}
					database.shardServers.otherServer = otherServer;
					database.shardServers[database.identifier] = database;
					database.lockTable["group/1"] = {identifier: database.identifier, callbacks: []};
					database.giveLock(otherServer.identifier, "group", 1);
					console.log(database.lockTable["group/1"].identifier == otherServer.identifier);
			}
			eventually do
				@proc.output == "locked\ntrue\n"
			end
		end
	end

	describe "releaseLock" do
		it "should release a lock the shardserver owns and execute all callbacks" do
			@proc = async_eval_js %Q{
					var ShardedDatabase = require('zangetsu/sharded_database');
					var database = new ShardedDatabase.Database('tmp/config.json');
					database.lockTable["group/1"] = {identifier: database.identifier, callbacks: [function() {
						console.log('callback');
					}, function() {
						console.log('callback');
					}]};
					database.releaseLock("group", 1);
					console.log(database.lockTable["group/1"]  == undefined);
			}
			eventually do
				@proc.output == "callback\ncallback\ntrue\n"
			end
		end
	end

	describe "lock" do
		it "should request all nodes for a lock on a file" do
			@proc = async_eval_js %Q{
					var ShardedDatabase = require('zangetsu/sharded_database');
					var database = new ShardedDatabase.Database('tmp/config.json');
					var otherServer = {
						identifier: 'otherServer',
						lock: function(key) {
							console.log("lock");
						},
						priority: 1
					}
					var otherServer2 = {
						identifier: 'otherServer2',
						lock: function(key) {
							console.log("lock");
						},
						priority: 2
					}
					database.shardServers.otherServer = otherServer;
					database.shardServers.otherServer2 = otherServer2;
					database.priority = 0;
					database.lock("group", 1, function(){});
					console.log(database.lockTable["group/1"].identifier  == database.identifier);
			}
			eventually do
				@proc.output == "lock\nlock\ntrue\n"
			end
		end

		it "should not lock if the file is already locked" do
			@proc = async_eval_js %Q{
					var ShardedDatabase = require('zangetsu/sharded_database');
					var database = new ShardedDatabase.Database('tmp/config.json');
					database.lockTable["group/1"] = {identifier: 'otherServer', callbacks: []};
					database.lock("group", 1, function(){ console.log("callback");});
					console.log(database.lockTable["group/1"].identifier  == database.identifier);
					database.lockTable["group/1"].callbacks[0]();
			}
			eventually do
				@proc.output == "false\ncallback\n"
			end
		end

		it "should save the lock time with the lock" do
			@proc = async_eval_js %Q{
					var ShardedDatabase = require('zangetsu/sharded_database');
					var database = new ShardedDatabase.Database('tmp/config.json');
					var otherServer = {
						identifier: 'otherServer',
						lock: function(key) {
						},
						priority: 1
					}
					var otherServer2 = {
						identifier: 'otherServer2',
						lock: function(key) {
						},
						priority: 2
					}
					database.shardServers.otherServer = otherServer;
					database.shardServers.otherServer2 = otherServer2;
					database.priority = 0;
					database.lock("group", 1, function(){});
					var currentTime = new Date().getTime();
					console.log(database.lockTable["group/1"].time);
					console.log(currentTime);
			}
			first, second = [false, false]
			eventually do
				first, second = @proc.output.split("\n").map(&:to_i)
				first and second
			end
			first.should be_close(second, 10)
		end

	end

	describe "monitorLocks" do
		it "it should query shards for old locks" do
			@proc = async_eval_js %Q{
					var ShardedDatabase = require('zangetsu/sharded_database');
					var database = new ShardedDatabase.Database('tmp/config.json');
					var otherServer = {
						identifier: 'otherServer',
						listLocks: function(key) {
							console.log('otherServer');
						},
						priority: 1
					}
					var otherServer2 = {
						identifier: 'otherServer2',
						listLocks: function(key) {
							console.log('otherServer2');
						},
						priority: 2
					}
					database.shardServers.otherServer = otherServer;
					database.shardServers.otherServer2 = otherServer2;
					database.priority = 0;
					var currentTime = new Date().getTime();
					database.lockTable["group/1"] = {
						identifier: 'otherServer',
						time: currentTime - 30000
					};
					database.lockTable["group/2"] = {
						identifier: 'otherServer',
						time: currentTime
					};
					database.lockTable["group/3"] = {
						identifier: 'otherServer',
						time: currentTime - 20000
					};
					database.monitorLocks();
			}
			eventually do
				@proc.output == "otherServer\n"
			end
		end

		 it "should releaseLocks on given-up locks" do
			@proc = async_eval_js %Q{
					var ShardedDatabase = require('zangetsu/sharded_database');
					var database = new ShardedDatabase.Database('tmp/config.json');
					var otherServer = {
						identifier: 'otherServer',
						listLocks: function(callback) {
							callback(["group/1"]);
						},
						priority: 1
					}
					var otherServer2 = {
						identifier: 'otherServer2',
						listLocks: function(callback) {
							callback(["group/2"]);
						},
						priority: 2
					}
					database.shardServers.otherServer = otherServer;
					database.shardServers.otherServer2 = otherServer2;
					database.priority = 0;
					var currentTime = new Date().getTime();
					database.lockTable["group/1"] = {
						identifier: 'otherServer',
						time: currentTime - 30000
					};
					database.lockTable["group/2"] = {
						identifier: 'otherServer2',
						time: currentTime - 30000
					};
					database.lockTable["group/3"] = {
						identifier: 'otherServer',
						time: currentTime - 30000
					};
					database.releaseLock = function(group, key) {
						console.log(group + '/' + key);
					}
					database.monitorLocks();
			}
			eventually do
				@proc.output == "group/3\n"
			end
		 end
	end

	describe "receiveLock and unLock" do
		it "should run the first callback and then release the lock" do
			@proc = async_eval_js %Q{
					var ShardedDatabase = require('zangetsu/sharded_database');
					var database = new ShardedDatabase.Database('tmp/config.json');

					var callback = function() {
						console.log("executed");	
						return true;
					}

					database.lockTable["group/1"] = {identifier: database.identifier, callbacks: [callback], affirmed: []};

					var notify = function(k, result) {
						console.log(result);
					}

					var otherServer = { identifier: 'otherServer', releaseLock: notify }
					var otherServer2 = { identifier: 'otherServer2', releaseLock: notify }
					database.shardServers.otherServer = otherServer;
					database.shardServers.otherServer2 = otherServer2;

					database.receiveLock('otherServer', "group", 1);
					console.log(database.lockTable["group/1"].affirmed[0]);
					database.receiveLock('otherServer2', "group", 1);
			}
			eventually do
				@proc.output == "otherServer\nexecuted\ntrue\ntrue\n"
			end
		end
	end

	describe "listLocks" do
		it "should ask wether it still lays claim to a lock" do
			@proc = async_eval_js %Q{
					var ShardedDatabase = require('zangetsu/sharded_database');
					var database = new ShardedDatabase.Database('tmp/config.json');
					database.lockTable["group/1"] = {identifier: database.identifier};
					database.shardServers["otherServer"] = { replyLocks: function(list) {
						console.log(list[0]);
					}};
					database.listLocks("otherServer");
			}
			eventually do
				@proc.output == "group/1\n"
			end
		end
	end
end


