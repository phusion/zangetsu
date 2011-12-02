# encoding: binary
require File.expand_path(File.dirname(__FILE__) + "/spec_helper")

describe "ShardedDatabase" do
	before :all do
		Dir.mkdir 'tmp'
		Dir.mkdir 'tmp/db'
		@dbpath = 'tmp/db'
	end

	def initialize_remote
		FileUtils.mkdir_p(@dbpath)
		@shard_socket = TCPServer.new('127.0.0.1', TEST_SERVER_PORT)
		@shard_socket.listen(50)
		@shard_socket.fcntl(Fcntl::F_SETFL, @shard_socket.fcntl(Fcntl::F_GETFL) | Fcntl::O_NONBLOCK)
		@shard_code = %Q{
				var Server = require('zangetsu/server').Server;
				var server = new Server("tmp/db");
				server.startAsMasterWithFD(#{@shard_socket.fileno});
		}
		@shard = async_eval_js(@shard_code, :capture => true)
		@connection = TCPSocket.new('127.0.0.1', TEST_SERVER_PORT)
		@connection.sync = true
	end

	def finalize_remote
		@connection.close if @connection
		if @shard && !@shard.closed?
			@shard.close
		end
		@shard_socket.close if @shard_socket
	end

	before :each do
		@shard_code = %Q{
			var Shard = require('zangetsu/shard').Shard;
			var ioutils = require('zangetsu/io_utils');
			var shard = new Shard(
				{
					hostname : '127.0.0.1',
					port: #{TEST_SERVER_PORT}
				}
			);
		}
		initialize_remote
	end

	after :each do
		finalize_remote
		if @proc && !@proc.closed?
			@proc.close
		end
	end

	describe "distributed locking" do
		describe "acquireLock" do
			before :each do
				config = %Q{{ "shards" : [], "shardServers" : []}}
				File.open('tmp/config.json', 'w') {|f| f.write(config) }
			end

			after :each do
				File.delete('tmp/config.json')
			end

			it "should ask other shardservers for the lock" do
				@output, @error = eval_js! %Q{
					var ShardedDatabase = require('zangetsu/sharded_database');
					var database = new ShardedDatabase.Database('tmp/config.json');
					database.addShardServer({hostname: 'aap', port: 5321});
					database.addShardServer({hostname: 'noot', port: 5321});
					database.addShardServer({hostname: 'mies', port: 5321});
					var proto = database.shardServers.aap.constructor.prototype;
					proto.acquireLock = function(group, key) {
						console.log("acquireLock");
					}
					database.acquireLock("group", 1);
					console.log(database.lockTable["group" + '/' + 1].hostname);
				}
				@output.should == "acquireLock\nacquireLock\nacquireLock\nlocalhost\n"
			end

			it "should not ask for locks for existing lock but queue callback" do
				@output, @error = eval_js! %Q{
					var ShardedDatabase = require('zangetsu/sharded_database');
					var database = new ShardedDatabase.Database('tmp/config.json');
					database.addShardServer({hostname: 'aap', port: 5321});
					var proto = database.shardServers.aap.constructor.prototype;
					proto.acquireLock = function(group, key) {
						console.log("acquireLock");
					}
					database.lockTable["group/1"] = {hostname : "noot", callbacks : [function(){}]}
					database.acquireLock("group", 1, function(){});
					console.log(database.lockTable["group/1"].callbacks.length);
				}
				@output.should == "2\n"
			end
		end

		# TODO possible issue:
		#   since locks are acquired when new files are added, and new files get added
		#   when new days start, all locks acquires will rougly be at the same time..
		#
		# TODO
		#	Rekening houden met dat shardservers down kunnen gaan
		#
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
			before :each do
				config = %Q{{ "shards" : [], "shardServers" : []}}
				File.open('tmp/config.json', 'w') {|f| f.write(config) }
			end

			after :each do
				File.delete('tmp/config.json')
			end

			it "should acknowledge and register the lock" do
				@output, @error = eval_js! %Q{
					var ShardedDatabase = require('zangetsu/sharded_database');
					var database = new ShardedDatabase.Database('tmp/config.json');
					database.giveLock("group", 1);
				}
			end
			it "should deny when higher ranked and also requesting lock"
			it "should update lockTable with higher ranked server"
		end

		describe "releaseLock" do
			it "should release a lock the shardserver owns and execute all callbacks"
		end

		describe "lock" do
			it "should request all nodes for a lock on a file"
			it "should always work when two shard servers request the same lock"
			it "should call unlock afterwards"
			it "should even call unlock when it has crashed"
		end

		describe "unlock" do
			it "should tell all nodes that the lock has been released"
			it "should work when it crashes in the middle of unlocking"
		end
	end


	describe "get" do
		it "should forward the call to the right database"
	end

	describe "add" do
		it "should forward the add to the right database if the file exists"
		it "should forward the add to a chosen database if the file does not exist"
		it "should lock the file before it is created"
	end
	
	describe "remove" do
		it "should forward the remove call to the right database"
		it "should lock the file before it is removed"
	end

	describe "rebalance" do
		it "should move data from oversaturated databases to undersaturated databases"
		it "should lock the file before it is moved"
	end

	describe "addToToc" do
		before :all do
			config = %Q{{ "shards" : [], "shardServers" : []}}
			File.open('tmp/config.json', 'w') {|f| f.write(config) }
			@output, @error = eval_js! %Q{
				var ShardedDatabase = require('zangetsu/sharded_database');
				var database = new ShardedDatabase.Database('tmp/config.json');
				var toc = {
					"groups" : {
						"a" : {
							"1" : {"size" : 1},
							"2" : {"size" : 2}
						}
					}
				};
				var shard = {"hostname" : "shard1"};
				database.addToToc(shard, toc);
				console.log(shard.total_size);

				toc = {
					"groups" : {
						"a" : {
							"3" : {"size" : 1}
						}
					}
				};
				var shard2 = {"hostname" : "shard2"};
				database.addToToc(shard2, toc);
				console.log(database.toc.groups.a["2"].shard.hostname);
				console.log(database.toc.groups.a["3"].shard.hostname);
			}
			File.delete('tmp/config.json')
		end
		it "should add a TOC from a shard to the shardserver" do
			@output.lines.to_a[0].to_i.should == 3
		end
		it "should associate the shard with every TOC entry" do
			@output.lines.to_a[1].chomp.should == "shard1"
			@output.lines.to_a[2].chomp.should == "shard2"
		end
	end

	describe "adding and removing shards" do
		describe "addShard" do
			it "the added shard's toc should be requested and added"
		end

		describe "removeShard" do
			it "should be decided what to do with it"
		end
	end

	describe "adding and removing shardservers" do
		describe "addShardServer and removeShardServer" do
			it "should add/remove the server to its list of shardservers" do
				config = %Q{{ "shards" : [], "shardServers" : []}}
				File.open('tmp/config.json', 'w') {|f| f.write(config) }
				@output, @error = eval_js! %Q{
					var ShardedDatabase = require('zangetsu/sharded_database');
					var database = new ShardedDatabase.Database('tmp/config.json');
					var server = {"hostname" : "aap", "port" : 1532};
					database.addShardServer(server);
					console.log(database.shardServers["aap"].hostname);
					database.removeShardServer(server);
					console.log(database.shardServers["aap"]);
				}
				@output.should == "aap\nundefined\n"
				File.delete('tmp/config.json')
			end
		end

		describe "removeShardServer" do
			it "should do things regarding locks"
		end
	end

	describe "changing shards" do
		before :each do
			config = %Q{
				{ "shards" :
				 [
					{"hostname" : "first", "port" : 8393},
					{"hostname" : "second", "port" : 8394}
				 ],
				  "shardServers" :
				 [
					{"hostname" : "firstS", "port" : 8393},
					{"hostname" : "secondS", "port" : 8394}
				 ]
				}
			}
			config_2 = %Q{
				{ "shards" :
				 [
					{"hostname" : "third", "port" : 8392},
					{"hostname" : "first", "port" : 8393},
					{"hostname" : "second", "port" : 8394}
				 ],
				  "shardServers" :
				 [
					{"hostname" : "thirdS", "port" : 8392},
					{"hostname" : "firstS", "port" : 8393},
					{"hostname" : "secondS", "port" : 8394}
				 ]
				}
			}
			File.open('tmp/config_1.json', 'w') {|f| f.write(config) }
			File.open('tmp/config_2.json', 'w') {|f| f.write(config_2) }
		end

		after :each do
			File.delete('tmp/config_1.json');
			File.delete('tmp/config_2.json');
		end

		describe "shardsChanged & shardServersChanged" do
			it "should add new shards and remove old ones" do
				@output, @error = eval_js! %Q{
					var ShardedDatabase = require('zangetsu/sharded_database').Database;
					ShardedDatabase.prototype.oldAddShard = ShardedDatabase.prototype.addShard;
					ShardedDatabase.prototype.addShard = function(description) {
						console.log('add');	
						this.oldAddShard(description);
					};
					ShardedDatabase.prototype.oldRemoveShard = ShardedDatabase.prototype.removeShard;
					ShardedDatabase.prototype.removeShard = function(shard) {
						console.log('remove');	
						this.oldRemoveShard(shard);
					};
					var database = new ShardedDatabase("tmp/config_1.json");
					database.configFile = "tmp/config_2.json";
					database.configure();
					database.configFile = "tmp/config_1.json";
					database.configure();
				}
				@output.should == "add\nadd\nadd\nremove\n"
			end

			it "should add new shardServers and remove old ones" do
				@output, @error = eval_js! %Q{
					var ShardedDatabase = require('zangetsu/sharded_database').Database;
					ShardedDatabase.prototype.oldAddShardServer = ShardedDatabase.prototype.addShardServer;
					ShardedDatabase.prototype.addShardServer = function(description) {
						console.log('add');	
						this.oldAddShardServer(description);
					};
					ShardedDatabase.prototype.oldRemoveShardServer = ShardedDatabase.prototype.removeShardServer;
					ShardedDatabase.prototype.removeShardServer = function(shard) {
						console.log('remove');	
						this.oldRemoveShardServer(shard);
					};
					var database = new ShardedDatabase("tmp/config_1.json");
					database.configFile = "tmp/config_2.json";
					database.configure();
					database.configFile = "tmp/config_1.json";
					database.configure();
				}
				@output.should == "add\nadd\nadd\nremove\n"
			end
		end

		describe "configure" do
			it "it should call shardsChanged iff the amount of shards changed" do
				@output, @error = eval_js! %Q{
				var ShardedDatabase = require('zangetsu/sharded_database').Database;
				ShardedDatabase.prototype.shardsChanged = function(shards) {
					console.log('shardsChanged');
					this.shardConfiguration = shards;
				}
				ShardedDatabase.prototype.shardServersChanged = function(servers) {
					console.log('shardServersChanged');
					this.shardServerConfiguration = servers;
				}
				var database = new ShardedDatabase("tmp/config_1.json");
				database.configure(); // should not have changed
				database.configFile = "tmp/config_2.json";
				database.configure(); // should have changed
				}
				@output.should == "shardsChanged\nshardServersChanged\nshardsChanged\nshardServersChanged\n"
			end
		end
	end
end
