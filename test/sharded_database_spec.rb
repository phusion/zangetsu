# encoding: binary
require File.expand_path(File.dirname(__FILE__) + "/spec_helper")

describe "ShardedDatabase" do
	before :all do
		begin
			Dir.mkdir 'tmp'
			Dir.mkdir 'tmp/db'
		rescue
		end
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
			@proc = async_eval_js %Q{
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
				var shard = {"identifier" : "shard1"};
				database.addToToc(shard, toc);
				console.log(shard.total_size);

				toc = {
					"groups" : {
						"a" : {
							"3" : {"size" : 1}
						}
					}
				};
				var shard2 = {"identifier" : "shard2"};
				database.addToToc(shard2, toc);
				console.log(database.toc.groups.a["2"].shard.identifier);
				console.log(database.toc.groups.a["3"].shard.identifier);
			}
		end

		after :all do
			@proc.close if not @proc.closed?
		end

		it "should add a TOC from a shard to the shardserver" do
			eventually do
				@proc.output.lines.to_a[0].to_i == 3
			end
		end
		it "should associate the shard with every TOC entry" do
			eventually do
				@proc.output.lines.to_a[1].chomp == "shard1"
				@proc.output.lines.to_a[2].chomp == "shard2"
			end
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
				@proc = async_eval_js %Q{
					var ShardedDatabase = require('zangetsu/sharded_database');
					var database = new ShardedDatabase.Database('tmp/config.json');
					var server = {"hostname" : "aap", "port" : 1532};
					database.addShardServer(server);
					console.log(database.shardServers["aap"].hostname);
					database.removeShardServer(server);
					console.log(database.shardServers["aap"]);
				}
				eventually do
					@proc.output == "aap\nundefined\n"
				end
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
				@proc = async_eval_js %Q{
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
				eventually do
					@proc.output == "add\nadd\nadd\nremove\n"
				end
			end

			it "should add new shardServers and remove old ones" do
				@proc = async_eval_js %Q{
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
				eventually do
					@proc.output == "add\nadd\nadd\nremove\n"
				end
			end
		end

		describe "configure" do
			it "it should call shardsChanged iff the amount of shards changed" do
				@proc = async_eval_js %Q{
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
				eventually do
					@proc.output == "shardsChanged\nshardServersChanged\nshardsChanged\nshardServersChanged\n"
				end
			end
		end
	end
end
