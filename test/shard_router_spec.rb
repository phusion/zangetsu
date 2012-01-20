# encoding: binary
require File.expand_path(File.dirname(__FILE__) + "/spec_helper")

describe "ShardedRouter" do
	before :all do
		begin
			Dir.mkdir 'tmp'
			Dir.mkdir 'tmp/db'
		rescue
		end
		@dbpath = 'tmp/db'
	end

	describe "addToToc" do
		before :all do
			config = %Q{{ "shards" : [], "shardServers" : []}}
			File.open('tmp/config.json', 'w') {|f| f.write(config) }
			@proc = async_eval_js %Q{
				var ShardRouter = require('zangetsu/shard_router').ShardRouter;
				var database = new ShardRouter('tmp/config.json');
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
					var Router = require('zangetsu/shard_router').ShardRouter;
					var database = new Router('tmp/config.json');
					var server = {"hostname" : "aap", "port" : 1532};
					database.addShardServer(server);
					console.log(database.shardServers["aap:1532"].hostname);
					database.removeShardServer({identifier : "aap:1532"});
					console.log(database.shardServers["aap:1532"]);
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
			begin
				Dir.mkdir('tmp')
			rescue
			end
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
					var Router = require('zangetsu/shard_router').ShardRouter;
					Router.prototype.oldAddShard = Router.prototype.addShard;
					Router.prototype.addShard = function(description) {
						console.log('add');	
						this.oldAddShard(description);
					};
					Router.prototype.oldRemoveShard = Router.prototype.removeShard;
					Router.prototype.removeShard = function(shard) {
						console.log('remove');	
						this.oldRemoveShard(shard);
					};
					var database = new Router("tmp/config_1.json");
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
					var ShardRouter = require('zangetsu/shard_router').ShardRouter;
					ShardRouter.prototype.oldAddShardServer = ShardRouter.prototype.addShardServer;
					ShardRouter.prototype.addShardServer = function(description) {
						console.log('add');	
						this.oldAddShardServer(description);
					};
					ShardRouter.prototype.oldRemoveShardServer = ShardRouter.prototype.removeShardServer;
					ShardRouter.prototype.removeShardServer = function(shard) {
						console.log('remove');	
						this.oldRemoveShardServer(shard);
					};
					var database = new ShardRouter("tmp/config_1.json");
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
				var ShardRouter = require('zangetsu/shard_router').ShardRouter;
				ShardRouter.prototype.shardsChanged = function(shards) {
					console.log('shardsChanged');
					this.shardConfiguration = shards;
				}
				ShardRouter.prototype.shardServersChanged = function(servers) {
					console.log('shardServersChanged');
					this.shardServerConfiguration = servers;
				}
				var database = new ShardRouter("tmp/config_1.json");
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
