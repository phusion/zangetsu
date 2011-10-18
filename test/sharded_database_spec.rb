# encoding: binary
require File.expand_path(File.dirname(__FILE__) + "/spec_helper")

describe "ShardedDatabase" do
	before :all do
		@dbpath = 'tmp/db'
		@new_server_js = %Q{
			var ShardedDatabase = require('zangetsu/sharded_database');
			var database = new ShardedDatabase.Database();
		}
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

	describe "addToToc" do
		before :all do
			@output, @error = eval_js! %Q{
				#{@new_server_js}
				var toc = {
					"groups" : {
						"a" : {
							"1" : {"size" : 1},
							"2" : {"size" : 2}
						}
					}
				};
				var shard = {"name" : "shard1"};
				database.addToToc(shard, toc);
				console.log(shard.total_size);

				toc = {
					"groups" : {
						"a" : {
							"3" : {"size" : 1}
						}
					}
				};
				var shard2 = {"name" : "shard2"};
				database.addToToc(shard2, toc);
				console.log(database.toc.groups.a["2"].shard.name)
				console.log(database.toc.groups.a["3"].shard.name)
			}
		end
		it "should add a TOC from a shard to the shardserver" do
			@output.lines.to_a[0].to_i.should == 3
		end
		it "should associate the shard with every TOC entry" do
			@output.lines.to_a[1].chomp.should == "shard1"
			@output.lines.to_a[2].chomp.should == "shard2"
		end
	end

	describe "configure" do
		it "should connect to the databases listed in config/shards.json
		    and update the toc accordingly" do
			config = %Q{
				{ "shards" :
				 [
					{"hostname" : "localhost:8392"},
					{"hostname" : "localhost:8393"},
					{"hostname" : "localhost:8394"}
				 ]
				}
			}
			File.open('tmp/config_shards.json', 'w') {|f| f.write(config) }
			@output, @error = eval_js! %Q{
				var ShardedDatabase = require('zangetsu/sharded_database');
				var database = new ShardedDatabase.Database("tmp/config_shards.json");
				console.log();
			}
			pending
		end
	end
end
