# encoding: binary
require File.expand_path(File.dirname(__FILE__) + "/spec_helper")

describe "ShardedDatabase" do
	after :each do
		if @proc && !@proc.closed?
			@proc.close
		end
	end

	describe "get" do
		it "should forward the call to the right connection" do
			@proc = async_eval_js %Q{
				var Database = require ('zangetsu/sharded_database').Database;
				var router = {
					"toc" : {"a" : { 1 : {
						"shard" : { "identifier" : "one" }
					}  }}
				};
				var database = new Database(router);
				database.shardConnections["one"] = {"get" : function(a,b,c, call) { call();}};
				database.get("a", 1, 1, function() { console.log('done'); });
			}
			eventually do
				@proc.output.include? 'done'
			end
		end

		it "should disconnect with an error when not found, like single servers do"
	end

	describe "add" do
		it "should forward the add to the right database if the file exists" do
			@proc = async_eval_js %Q{
				var Database = require ('zangetsu/sharded_database').Database;
				var router = {
					"toc" : {"a" : { 1 : {
						"shard" : { "identifier" : "one" }
					}  }},
					"isLocked" : function(group, stamp) { return false; }
				};
				var database = new Database(router);
				database.shardConnections["one"] = {"add" : function(g,t,o,s,b, call) { call();}};
				database.add("wah", "a", 1, [], 1, function() { console.log('done'); });
			}
			eventually do
				@proc.output.include? 'done'
			end
		end

		it "should lock the file before it is created" do
			@proc = async_eval_js %Q{
				var Database = require ('zangetsu/sharded_database').Database;
				var router = {
					"toc" : {"a" : { 1 : {
						"shard" : { "identifier" : "one" }
					}  }},
					"lock" : function(g,t,f) {
						console.log('locked');
						f(true);
					},
					"shards" : [
						{ "identifier" : "one" }
					]
				};
				var database = new Database(router);
				database.shardConnections["one"] = {"add" : function(g,t,o,s,b, call) { call();}};
				database.add("wah", "a", 2, [], 1, function() { console.log('done'); });
			}
			eventually do
				@proc.output.include? "locked\ndone"
			end
		end

		it "should queue the add when the file is locked" do
			@proc = async_eval_js %Q{
				var Database = require ('zangetsu/sharded_database').Database;
				var Router = require('zangetsu/shard_router').ShardRouter;
				Router.prototype.configure = function() {};
				var router = new Router();
				router.toc = {"a" : { 1 : {
					"shard" : { "identifier" : "one" }
				}  }};
				router.lockTable = {"a/1" : { "callbacks" : [] } };
				var database = new Database(router);
				database.shardConnections["one"] = {"add" : function(g,t,o,s,b, call) { call();}};
				database.add("wah", "a", 1, [], 1, function() { console.log('done'); });
				console.log('unlock');
				var callback = router.lockTable["a/1"].callbacks[0];
				delete router.lockTable["a/1"];
				callback();
			}
			eventually do
				@proc.output.include? "unlock\ndone"
			end
		end

		it "should do something when it fails"
	end

	describe "results" do
		it "should concatenate the results of adding shards" do
			@proc = async_eval_js %Q{
				var Database = require ('zangetsu/sharded_database').Database;
				var router = {
					"toc" : {"a" : { 1 : {
						"shard" : { "identifier" : "one" }
					}  }},
					"lockTable" : {"a/1" : { "callbacks" : [] } }
				};
				var database = new Database(router);
				database.shardConnections["one"] = {"results" : function(c) { c({"a" : "done" }); }};
				database.shardConnections["two"] = {"results" : function(c) { c({"b" : "done" }); }};
				database.addingShards = {"one" : true, "two" : true};

				database.results(function(results) {
					console.log(results["a"] + "\\n" + results["b"]);
				});

			}
			eventually do
				@proc.output.include? "done\ndone"
			end
		end
	end
	
	describe "remove" do
		it "should forward the remove call to the right database" do
			@proc = async_eval_js %Q{
				var Database = require ('zangetsu/sharded_database').Database;
				var router = {
					"toc" : {"a" : { 1 : {
						"shard" : { "identifier" : "one" }
					}  }},
					"lock" : function(g,t,f) {
						console.log('locked');
						f(true);
					},
					"shards" : [
						{ "identifier" : "one" }
					]
				};
				var database = new Database(router);
				database.shardConnections["one"] = {"remove" : function(g,t,call) { call();}};
				database.remove("a", 1, function() { console.log('done'); });
			}
			eventually do
				@proc.output.include? "locked\ndone"
			end
		end
	end

	describe "rebalance" do
		it "should move data from oversaturated databases to undersaturated databases"
		it "should lock the file before it is moved"
	end
end
