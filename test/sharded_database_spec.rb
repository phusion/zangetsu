# encoding: binary
require File.expand_path(File.dirname(__FILE__) + "/spec_helper")

describe "ShardedDatabase" do
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

	describe "configure" do
		it "should connect to the databases listed in config/shards.json and update the toc accordingly"
	end
end
