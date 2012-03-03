# encoding: binary
require File.expand_path(File.dirname(__FILE__) + "/spec_helper")

describe "ShardConnection" do
	# ShardConnection is a proxy for physical shards
	
	def initialize_remote
		@dbpath = 'tmp/db'
		FileUtils.mkdir_p(@dbpath)
		@code = %Q{
			var Server = require('zangetsu/server').Server;
			var server = new Server("tmp/db");
			server.startAsMaster('127.0.0.1', #{TEST_SERVER_PORT});
		}
		@server = async_eval_js(@code, :capture => !DEBUG)
		@connection = wait_for_port(TEST_SERVER_PORT)
	end

	def finalize_remote
		@connection.close if @connection
		if @server && !@server.closed?
			@server.close
		end
	end

	before :each do
		@shard_code = %Q{
			var ShardConnection = require('zangetsu/shard_connection').ShardConnection;
			var ioutils = require('zangetsu/io_utils');
			var shard = new ShardConnection(
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

	describe "initialize" do
		it "should give the shard an identifier" do
			output, error = eval_js! %Q{
				var ShardConnection = require('zangetsu/shard_connection').ShardConnection;
				var shard = new ShardConnection({hostname: 'hostname', port: 4312});
				console.log(shard.identifier);
			}
			error.should == ""
			output.should == "hostname:4312\n"
		end
	end
	
	describe "_connect" do
		before :each do
			@connect_code = @shard_code + %Q{
				shard._connect();
			}
		end

		it "should perform the handshake" do
			@proc = async_eval_js @connect_code
			eventually do
				@proc.output.include? "Connected"
			end
		end
	end

	describe "add" do
		it "should add data to the shard and reply when done" do
			code = @shard_code + %Q{
				var done = function(err) {
					console.log(err);
				}
				var buffer = new Buffer("string");
				shard.add("groupName", 1, 2, buffer.length, [buffer], done);
			}

			@proc = async_eval_js code
			eventually do
				Dir.entries(@dbpath).include? "groupName" and
				Dir.entries(@dbpath + '/groupName').include? "0"
			end
		end
	end

	describe "get" do
		it "should fetch data from the shard" do
			code = @shard_code + %Q{
				var callback = function(message, buffers) {
					console.log(message);
					console.log(buffers[0].toString('utf8'));
				}
				var done = function(err) {
					shard.results(function(results) {
						console.log(results);
						shard.get("groupName", 0, 0, callback);
					});
				}
				var buffer = new Buffer("string");
				shard.add("groupName", 1, 2, buffer.length, [buffer], done);
			}
			@proc = async_eval_js code
			eventually do
				@proc.output.include? "string\n"
			end
		end
	end

	describe "getTOC" do
		it "should fetch the toc from the shard" do
			code = @shard_code + %Q{
				var callback = function(message, buffers) {
					console.log(message);
					var truth = message.groupName['0'].size == 35;
					console.log(truth);
				}
				var done = function(err) {
					shard.results(function() {
						shard.getTOC(callback);
					});
				}
				var buffer = new Buffer("string");
				shard.add("groupName", 1, 2, buffer.length, [buffer], done);
			}
			@proc = async_eval_js code
			eventually do
				@proc.output.include? "true\n"
			end
		end
	end

	describe "results" do
		it "should get the results of previous write operations" do
			code = @shard_code + %Q{
				var callback = function(message, buffers) {
					console.log(message.results['2'].status);
				}
				var done = function(err) {
					shard.results(callback);
				}
				var buffer = new Buffer("string");
				shard.add("groupName", 1, 2, buffer.length, [buffer], done);
			}
			@proc = async_eval_js code	
			eventually do
				@proc.output.include? "ok\n"
			end
		end
	end

	describe "ping" do
		it "should receive a reply from the shard" do
			code = @shard_code + %Q{
				var callback = function(message, buffers) {
					console.log(message.status == 'ok');
				}
				shard.ping(callback);
			}
			@proc = async_eval_js code
			eventually do
				@proc.output.include? "true\n"
			end
		end
	end

	describe "remove" do
		it "should remove data from one group" do
			code = @shard_code + %Q{
				var callback = function(message, buffers) {
					console.log(message.status == 'ok');
				}
				var done = function(err) {
					shard.remove("groupName", null, callback);
				}
				var buffer = new Buffer("string");
				shard.add("groupName", 1, 2, buffer.length, [buffer], done);
			}
			@proc = async_eval_js code
			eventually do
				@proc.output.include?("true\n") and
					not Dir.entries(@dbpath).include? "groupName"
			end
		end
	end

	describe "removeOne" do
		it "should remove data for one timestamp" do
			code = @shard_code + %Q{
				var callback = function(message, buffers) {
					console.log(message.status == 'ok');
				}
				var done = function(err) {
					shard.removeOne("groupName", 0, callback);
				}
				var buffer = new Buffer("string");
				shard.add("groupName", 1, 2, buffer.length, [buffer], done);
			}
			@proc = async_eval_js code
			eventually do
				@proc.output.include?("true\n") and
					not Dir.entries(@dbpath + '/groupName').include? "0"
			end
		end
	end
end
