# encoding: binary
require File.expand_path(File.dirname(__FILE__) + "/spec_helper")

describe "Shard" do
	# Shard is a proxy for physical shards
	
	def initialize_remote
		@dbpath = 'tmp/db'
		FileUtils.mkdir_p(@dbpath)
		@server_socket = TCPServer.new('127.0.0.1', TEST_SERVER_PORT)
		@server_socket.listen(50)
		@server_socket.fcntl(Fcntl::F_SETFL, @server_socket.fcntl(Fcntl::F_GETFL) | Fcntl::O_NONBLOCK)
		@server_code = %Q{
				var Server = require('zangetsu/server').Server;
				var server = new Server("tmp/db");
				server.startAsMasterWithFD(#{@server_socket.fileno});
		}
		@server = async_eval_js(@server_code, :capture => true)
		@connection = TCPSocket.new('127.0.0.1', TEST_SERVER_PORT)
		@connection.sync = true
	end

	def finalize_remote
		@connection.close if @connection
		if @server && !@server.closed?
			@server.close
		end
		@server_socket.close if @server_socket
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
	
	describe "connect" do
		before :each do
			@connect_code = @shard_code + %Q{
				shard.connect(function(message) {
					console.log("connected");
				});
			}
		end

		it "should perform the handshake" do
			@proc = async_eval_js @connect_code
			eventually do
				output = @proc.output
				@proc.output == "connected\n"
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
					console.log(buffers[0].toString('utf8'));
				}
				var done = function(err) {
					shard.get("groupName", 1, 0, callback);
				}
				var buffer = new Buffer("string");
				shard.add("groupName", 1, 2, buffer.length, [buffer], done);
			}
			@proc = async_eval_js code
			eventually do
				@proc.output == "string\n"
			end
		end
	end

	describe "getTOC" do
		it "should fetch the toc from the shard" do
			code = @shard_code + %Q{
				var callback = function(message, buffers) {
					var truth = message.groupName['0'].size == 26;
					console.log(truth);
				}
				var done = function(err) {
					shard.getTOC(callback);
				}
				var buffer = new Buffer("string");
				shard.add("groupName", 1, 2, buffer.length, [buffer], done);
			}
			@proc = async_eval_js code
			eventually do
				@proc.output == "true\n"
			end
		end
	end

	describe "results" do
		it "should get the results of previous write operations" do
			pending "Ask hongli how results should behave"
			code = @shard_code + %Q{
				var callback = function(message, buffers) {
					console.log(message);
				}
				var done = function(err) {
					shard.results(false, callback);
				}
				var buffer = new Buffer("string");
				shard.add("groupName", 1, 2, buffer.length, [buffer], done);
			}
			@proc = async_eval_js code, :capture => false
			eventually do
				@proc.output == "true\n"
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
				@proc.output == "true\n"
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
				@proc.output == "true\n" and
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
				@proc.output == "true\n" and
					not Dir.entries(@dbpath + '/groupName').include? "0"
			end
		end
	end
end
