# encoding: binary
require File.expand_path(File.dirname(__FILE__) + "/spec_helper")

describe "ShardServerProxy" do
	# ShardServer is a proxy for physical shard servers
	
	def initialize_remote
		@dbpath = 'tmp/db'
		FileUtils.mkdir_p(@dbpath)
		@code = %Q{
			var Server = require('zangetsu/shard_server').ShardServer;
			var server = new Server("tmp/db");
			server.start('127.0.0.1', #{TEST_SERVER_PORT}, "localhost");
		}
		@server = async_eval_js(@code, :capture => !DEBUG)
		@connection = wait_for_port(TEST_SERVER_PORT)
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
			pending
			@proc = async_eval_js @connect_code
			eventually do
				output = @proc.output
				@proc.output == "connected\n"
			end
		end
	end
end
