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
			var shard = new Shard(
				{
					hostname : '127.0.0.1',
					port: #{TEST_SERVER_PORT}
				}
			);
		}
	end
	
	describe "connect" do
		before :each do
			@connect_code = @shard_code + %Q{
				shard.connect(function(message) {
					console.log("connected");
				});
			}
			# and setup server
			initialize_remote
		end

		after :each do
			finalize_remote
		end

		it "should perform the handshake" do
			proc = async_eval_js @connect_code
			eventually do
				output = proc.output
				proc.output == "connected\n"
			end
			proc.close
		end
	end

end
