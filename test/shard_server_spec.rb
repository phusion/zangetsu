# encoding: binary
require File.expand_path(File.dirname(__FILE__) + "/spec_helper")
require File.expand_path(File.dirname(__FILE__) + "/shared_server_spec")

describe "ShardServer" do
	it "should be able to add an external database"
	it "should be able to rebalance data after adding an external database"
	it "should be able to remove an external database without losing data"

	it_should_behave_like "A Zangetsu Server"

	before :each do
		@server_socket = TCPServer.new('127.0.0.1', TEST_SERVER_PORT)
		@server_socket.listen(50)
		@server_socket.fcntl(Fcntl::F_SETFL, @server_socket.fcntl(Fcntl::F_GETFL) | Fcntl::O_NONBLOCK)
		@code = %Q{
			var ShardServer = require('zangetsu/shards').ShardServer;
			var server = new ShardServer();
			server.startWithFD(#{@server_socket.fileno});
		}
		@server = async_eval_js(@code, :capture => true)
		@connection = TCPSocket.new('127.0.0.1', TEST_SERVER_PORT)
		@connection.sync = true
	end

	def data_exist?(key, data)
		data = [data] if not data.is_a? Array
		pending "implement data_exist?"
	end

	def should_be_added(key, data)
		pending "implement should_be_added"
	end
end
