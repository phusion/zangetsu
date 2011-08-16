# encoding: binary
require File.expand_path(File.dirname(__FILE__) + "/spec_helper")
require File.expand_path(File.dirname(__FILE__) + "/shared_server_spec")

describe "Server" do
	it_should_behave_like "A Zangetsu Server"

	before :each do
		@dbpath = 'tmp/db'
		FileUtils.mkdir_p(@dbpath)
		@server_socket = TCPServer.new('127.0.0.1', TEST_SERVER_PORT)
		@server_socket.listen(50)
		@server_socket.fcntl(Fcntl::F_SETFL, @server_socket.fcntl(Fcntl::F_GETFL) | Fcntl::O_NONBLOCK)
		@code = %Q{
			var Server = require('zangetsu/server').Server;
			var server = new Server("tmp/db");
			server.startAsMasterWithFD(#{@server_socket.fileno});
		}
		@server = async_eval_js(@code, :capture => true)
		@connection = TCPSocket.new('127.0.0.1', TEST_SERVER_PORT)
		@connection.sync = true
	end

	def timestamp_to_day(timestamp)
		timestamp / (60 * 60 * 24)
	end

	def path_for_key(key)
		"#{@dbpath}/#{key[:group]}/#{timestamp_to_day(key[:timestamp])}/data"
	end

	def data_file_exist?(key)
		File.exist?(path_for_key(key))
	end

	def data_exist?(key, data)
		data = [data] if not data.is_a? Array
		File.stat(path_for_key(key)).size ==
			(@header_size * data.size) + data.map(&:size).inject(0){|m,x|m+=x} + (@footer_size * data.size)
	end

	def should_be_added(key, data)
		eventually do
			data_file_exist?(key)
		end
		eventually do
			data_exist?(key, "hello world")
		end
	end
end
