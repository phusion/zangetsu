# encoding: binary
require File.expand_path(File.dirname(__FILE__) + "/spec_helper")
require File.expand_path(File.dirname(__FILE__) + "/shared_server_spec")

describe "Server" do
	before :each do
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
			(HEADER_SIZE * data.size) +
			data.map(&:size).inject(0) { |m, x| m += x } +
			(FOOTER_SIZE * data.size)
	end

	def should_be_added(key, data)
		eventually do
			data_file_exist?(key)
		end
		eventually do
			data_exist?(key, "hello world")
		end
	end

	it_should_behave_like "A Zangetsu Server"
end
