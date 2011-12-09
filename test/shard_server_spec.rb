# encoding: binary
require File.expand_path(File.dirname(__FILE__) + "/spec_helper")
require File.expand_path(File.dirname(__FILE__) + "/shared_server_spec")

describe "ShardServer" do
	it "should be able to add an external database"
	it "should be able to rebalance data after adding an external database"
	it "should be able to remove an external database without losing data"

	#it_should_behave_like "A Zangetsu Server"

	# before :each do
	# 	@dbpath = 'tmp/db'
	# 	FileUtils.mkdir_p(@dbpath)
	# 	@code = %Q{
	# 		var Server = require('zangetsu/shard_server').ShardServer;
	# 		var server = new Server("tmp/db");
	# 		server.start('127.0.0.1', #{TEST_SERVER_PORT}, "localhost");
	# 	}
	# 	@server = async_eval_js(@code, :capture => !DEBUG)
	# 	@connection = wait_for_port(TEST_SERVER_PORT)
	# end

	after :each do
		@connection.close if @connection
		if @server && !@server.closed?
			@server.close
		end
	end

	def data_exist?(key, data)
		data = [data] if not data.is_a? Array
		pending "implement data_exist?"
	end

	def should_be_added(key, data)
		pending "implement should_be_added"
	end
end
