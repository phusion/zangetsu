# encoding: binary
require File.expand_path(File.dirname(__FILE__) + "/spec_helper")
require File.expand_path(File.dirname(__FILE__) + "/shared_server_spec")

describe "ShardServer" do
	it "should launch from a configuration file" do
	 	@dbpath = 'tmp/db'
		FileUtils.mkdir_p(@dbpath + '2')
	 	FileUtils.mkdir_p(@dbpath)

		@shard_port = TEST_SERVER_PORT + 1
		@shard_code = %Q{
			var Server = require('zangetsu/server').Server;
			var server = new Server("tmp/db");
			server.startAsMaster('127.0.0.1', #{@shard_port});
		}
		@shard = async_eval_js(@shard_code, :capture => !DEBUG)

		@shard_port2 = TEST_SERVER_PORT + 2
		@shard_code2 = %Q{
			var Server = require('zangetsu/server').Server;
			var server = new Server("tmp/db2");
			server.startAsMaster('127.0.0.1', #{@shard_port2});
		}
		@shard2 = async_eval_js(@shard_code2, :capture => !DEBUG)

		config = %Q{
			{ "shards" : [
				{"hostname" : "localhost", "port" : #{@shard_port}}
,				{"hostname" : "localhost", "port" : #{@shard_port2}}
			], "shardRouters" : [
			]
			}
		}
		File.open('tmp/config.json', 'w') {|f| f.write(config) }

	 	@code = %Q{
	 		var Server = require('zangetsu/shard_server').ShardServer;
	 		var server = new Server("tmp/config.json");
	 		server.start('127.0.0.1', #{TEST_SERVER_PORT});
	 	}

	 	@server = async_eval_js(@code, :capture => !DEBUG)
	 	@connection = wait_for_port(TEST_SERVER_PORT)

		read_json
		write_json({})
		read_json.should== {'status' => 'ok' }
		10.times do |i|
			write_json(
				:group => "foo#{i}",
				:timestamp => 48 * 60 * 60,
				:command => 'add',
				:size => "hello world".size,
				:opid => i
			)
			@connection.write("hello world")
			
			write_json(
				:command => 'results'
			)
			read_json.should == {
				"results"=> {
					"#{i}"=>{ "status"=>"ok" }
				},
				"status"=>"ok"
			}
		end

		File.delete('tmp/config.json')
		@connection.close if @connection
		if @server && !@server.closed?
			@server.close
		end
		if @shard && !@shard.closed?
			@shard.close
		end
	end
	it "should be able to add an external database"
	it "should be able to rebalance data after adding an external database"
	it "should be able to remove an external database without losing data"
end

describe "ShardServer operations" do
	it_should_behave_like "A Zangetsu Server"

	 before :each do
	 	@dbpath = 'tmp/db'
	 	FileUtils.mkdir_p(@dbpath)

		@shard_port = TEST_SERVER_PORT + 1
		@shard_code = %Q{
			var Server = require('zangetsu/server').Server;
			var server = new Server("tmp/db");
			server.startAsMaster('127.0.0.1', #{@shard_port});
		}
		@shard = async_eval_js(@shard_code, :capture => !DEBUG)

		config = %Q{{ "shards" : [{"hostname" : "localhost", "port" : #{@shard_port}}], "shardServers" : []}}
		File.open('tmp/config.json', 'w') {|f| f.write(config) }
	 	@code = %Q{
	 		var Server = require('zangetsu/shard_server').ShardServer;
	 		var server = new Server("tmp/config.json");
	 		server.start('127.0.0.1', #{TEST_SERVER_PORT});
	 	}
	 	@server = async_eval_js(@code, :capture => !DEBUG)
	 	@connection = wait_for_port(TEST_SERVER_PORT)
	 end

	after :each do
		File.delete('tmp/config.json')
		@connection.close if @connection
		if @server && !@server.closed?
			@server.close
		end
		if @shard && !@shard.closed?
			@shard.close
		end
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
end
