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

describe "Server" do
	after :each do
		@connection.close if @connection && !@connection.closed?
		@server.close if @server && !@server.closed?
	end
	
	def start_as_slave
		@dbpath = 'tmp/db'
		FileUtils.mkdir_p(@dbpath)
		@code = %Q{
			var Server = require('zangetsu/server').Server;
			var server = new Server("tmp/db");
			server.startAsSlave('127.0.0.1', #{TEST_SERVER_PORT}, undefined,
				'127.0.0.1', #{TEST_SERVER_PORT2});
		}
		@server = async_eval_js(@code, :capture => !DEBUG)
		@connection = wait_for_port(TEST_SERVER_PORT)
	end

	def handshake(args = {})
		read_json
		write_json(args)
		read_json.should == { 'status' => 'ok' }
	end

	describe "adding" do
		before :each do
			start_as_slave
			handshake
		end

		it "returns an error if the server is a slave" do
			write_json(
				:group => 'foo',
				:timestamp => 48 * 60 * 60,
				:command => 'add',
				:size => "hello world".size,
				:opid => 1
			)
			read_json.should == {
				'status' => 'error',
				'message' => 'This command is not allowed because the server is in slave mode',
				'disconnect' => true
			}
		end
	end

	describe "remove" do
		before :each do
			start_as_slave
			handshake
		end

		it "returns an error if the server is a slave" do
			write_json(
				:command => 'remove',
				:group => 'foo'
			)
			read_json.should == {
				'status' => 'error',
				'message' => 'This command is not allowed because the server is in slave mode',
				'disconnect' => true
			}
		end
	end

	describe "removeOne" do
		before :each do
			start_as_slave
			handshake
		end

		it "returns an error if the server is a slave" do
			write_json(
				:command => 'removeOne',
				:group => 'foo',
				:dayTimestamp => 1
			)
			read_json.should == {
				'status' => 'error',
				'message' => 'This command is not allowed because the server is in slave mode',
				'disconnect' => true
			}
		end
	end
end
