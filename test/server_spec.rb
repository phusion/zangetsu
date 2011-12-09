# encoding: binary
require File.expand_path(File.dirname(__FILE__) + "/spec_helper")
require File.expand_path(File.dirname(__FILE__) + "/shared_server_spec")

describe "Server" do
	it_should_behave_like "A Zangetsu Server"

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
	
	after :each do
		@connection.close if @connection
		if @server && !@server.closed?
			@server.close
		end
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
	
	describe "adding" do
		before :each do
			handshake
		end
		
		it "writes to the database asynchronously" do
			write_json(:command => 'add',
				:group => 'foo',
				:timestamp => 48 * 60 * 60,
				:size => "hello world".size,
				:opid => 1)
			@connection.write("hello world")
			
			should_never_happen do
				socket_readable?(@connection)
			end
			eventually do
				File.exist?("#{@dbpath}/foo/2/data")
			end
			eventually do
				File.stat("#{@dbpath}/foo/2/data").size ==
					HEADER_SIZE + "hello world".size + FOOTER_SIZE
			end
		end
		
		specify "results can be obtained through the 'results' command" do
			write_json(:command => 'add',
				:group => 'foo',
				:timestamp => 48 * 60 * 60,
				:size => "hello".size,
				:opid => 1)
			@connection.write("hello")
			write_json(:command => 'add',
				:group => 'foo',
				:timestamp => 48 * 60 * 60,
				:size => "world!".size,
				:opid => 2)
			@connection.write("world!")
			
			should_never_happen do
				socket_readable?(@connection)
			end
			
			write_json(:command => 'results')
			read_json.should == {
				"results" => {
					"1" => {
						"status" => "ok",
						"offset" => 0
					},
					"2" => {
						"status" => "ok",
						"offset" => HEADER_SIZE + "hello".size + FOOTER_SIZE
					}
				},
				"status" => "ok"
			}
			
			File.stat("#{@dbpath}/foo/2/data").size.should ==
				HEADER_SIZE + "hello".size + FOOTER_SIZE +
				HEADER_SIZE + "world!".size + FOOTER_SIZE
		end
	end

	describe "getting" do
		before :each do
			handshake
		end

		def add_data(data, options = {})
			write_json({
				:command => 'add',
				:group => 'foo',
				:timestamp => 24 * 60 * 60,
				:size => data.size,
				:opid => 1
			}.merge(options))
			@connection.write(data)
			write_json(:command => 'results')
			results = read_json
			results['status'].should == 'ok'
			return results['results']['1']['offset']
		end

		it "works" do
			add_data('hello')
			write_json(:command => 'get',
				:group => 'foo',
				:timestamp => 24 * 60 * 60,
				:offset => 0)
			result = read_json
			result['status'].should == 'ok'
			result['corrupted'].should be_nil
			@connection.read(result['size']).should == 'hello'
		end

		it "only returns metadata for corrupted records" do
			add_data('hello', :corrupted => true)
			write_json(:command => 'get',
				:group => 'foo',
				:timestamp => 24 * 60 * 60,
				:offset => 0)
			result = read_json
			result['status'].should == 'ok'
			result['corrupted'].should be_true
			@connection.read(result['size']).should == 'hello'
		end

		it "returns an error if the read operation fails" do
			write_json(:command => 'get',
				:group => 'foo',
				:timestamp => 24 * 60 * 60,
				:offset => 0)
			result = read_json
			result['status'].should == 'error'
			result['message'].should == 'Cannot get requested data: Time entry not found'
		end
	end
end
