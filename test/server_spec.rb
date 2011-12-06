# encoding: binary
require File.expand_path(File.dirname(__FILE__) + "/spec_helper")

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
	
	after :each do
		@connection.close if @connection
		if @server && !@server.closed?
			@server.close
		end
	end
	
	def handshake(args = {})
		read_json
		write_json(args)
		read_json.should == { 'status' => 'ok' }
	end
	
	describe "handshake" do
		it "works as expected" do
			response = read_json
			response['protocolMajor'].should == 1
			response['protocolMinor'].should == 0
			response['serverName'].should =~ /zangetsu/i
			
			write_json({})
			read_json.should == { 'status' => 'ok' }
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
		
		it "complains if an opid is given for which the result isn't yet fetched" do
			write_json(:command => 'add',
				:group => 'foo',
				:timestamp => 48 * 60 * 60,
				:size => "hello".size,
				:opid => 1)
			@connection.write("hello")
			should_never_happen { socket_readable?(@connection) }
			
			write_json(:command => 'add',
				:group => 'foo',
				:timestamp => 48 * 60 * 60,
				:size => "hello".size,
				:opid => 1)
			@connection.write("hello")
			response = read_json
			response["status"].should == "error"
			response["message"].should =~ /opid is already used/
		end
	end
	
	describe "fetching results" do
		before :each do
			handshake
		end
		
		it "clears the result set" do
			write_json(:command => 'add',
				:group => 'foo',
				:timestamp => 48 * 60 * 60,
				:size => "hello".size,
				:opid => 1)
			@connection.write("hello")
			
			write_json(:command => 'results')
			read_json.should == {
				"results" => {
					"1" => {
						"status" => "ok",
						"offset" => 0
					}
				},
				"status" => "ok"
			}
			
			write_json(:command => 'results')
			read_json.should == {
				"results" => {},
				"status"  => "ok"
			}
		end
		
		it "deletes any active opids" do
			write_json(:command => 'add',
				:group => 'foo',
				:timestamp => 48 * 60 * 60,
				:size => "hello".size,
				:opid => 1)
			@connection.write("hello")
			
			write_json(:command => 'results')
			read_json.should == {
				"results" => {
					"1" => {
						"status" => "ok",
						"offset" => 0
					}
				},
				"status" => "ok"
			}
			
			write_json(:command => 'add',
				:group => 'foo',
				:timestamp => 48 * 60 * 60,
				:size => "hello".size,
				:opid => 1)
			@connection.write("hello")
			
			write_json(:command => 'results')
			read_json.should == {
				"results" => {
					"1" => {
						"status" => "ok",
						"offset" => HEADER_SIZE + "hello".size + FOOTER_SIZE
					}
				},
				"status" => "ok"
			}
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