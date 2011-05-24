# encoding: binary
require File.expand_path(File.dirname(__FILE__) + "/spec_helper")

describe "Replication" do
	before :all do
		output, error = eval_js!(%Q{
			var TimeEntry = require('zangetsu/time_entry.js');
			console.log(TimeEntry.HEADER_SIZE);
			console.log(TimeEntry.FOOTER_SIZE);
		})
		@header_size, @footer_size = output.split("\n")
		@header_size = @header_size.to_i
		@footer_size = @footer_size.to_i
		
		@dbpath = 'tmp/db'
		@common_code = %Q{
			var sys      = require('sys');
			var Server   = require('zangetsu/server');
			var server   = new Server.Server('#{@dbpath}');
			var Database = require('zangetsu/database');
			var database = server.database;
			var CRC32    = require('zangetsu/crc32.js');
			
			function add(groupName, dayTimestamp, strOrBuffers, callback) {
				var buffers;
				if (typeof(strOrBuffers) == 'string') {
					buffers  = [new Buffer(strOrBuffers)];
				} else {
					buffers = strOrBuffers;
				}
				var checksum = CRC32.toBuffer(buffers);
				database.add(groupName, dayTimestamp, buffers, checksum,
					function(err, offset, rawSize, buffers)
				{
					if (err) {
						console.log(err);
						process.exit(1);
					} else if (callback) {
						callback(offset, rawSize, buffers);
					}
				});
			}
		}
	end
	
	before :each do
		FileUtils.mkdir_p(@dbpath)
		@server_socket = TCPServer.new('127.0.0.1', TEST_SERVER_PORT)
		@server_socket.listen(50)
		@server_socket.fcntl(Fcntl::F_SETFL, @server_socket.fcntl(Fcntl::F_GETFL) | Fcntl::O_NONBLOCK)
	end
	
	after :each do
		@connection.close if @connection
		@connection2.close if @connection2
		if @server && !@server.closed?
			sleep 0.1 if DEBUG
			@server.close
		end
		@server_socket.close if @server_socket
	end
	
	def connect_to_server(port = TEST_SERVER_PORT)
		socket = TCPSocket.new('127.0.0.1', port)
		socket.sync = true
		return socket
	end
	
	context "master server" do
		describe "when a server joins it" do
			def start_master
				@code = %Q{
					#{@common_code}
					server.startAsMasterWithFD(#{@server_socket.fileno});
				}
				@server = async_eval_js(@code, :capture => !DEBUG)
				@connection = connect_to_server
			end
			
			def handshake(connection = @connection, args = { :identity => 'replica-member' })
				read_json(connection)
				write_json(connection, args)
				return read_json(connection)
			end
			
			it "informs about both sides' roles during handshake" do
				start_master
				handshake.should == {
					'status' => 'ok',
					'your_role' => 'slave',
					'my_role' => 'master'
				}
			end
			
			it "synchronizes the joining server" do
				eval_js!(%Q{
					#{@common_code}
					
					add('baz', 2, 'hello');
					add('baz', 2, 'world');
					add('baz', 4, 'this is a sentence');
					add('baz', 5, 'xxxxx');
					add('baz', 6, 'xxx');
					
					add('test', 5, 'test data');
				})
				FileUtils.mkdir_p("#{@dbpath}/test/3")
				FileUtils.mkdir_p("#{@dbpath}/test/4")
				
				FileUtils.mkdir_p('tmp/slavedb')
				eval_js!(%Q{
					#{@common_code}
					database = new Database.Database('tmp/slavedb');
					
					add('foo', 123, 'xxx');
					add('foo', 456, 'xxx');
					
					add('bar', 789, 'xxx');
					
					add('baz', 1, 'xxx');
					add('baz', 2, 'hello');
					add('baz', 3, 'xxx');
					add('baz', 5, 'xxxxxxxxxxx');
					add('baz', 6, 'xxx');
				})
				
				slave_toc = {
					# Doesn't exist on the master
					:foo => {
						'123' => { :size => nil },
						'456' => { :size => nil }
					},
					# Doesn't exist on the master
					:bar => {
						'789' => { :size => nil }
					},
					# Group exists on the master
					:baz => {
						# Doesn't exist on the master
						'1' => { :size => nil },
						# Exists on the master but is larger than on slave
						'2' => { :size => nil },
						# Doesn't exist on the master
						'3' => { :size => nil },
						# Exists on the master but is smaller than on slave
						'5' => { :size => nil },
						
						# 4 only exists on the master
						
						# Same size on both master and slave
						'6' => { :size => nil }
					}
					# group 'test' does not exist on the slave
				}
				
				slave_toc.each_pair do |group_name, time_entries|
					time_entries.each_pair do |day_timestamp, details|
						size = File.size("tmp/slavedb/#{group_name}/#{day_timestamp}/data")
						details[:size] = size
					end
				end
				
				
				start_master
				handshake
				
				# It first asks for the slave's TOC.
				read_json.should == { 'command' => 'getToc' }
				write_json(slave_toc)
				
				# Then it commands the slave to delete nonexistant groups and time entries
				# and time entries that are larger on the slave than on the master,
				# and to fill time entries that are smaller than on the master.
				commands = []
				9.times do
					command = read_json
					if command['command'] == 'add'
						command['data'] = @connection.read(command['size'])
						commands << command
						read_json.should == { 'command' => 'results' }
						write_json(:status => 'ok', :results => { 1 => 0 })
					else
						commands << command
						write_json(:status => 'ok')
					end
				end
				commands.should include('command' => 'remove', 'group' => 'foo')
				commands.should include('command' => 'remove', 'group' => 'bar')
				commands.should include('command' => 'removeOne',
					'group' => 'baz', 'dayTimestamp' => 1)
				commands.should include('command' => 'removeOne',
					'group' => 'baz', 'dayTimestamp' => 3)
				commands.should include('command' => 'removeOne',
					'group' => 'baz', 'dayTimestamp' => 5)
				commands.should include('command' => 'add',
					'group' => 'baz', 'dayTimestamp' => 2, 'opid' => 1,
					'size' => 5, 'data' => 'world')
				commands.should include('command' => 'add',
					'group' => 'baz', 'dayTimestamp' => 4, 'opid' => 1,
					'size' => 18, 'data' => 'this is a sentence')
				commands.should include('command' => 'add',
					'group' => 'baz', 'dayTimestamp' => 5, 'opid' => 1,
					'size' => 5, 'data' => 'xxxxx')
				commands.should include('command' => 'add',
					'group' => 'test', 'dayTimestamp' => 5, 'opid' => 1,
					'size' => 9, 'data' => 'test data')
				
				read_json.should == { 'command' => 'ping' }
				write_json(:status => 'ok')
				
				should_never_happen { socket_readable?(@connection) }
			end
			
			it "doesn't send removal commands if there's nothing to prune" do
				pending
			end
			
			it "doesn't send add commands if there's nothing to fill" do
				pending
			end
			
			specify "if anything was removed from the database while synchronization " +
				"is in progress then those concurrent removals won't be missed" do
				pending
			end
			
			specify "if anything was written to the database while synchronization " +
				"is in progress then those concurrent adds won't be missed" do
				pending
			end
			
			it "sends the topology after synchronization finishes"
		
			it "replicates add and remove commands after synchronization is done" do
				FileUtils.mkdir_p("#{@dbpath}/foo/2")
				File.open("#{@dbpath}/foo/2/data", "w") do |f|
					f.write("xxxxx")
				end
				start_master
				
				# Replica member joins master
				handshake
				read_json.should == { 'command' => 'getToc' }
				write_json({
					:foo => {
						2 => {
							:size => 5
						}
					}
				})
				read_json.should == { 'command' => 'ping' }
				write_json(:status => 'ok')
				
				# Regular client connects to master
				@connection2 = connect_to_server
				handshake(@connection2, {})
				
				# Adds to foo/2
				write_json(@connection2,
					:command => 'add',
					:group => 'foo',
					:timestamp => 48 * 60 * 60,
					:size => "hello".size,
					:opid => 1)
				@connection2.write("hello")
				
				# Adds to foo/1
				write_json(@connection2,
					:command => 'add',
					:group => 'foo',
					:timestamp => 24 * 60 * 60,
					:size => "world".size,
					:opid => 2)
				@connection2.write("world")
				
				# Adds to foo/3
				write_json(@connection2,
					:command => 'add',
					:group => 'foo',
					:timestamp => 72 * 60 * 60,
					:size => "xxx".size,
					:opid => 3)
				@connection2.write("xxx")
				
				# Removes foo/1 and foo/2
				write_json(@connection2,
					:command => 'remove',
					:group => 'foo',
					:timestamp => 72 * 60 * 60)
				read_json(@connection2)['status'].should == 'ok'
				
				write_json(@connection2, :command => 'results')
				read_json(@connection2)['status'].should == 'ok'
				
				# We expect all commands to be replicated in the same order
				
				read_json.should == {
					'command' => 'add',
					'group' => 'foo',
					'dayTimestamp' => 2,
					'size' => "hello".size
				}
				@connection.read("hello".size).should == "hello"
				read_json.should == { 'command' => 'results' }
				write_json(:status => 'ok')
				
				read_json.should == {
					'command' => 'add',
					'group' => 'foo',
					'dayTimestamp' => 1,
					'size' => "world".size
				}
				@connection.read("world".size).should == "world"
				read_json.should == { 'command' => 'results' }
				write_json(:status => 'ok')
				
				read_json.should == {
					'command' => 'add',
					'group' => 'foo',
					'dayTimestamp' => 3,
					'size' => "xxx".size
				}
				@connection.read("xxx".size).should == "xxx"
				read_json.should == { 'command' => 'results' }
				write_json(:status => 'ok')
				
				read_json.should == {
					'command' => 'removeOne',
					'group' => 'foo',
					'dayTimestamp' => 2
				}
				write_json(:status => 'ok')
				
				read_json.should == {
					'command' => 'removeOne',
					'group' => 'foo',
					'dayTimestamp' => 1
				}
				write_json(:status => 'ok')
				
				should_never_happen { socket_readable?(@connection) }
			end
			
			specify "it refills a time entry if filling fails because the slave time entry size is incorrect"
		end
	end
	
	context "slave server" do
		describe "when joining a master" do
			before :each do
				@server_socket2 = TCPServer.new('127.0.0.1', TEST_SERVER_PORT2)
				@server_socket2.listen(50)
				@server_socket2.fcntl(Fcntl::F_SETFL, @server_socket2.fcntl(Fcntl::F_GETFL) | Fcntl::O_NONBLOCK)
			end
			
			after :each do
				@server_socket2.close
			end
			
			def start_slave
				@code = %Q{
					#{@common_code}
					server.startAsReplicaMemberWithFD(#{@server_socket.fileno},
						undefined, '127.0.0.1', #{TEST_SERVER_PORT2});
				}
				@server = async_eval_js(@code, :capture => !DEBUG)
				Timeout.timeout(3, RuntimeError) do
					select([@server_socket2])
				end
				@connection = @server_socket2.accept
			end
			
			def handshake(connection = @connection, args = { :identity => 'replica-member' })
				write_json(:protocolMajor => 1, :protocolMinor => 0)
				read_json.should == { 'identity' => 'replica-member' }
				write_json(:status => 'ok', :your_role => 'slave', :my_role => 'master')
			end
			
			it "works" do
				eval_js!(%Q{
					#{@common_code}
					add('foo', 1, 'hello');
					add('foo', 1, 'world');
					add('foo', 2, 'test');
					add('bar', 2, 'some text');
				})
				start_slave
				handshake
			end
		end
	end
end