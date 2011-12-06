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
			var Server   = require('zangetsu/server');
			var server   = new Server.Server('#{@dbpath}');
			var Database = require('zangetsu/database');
			var database = server.database;
			var CRC32    = require('zangetsu/crc32.js');

			server.resultCheckThreshold = 1;
			
			function add(groupName, dayTimestamp, strOrBuffers, options, callback) {
				var buffers;
				if (typeof(strOrBuffers) == 'string') {
					buffers  = [new Buffer(strOrBuffers)];
				} else {
					buffers = strOrBuffers;
				}
				if (typeof(options) == 'function') {
					callback = options;
					options = undefined;
				}
				var checksum = CRC32.toBuffer(buffers);
				database.add(groupName, dayTimestamp, buffers, checksum, options,
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
	end
	
	after :each do
		@connection.close if @connection
		@connection2.close if @connection2
		@connection3.close if @connection3
		if @server && !@server.closed?
			sleep 0.1 if DEBUG
			@server.close
		end
	end
	
	def connect_to_server(port = TEST_SERVER_PORT)
		return wait_for_port(port)
	end
	
	context "master server" do
		describe "when a server joins it" do
			def start_master
				@code = %Q{
					#{@common_code}
					server.startAsMaster('127.0.0.1', #{TEST_SERVER_PORT});
				}
				@server = async_eval_js(@code, :capture => !DEBUG)
				@connection = connect_to_server
			end
			
			def handshake(connection = @connection, args = { :identity => 'replica-slave' })
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
					add('test', 5, 'more test data');
				}, :capture => false)
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
				}, :capture => false)
				
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
				10.times do
					command = read_json
					if command['command'] == 'add'
						command['data'] = @connection.read(command['size'])
						commands << command
						read_json.should == { 'command' => 'results' }
						write_json(:status => 'ok', :results => { 0 => { :status => 'ok' } })
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
					'group' => 'baz', 'timestamp' => 2 * 24 * 60 * 60, 'opid' => 0,
					'size' => 5, 'data' => 'world')
				commands.should include('command' => 'add',
					'group' => 'baz', 'timestamp' => 4 * 24 * 60 * 60, 'opid' => 0,
					'size' => 18, 'data' => 'this is a sentence')
				commands.should include('command' => 'add',
					'group' => 'baz', 'timestamp' => 5 * 24 * 60 * 60, 'opid' => 0,
					'size' => 5, 'data' => 'xxxxx')
				commands.should include('command' => 'add',
					'group' => 'test', 'timestamp' => 5 * 24 * 60 * 60, 'opid' => 0,
					'size' => 9, 'data' => 'test data')
				commands.should include('command' => 'add',
					'group' => 'test', 'timestamp' => 5 * 24 * 60 * 60, 'opid' => 0,
					'size' => 14, 'data' => 'more test data')
				
				read_json.should == { 'command' => 'ping' }
				write_json(:status => 'ok')
				
				should_never_happen { socket_readable?(@connection) }
			end
			
			specify "if anything was written or removed from the database while synchronization " +
			        "is in progress then those concurrent modifications won't be missed" do
				eval_js!(%Q{
					#{@common_code}
					add('foo', 1, 'foo1')
					add('foo', 2, 'foo2')
					add('foo', 3, 'foo3')
				})

				start_master

				# Slave joins master
				@replica = @connection
				handshake(@replica)
				read_json(@replica).should == { 'command' => 'getToc' }
				write_json(@replica, {})

				# Normal client connects to master
				@client = @connection2 = connect_to_server
				handshake(@client, {})
				

				# Now, back to the replica...

				# Expecting 'foo1' record from master.
				read_json(@replica).should == {
					'command' => 'add',
					'group' => 'foo',
					'timestamp' => 24 * 60 * 60,
					'size' => "foo1".size,
					'opid' => 0
				}
				@replica.read('foo1'.size).should == 'foo1'
				read_json(@replica).should == { 'command' => 'results' }
				write_json(@replica,
					:status => 'ok',
					:results => {
						0 => {
							:status => 'ok',
							:offset => 0
						}
					}
				)

				# Expecting 'foo2' record from master.
				read_json(@replica).should == {
					'command' => 'add',
					'group' => 'foo',
					'timestamp' => 48 * 60 * 60,
					'size' => "foo2".size,
					'opid' => 0
				}
				@replica.read('foo2'.size).should == 'foo2'
				read_json(@replica).should == { 'command' => 'results' }
				write_json(@replica,
					:status => 'ok',
					:results => {
						0 => {
							:status => 'ok',
							:offset => 0
						}
					}
				)

				# Expecting 'foo3' record from master.
				read_json(@replica).should == {
					'command' => 'add',
					'group' => 'foo',
					'timestamp' => 72 * 60 * 60,
					'size' => "foo3".size,
					'opid' => 0
				}
				@replica.read('foo3'.size).should == 'foo3'

				# Before acknowledging this replication command, modify foo/2
				# and remove foo/1.
				write_json(@client,
					:command => 'add',
					:group => 'foo',
					:timestamp => 48 * 60 * 60 + 1,
					:size => 'hi'.size,
					:opid => 0)
				@client.write('hi')
				write_json(@client,
					:command => 'remove',
					:group => 'foo',
					:timestamp => 48 * 60 * 60)
				read_json(@client).should == { 'status' => 'ok' }
				write_json(@client, :command => 'results')
				read_json(@client)['status'].should == 'ok'

				# Now acknowledge the last replication command.
				read_json(@replica).should == { 'command' => 'results' }
				write_json(@replica,
					:status => 'ok',
					:results => {
						0 => {
							:status => 'ok',
							:offset => 0
						}
					}
				)

				# Expecting removal of foo/1.
				read_json(@replica).should == {
					'command' => 'removeOne',
					'group' => 'foo',
					'dayTimestamp' => 1
				}
				write_json(@replica, :status => 'ok')

				# Expecting foo/2 'hi' record.
				read_json(@replica).should == {
					'command' => 'add',
					'group' => 'foo',
					'timestamp' => 48 * 60 * 60,
					'size' => "hi".size,
					'opid' => 0
				}
				@replica.read('hi'.size).should == 'hi'
				read_json(@replica).should == { 'command' => 'results' }
				write_json(@replica,
					:status => 'ok',
					:results => {
						0 => {
							:status => 'ok',
							:offset => @header_size + 'foo2'.size + @footer_size
						}
					}
				)

				read_json(@replica).should == { 'command' => 'ping' }
				write_json(@replica, :status => 'ok')
				should_never_happen { socket_readable?(@replica) }
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
				

				# Let's command the master to do a few things

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


				# The slave should receive the same commands in the same order

				# Replication command for adding to add/2
				read_json.should == {
					'command' => 'add',
					'group' => 'foo',
					'timestamp' => 48 * 60 * 60,
					'size' => "hello".size,
					'opid' => 0
				}
				@connection.read("hello".size).should == "hello"
				read_json.should == { 'command' => 'results' }
				write_json(:status => 'ok')

				# Replication command for adding to foo/1
				read_json.should == {
					'command' => 'add',
					'group' => 'foo',
					'timestamp' => 24 * 60 * 60,
					'size' => "world".size,
					'opid' => 0
				}
				@connection.read("world".size).should == "world"
				read_json.should == { 'command' => 'results' }
				write_json(:status => 'ok')

				# Replication command for adding to foo/3
				read_json.should == {
					'command' => 'add',
					'group' => 'foo',
					'timestamp' => 72 * 60 * 60,
					'size' => "xxx".size,
					'opid' => 0
				}
				@connection.read("xxx".size).should == "xxx"
				read_json.should == { 'command' => 'results' }
				write_json(:status => 'ok')


				# Now back to the master...

				# Wait until add commands on the master are done
				write_json(@connection2, :command => 'results')
				read_json(@connection2)['status'].should == 'ok'
				
				# Removes foo/1 and foo/2
				write_json(@connection2,
					:command => 'remove',
					:group => 'foo',
					:timestamp => 72 * 60 * 60)
				read_json(@connection2)['status'].should == 'ok'
				
				# The removal command should have been replicaed
				
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
			
			specify "an add operation only completes after it has been replicated to all slaves" do
				FileUtils.mkdir_p("#{@dbpath}/foo/2")
				File.open("#{@dbpath}/foo/2/data", "w") do |f|
					f.write("xxxxx")
				end
				start_master
				
				# Replica member 1 joins master
				@replica1 = @connection
				handshake(@replica1)
				read_json(@replica1).should == { 'command' => 'getToc' }
				write_json(@replica1,
					:foo => {
						2 => {
							:size => 5
						}
					}
				)
				read_json(@replica1).should == { 'command' => 'ping' }
				write_json(@replica1, :status => 'ok')
				
				# Replica member 2 joins master
				@replica2 = @connection2 = connect_to_server
				handshake(@replica2)
				read_json(@replica2).should == { 'command' => 'getToc' }
				write_json(@replica2,
					:foo => {
						2 => {
							:size => 5
						}
					}
				)
				read_json(@replica2).should == { 'command' => 'ping' }
				write_json(@replica2, :status => 'ok')

				# Regular client connects to master
				@client = @connection3 = connect_to_server
				handshake(@client, {})


				# Tell master to add to foo/2
				data = 'hello'
				write_json(@client,
					:command => 'add',
					:group => 'foo',
					:timestamp => 48 * 60 * 60,
					:size => data.size,
					:opid => 1)
				@client.write(data)

				# Master should not respond to 'results' command yet
				# because no slave has accepted the data yet.
				write_json(@client, :command => 'results')
				should_never_happen { socket_readable?(@client) }

				# Slave 1 now accepts the data, but master still doesn't
				# respond to 'results' yet.
				read_json(@replica1).should == {
					'command' => 'add',
					'group' => 'foo',
					'timestamp' => 48 * 60 * 60,
					'size' => data.size,
					'opid' => 0
				}
				@replica1.read(data.size).should == data
				read_json(@replica1).should == { 'command' => 'results' }
				write_json(@replica1, :status => 'ok')
				should_never_happen { socket_readable?(@client) }
				
				# Slave 2 now accepts the data. Master now responds to
				# 'results' command.
				read_json(@replica2).should == {
					'command' => 'add',
					'group' => 'foo',
					'timestamp' => 48 * 60 * 60,
					'size' => data.size,
					'opid' => 0
				}
				@replica2.read(data.size).should == data
				read_json(@replica2).should == { 'command' => 'results' }
				write_json(@replica2, :status => 'ok')
				read_json(@client).should == {
					'status' => 'ok',
					'results' => {
						'1' => {
							'status' => 'ok',
							'offset' => 5
						}
					}
				}
			end

			it "refills a time entry if filling fails because the slave time entry data file is corrupted" do
				# If the data file in the slave time entry is truncated, then
				# the master's attempt to stream from slaveTimeEntry.size will fail.
				# In that case the entire time entry should be refilled.
				
				eval_js!(%Q{
					#{@common_code}
					add('foo', 1, 'hello');
					add('foo', 1, 'world');
				})

				start_master
				handshake
				read_json.should == { 'command' => 'getToc' }
				write_json({
					:foo => {
						1 => {
							:size => @header_size
						}
					}
				})
				
				read_json.should == {
					'command' => 'removeOne',
					'group' => 'foo',
					'dayTimestamp' => 1
				}
				write_json(:status => 'ok')

				read_json.should == {
					'command' => 'add',
					'group' => 'foo',
					'timestamp' => 24 * 60 * 60,
					'size' => 'hello'.size,
					'opid' => 0
				}
				@connection.read('hello'.size).should == 'hello'
				read_json.should == { 'command' => 'results' }
				write_json(
					:status => 'ok',
					:results => {
						0 => {
							:status => 'ok',
							:offset => 0
						}
					}
				)

				read_json.should == {
					'command' => 'add',
					'group' => 'foo',
					'timestamp' => 24 * 60 * 60,
					'size' => 'world'.size,
					'opid' => 0
				}
				@connection.read('world'.size).should == 'world'
				read_json.should == { 'command' => 'results' }
				write_json(
					:status => 'ok',
					:results => {
						0 => {
							:status => 'ok',
							:offset => @header_size + 'hello'.size + @footer_size
						}
					}
				)
			end

			it "correctly replicates records that are marked as corrupted" do
				eval_js!(%Q{
					#{@common_code}
					add('foo', 1, 'aaa');
					add('foo', 1, 'bbb', { corrupted: true });
					add('foo', 1, 'ccc');
				})

				start_master
				handshake
				read_json.should == { 'command' => 'getToc' }
				write_json({})
				offset = 0

				read_json.should == {
					'command' => 'add',
					'group' => 'foo',
					'timestamp' => 24 * 60 * 60,
					'size' => 'aaa'.size,
					'opid' => 0
				}
				@connection.read('aaa'.size).should == 'aaa'
				read_json.should == { 'command' => 'results' }
				write_json(
					:status => 'ok',
					:results => {
						0 => {
							:status => 'ok',
							:offset => offset
						}
					}
				)
				offset += @header_size + 'aaa'.size + @footer_size

				read_json.should == {
					'command' => 'add',
					'group' => 'foo',
					'timestamp' => 24 * 60 * 60,
					'size' => 'bbb'.size,
					'corrupted' => true,
					'opid' => 0
				}
				@connection.read('bbb'.size).should == 'bbb'
				read_json.should == { 'command' => 'results' }
				write_json(
					:status => 'ok',
					:results => {
						0 => {
							:status => 'ok',
							:offset => offset
						}
					}
				)
				offset += @header_size + 'bbb'.size + @footer_size

				read_json.should == {
					'command' => 'add',
					'group' => 'foo',
					'timestamp' => 24 * 60 * 60,
					'size' => 'ccc'.size,
					'opid' => 0
				}
				@connection.read('ccc'.size).should == 'ccc'
				read_json.should == { 'command' => 'results' }
				write_json(
					:status => 'ok',
					:results => {
						0 => {
							:status => 'ok',
							:offset => offset
						}
					}
				)
				offset += @header_size + 'aaa'.size + @footer_size
			end
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
				sleep 1
				@server_socket2.close
			end
			
			def start_slave
				@code = %Q{
					#{@common_code}
					server.startAsSlave('127.0.0.1', #{TEST_SERVER_PORT2})
				}
				@server = async_eval_js(@code, :capture => !DEBUG)
				Timeout.timeout(3, RuntimeError) do
					select([@server_socket2])
				end
				@connection = @server_socket2.accept
			end
			
			def handshake(connection = @connection, args = { :identity => 'replica-slave' })
				write_json(:protocolMajor => 1, :protocolMinor => 0, :role => 'master')
				read_json.should == { 'identity' => 'replica-slave' }
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
	end if false
end