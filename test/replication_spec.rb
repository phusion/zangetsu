# encoding: binary
require File.expand_path(File.dirname(__FILE__) + "/spec_helper")

describe "Replication" do
	before :each do
		@dbpath = 'tmp/db'
		FileUtils.mkdir_p(@dbpath)
		@server_socket = TCPServer.new('127.0.0.1', TEST_SERVER_PORT)
		@server_socket.listen(50)
		@server_socket.fcntl(Fcntl::F_SETFL, @server_socket.fcntl(Fcntl::F_GETFL) | Fcntl::O_NONBLOCK)
	end
	
	after :each do
		@connection.close if @connection
		if @server && !@server.closed?
			@server.close
		end
		@server_socket.close if @server_socket
	end
	
	def really_start_server(role)
		@code = %Q{
			var Server = require('zangetsu/server');
			var server = new Server.Server("tmp/db");
		}
		if role == :master
			@code << %Q{server.role = Server.MASTER;\n}
		else
			@code << %Q{server.role = Server.SLAVE;\n}
		end
		@code << %Q{
			server.listenFD(#{@server_socket.fileno});
		}
		@server = async_eval_js(@code, :capture => true)
		@connection = TCPSocket.new('127.0.0.1', TEST_SERVER_PORT)
		@connection.sync = true
	end
	
	describe "when joining a master server" do
		def start_server
			really_start_server(:master)
		end
		
		def handshake(args = { :identity => 'replica-member' })
			read_json
			write_json(args)
			return read_json
		end
		
		it "informs about both sides' roles during handshake" do
			start_server
			handshake.should == {
				'status' => 'ok',
				'your_role' => 'slave',
				'my_role' => 'master'
			}
		end
		
		it "synchronizes the joining server" do
			FileUtils.mkdir_p("#{@dbpath}/baz/2")
			File.open("#{@dbpath}/baz/2/data", "wb") do |f|
				f.write("hello world")
			end
			FileUtils.mkdir_p("#{@dbpath}/baz/4")
			File.open("#{@dbpath}/baz/4/data", "wb") do |f|
				f.write("this is a sentence")
			end
			
			FileUtils.mkdir_p("#{@dbpath}/test/3")
			FileUtils.mkdir_p("#{@dbpath}/test/4")
			
			FileUtils.mkdir_p("#{@dbpath}/test/5")
			File.open("#{@dbpath}/test/5/data", "wb") do |f|
				f.write("x" * 20)
			end
			
			start_server
			handshake
			
			# It first asks for the slave's TOC.
			read_json.should == { 'command' => 'getToc' }
			write_json(
				# Doesn't exist on the master
				:foo => {
					'123' => { :size => 1 },
					'456' => { :size => 2 }
				},
				# Doesn't exist on the master
				:bar => {
					'789' => { :size => 3 }
				},
				# Group exists on the master
				:baz => {
					# Doesn't exist on the master
					'1' => { :size => 4 },
					# Exists on the master but is larger than on slave
					'2' => { :size => 5 },
					# Doesn't exist on the master
					'3' => { :size => 6 },
					# Exists on the master but is smaller than on slave
					'5' => { :size => 100 }
					# 4 only exists on the master
				}
				# group 'test' does not exist on the slave
			)
			
			# Then it commands the slave to delete nonexistant groups and time entries
			# and time entries that are larger on the slave than on the master.
			commands = []
			5.times do
				commands << read_json
				write_json(:status => 'ok')
			end
			commands.should include('command' => 'remove', 'group' => 'foo')
			commands.should include('command' => 'remove', 'group' => 'bar')
			commands.should include('command' => 'removeOne',
				'group' => 'baz', 'dayTimestamp' => 1)
			commands.should include('command' => 'removeOne',
				'group' => 'baz', 'dayTimestamp' => 3)
			commands.should include('command' => 'removeOne',
				'group' => 'baz', 'dayTimestamp' => 5)
			
			# Then it fills the time entries on the slave that are smaller
			# than on the master.
			commands.clear
			3.times do
				command = read_json
				commands << command
				command['data'] = @connection.read(command['size'])
			end
			commands.should include('command' => 'addRaw',
				'group' => 'baz', 'dayTimestamp' => 2,
				'size' => 6, 'data' => ' world')
			commands.should include('command' => 'addRaw',
				'group' => 'baz', 'dayTimestamp' => 4,
				'size' => 18, 'data' => 'this is a sentence')
			commands.should include('command' => 'addRaw',
				'group' => 'test', 'dayTimestamp' => 5,
				'size' => 20, 'data' => 'x' * 20)
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
		
		specify "when synchronizing, large data files are streamed in small chunks" do
			FileUtils.mkdir_p("#{@dbpath}/foo/123")
			File.open("#{@dbpath}/foo/123/data", "wb") do |f|
				f.write("x" * (1024 * 80))
			end
			start_server
			handshake
			
			read_json.should == { 'command' => 'getToc' }
			write_json(
				:foo => {
					'123' => { :size => 10 }
				}
			)
			
			read_json.should == {
				'command' => 'addRaw',
				'group' => 'foo',
				'dayTimestamp' => 123,
				'size' => 32 * 1024
			}
			@connection.read(32 * 1024).should == "x" * (32 * 1024)
			
			read_json.should == {
				'command' => 'addRaw',
				'group' => 'foo',
				'dayTimestamp' => 123,
				'size' => 32 * 1024
			}
			@connection.read(32 * 1024).should == "x" * (32 * 1024)
			
			read_json.should == {
				'command' => 'addRaw',
				'group' => 'foo',
				'dayTimestamp' => 123,
				'size' => 16374
			}
			@connection.read(16374).should == "x" * 16374
		end
		
		it "sends the topology after synchronization finishes"
	end
	
	describe "when joining a slave server" do
		def start_server
			really_start_server(:slave)
		end
		
		def handshake(args = { :identity => 'replica-member' })
			read_json
			write_json(args)
			return read_json
		end
		
		it "informs about both sides' roles during handshake" do
			start_server
			handshake.should == {
				'status' => 'ok',
				'your_role' => 'slave',
				'my_role' => 'slave'
			}
			should_never_happen do
				socket_readable?(@connection)
			end
		end
		
		it "sends the topology"
	end
end