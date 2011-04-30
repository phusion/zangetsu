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
	
	def start_server(role = :master)
		@code = %Q{
			var Server = require('optapdb/server');
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
		@server = async_eval_js(@code, :capture => false)
		@connection = TCPSocket.new('127.0.0.1', TEST_SERVER_PORT)
		@connection.sync = true
	end
	
	def handshake(args = { :identity => 'replica-member' })
		read_json
		write_json(args)
		read_json.should == { 'status' => 'ok' }
	end
	
	describe "when connecting slave to master" do
		before :each do
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
		end
		
		specify "the master synchronizes the slave" do
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
		
		specify "the master does not send group removal commands if there are " +
		        "no groups that only exist on the slave" do
			pending
		end
	end
end