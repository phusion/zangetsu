require 'rubygems'
require 'fileutils'
require 'socket'
require 'fcntl'
require 'timeout'
require 'json'

TEST_SERVER_PORT  = 3765
TEST_SERVER_PORT2 = 3766
DEBUG = ['1', 'yes', 'y', 'true'].include?(ENV['DEBUG'])

module SpecHelper
	ROOT = File.expand_path(File.dirname(__FILE__) + "/..")
	
	class BackgroundNodeProcess
		attr_accessor :script_file, :pid
		attr_accessor :input_file, :output_file, :error_file
		
		def closed?
			return !@pid
		end
		
		def output
			if closed? || @output
				return @output
			else
				return File.read(@output_file)
			end
		end
		
		def error
			if closed? || @error
				return @error
			else
				return File.read(@error_file)
			end
		end
		
		def kill
			raise "Already closed" if closed?
			Process.kill('SIGTERM', @pid)
		end
		
		def close(kill = true)
			raise "Already closed" if closed?
			self.kill if kill
			Process.waitpid(@pid)
			@pid    = nil
			@output = File.read(@output_file)
			@error  = File.read(@error_file)
			return [$?.exitstatus, @output, @error]
		ensure
			discard
		end
		
		def discard
			File.unlink(@script_file) rescue nil if @script_file
			File.unlink(@input_file) rescue nil if @input_file
			File.unlink(@output_file) rescue nil if @output_file
			File.unlink(@error_file) rescue nil if @error_file
			@script_file = @input_file = @output_file = @error_file = nil
		end
	end
	
	def eval_js(js, options = {})
		proc = async_eval_js(js, options)
		return proc.close(false)
	ensure
		proc.discard if proc
	end
	
	def eval_js!(js, options = {})
		exit_code, output, error = eval_js(js, options)
		if exit_code == 0
			return [output, error]
		else
			raise "Script failed with exit code #{exit_code}!\n" +
				"Output:\n  #{output}\n" +
				"Error:\n  #{error}"
		end
	end
	
	def async_eval_js(js, options = {})
		@_counter = 0 if !@_counter
		@_counter += 1
		tail = "#{Process.pid}-#{Thread.current.object_id}-#{@_counter}"
		proc = BackgroundNodeProcess.new
		proc.script_file = "/tmp/javascript-#{tail}.js"
		proc.input_file  = "/tmp/input-#{tail}.txt"
		proc.output_file = "/tmp/output-#{tail}.txt"
		proc.error_file  = "/tmp/error-#{tail}.txt"
		File.open(proc.script_file, "w") do |f|
			f.write(js.strip)
		end
		File.open(proc.input_file, "w") do |f|
			f.write(options[:input]) if options[:input]
		end
		File.open(proc.output_file, "w").close
		File.open(proc.error_file, "w").close
		proc.pid = fork do
			ENV['NODE_PATH'] = "#{ROOT}/lib"
			STDIN.reopen(proc.input_file, "r")
			if !options.has_key?(:capture) || options[:capture]
				STDOUT.reopen(proc.output_file, "w")
				STDERR.reopen(proc.error_file, "w")
			end
			exec("node", proc.script_file)
		end
		return proc
	rescue Exception => e
		proc.discard if proc
		raise e
	end
	
	def wait_for_port(port = TEST_SERVER_PORT)
		Timeout.timeout(5) do
			while true
				begin
					puts 'connecting'
					socket = TCPSocket.new('127.0.0.1', port)
					puts 'connected'
					socket.sync = true
					return socket
				rescue Errno::ECONNREFUSED
					puts 'refused'
					sleep 0.1
				end
			end
		end
	rescue Timeout::Error
		raise "Cannot connect in time"
	end
	
	def read_json(socket = @connection)
		return JSON.parse(socket.readline)
	end
	
	def write_json(socket, object = nil)
		if !socket.respond_to?(:write)
			object = socket
			socket = @connection
		end
		socket.write("#{JSON.generate(object)}\n")
	end
	
	def socket_readable?(socket)
		return !!select([socket], nil, nil, 0)
	end
	
	def should_never_happen(max_wait = 0.5, sleep_time = 0.001)
		deadline = Time.now + max_wait
		while Time.now < deadline
			result = yield
			if result
				violated "Something that should never happen happened anyway"
			else
				sleep(sleep_time)
			end
		end
	end
	
	def eventually(max_wait = 0.5, sleep_time = 0.001)
		deadline = Time.now + max_wait
		while Time.now < deadline
			result = yield
			if result
				return result
			else
				sleep(sleep_time)
			end
		end
		violated "Something that should eventually happen never happened"
	end
end

Spec::Runner.configure do |config|
	config.include SpecHelper
	
	config.after :each do
		if File.exist?("tmp")
			system("chmod -R +rwx tmp")
			FileUtils.rm_rf('tmp')
		end
	end
end
