require 'fileutils'

module SpecHelper
	ROOT = File.expand_path(File.dirname(__FILE__) + "/..")
	
	def eval_js(js, input = nil)
		script_file = "/tmp/javascript-#{Process.pid}-#{Thread.current.object_id}.js"
		input_file  = "/tmp/input-#{Process.pid}-#{Thread.current.object_id}.txt"
		output_file = "/tmp/output-#{Process.pid}-#{Thread.current.object_id}.txt"
		error_file  = "/tmp/error-#{Process.pid}-#{Thread.current.object_id}.txt"
		File.open(script_file.strip, "w") do |f|
			f.write(js)
		end
		File.open(input_file, "w") do |f|
			f.write(input) if input
		end
		File.open(output_file, "w").close
		File.open(error_file, "w").close
		pid = fork do
			ENV['NODE_PATH'] = "#{ROOT}/lib"
			STDIN.reopen(input_file, "r")
			STDOUT.reopen(output_file, "w")
			STDERR.reopen(error_file, "w")
			exec("node", script_file)
		end
		Process.waitpid(pid)
		return [$?.exitstatus,
			File.read(output_file),
			File.read(error_file)]
	ensure
		File.unlink(script_file) rescue nil if script_file
		File.unlink(input_file) rescue nil if input_file
		File.unlink(output_file) rescue nil if output_file
		File.unlink(error_file) rescue nil if error_file
	end
	
	def eval_js!(js, input = nil)
		exit_code, output, error = eval_js(js)
		if exit_code == 0
			return [output, error]
		else
			raise "Script failed with exit code #{exit_code}!\n" +
				"Output:\n  #{output}\n" +
				"Error:\n  #{error}"
		end
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
