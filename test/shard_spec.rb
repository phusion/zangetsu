# encoding: binary
require File.expand_path(File.dirname(__FILE__) + "/spec_helper")

describe "Shard" do
	# Shard is a proxy for physical shards
	
	def initialize_remote
		@remote_socket = TCPServer.new('127.0.0.1', TEST_SERVER_PORT)
		@remote_socket.listen(50)
	end

	before :each do
		@shard_code = %Q{
			var Shard = require('zangetsu/shard').Shard;
			var shard = new Shard(
				{
					hostname : '127.0.0.1',
					port: #{TEST_SERVER_PORT}
				}
			);
		}
	end
	
	describe "connect" do
		it "should connect to the shard" do
			code = @shard_code + %Q{
				shard.connect(function() {
					console.log("connected");
				});
			}
			initialize_remote
			proc = async_eval_js code, :capture => true
			c = @remote_socket.accept
			eventually do
				output = proc.output
				puts output if output != ""
				proc.output == "connected"
			end
			c.close
			@remote_socket.close
		end
	end
end
