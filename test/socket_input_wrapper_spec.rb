# encoding: binary
require File.expand_path(File.dirname(__FILE__) + "/spec_helper")

describe "SocketInputWrapper" do
	before :each do
		@dbpath = 'tmp/db'
		@header = %q{
			var events = require('events');
			var util   = require('util');
			var SocketInputWrapper = require('zangetsu/socket_input_wrapper').SocketInputWrapper;
			
			function DummySocket() {
				events.EventEmitter.call(this);
				this.paused = false;
				this.fd = 999;
			}
			util.inherits(DummySocket, events.EventEmitter);
			
			DummySocket.prototype.pause = function() {
				this.paused = true;
			}
			
			DummySocket.prototype.resume = function() {
				this.paused = false;
			}
			
			DummySocket.prototype.destroy = function() {
				this.fd = null;
				this.destroyed = true;
				this.emit('close');
			}
			
			var socket  = new DummySocket();
			var wrapper = new SocketInputWrapper(socket);
		}
	end
	
	it "emits socket data events" do
		output, error = eval_js!(%Q{
			#{@header}
			wrapper.onData = function(data) {
				console.log('Data:', data.toString('ascii'));
				return data.length;
			}
			socket.emit('data', new Buffer('aaabbb'));
		})
		output.should == "Data: aaabbb\n"
	end

	it "emits socket end events" do
		output, error = eval_js!(%Q{
			#{@header}
			wrapper.onEnd = function() {
				console.log('End');
			}
			socket.emit('end');
		})
		output.should == "End\n"
	end

	it "emits socket end events after all data has been consumed" do
		output, error = eval_js!(%Q{
			#{@header}
			wrapper.onData = function(data) {
				console.log('Data:', data.toString('ascii'));
				return data.length;
			}
			wrapper.onEnd = function() {
				console.log('End');
			}
			socket.emit('data', new Buffer('aaabbb'));
			socket.emit('end');
		})
		output.should ==
			"Data: aaabbb\n" +
			"End\n"
	end

	it "emits socket error events" do
		output, error = eval_js!(%Q{
			#{@header}
			wrapper.onError = function(msg) {
				console.log('Error:', msg);
			}
			socket.emit('error', 'foo');
		})
		output.should == "Error: foo\n"
	end

	it "emits socket error events after all data has been consumed" do
		output, error = eval_js!(%Q{
			#{@header}
			wrapper.onData = function(data) {
				console.log('Data:', data.toString('ascii'));
				return data.length;
			}
			wrapper.onError = function(msg) {
				console.log('Error:', msg);
			}
			socket.emit('data', new Buffer('aaabbb'));
			socket.emit('error', 'foo');
		})
		output.should ==
			"Data: aaabbb\n" +
			"Error: foo\n"
	end
	
	describe "if the onData callback didn't consume everything" do
		it "pauses the socket, re-emits the remaining data in the next tick, " +
		   "then resumes the socket when everything is consumed" do
			output, error = eval_js!(%Q{
				#{@header}
				var counter = 0;
				wrapper.onData = function(data) {
					counter++;
					console.log('onData called; paused:', socket.paused);
					console.log('Data:', data.toString('ascii'));
					if (counter == 1) {
						return 3;
					} else {
						return 1;
					}
				}
				socket.emit('data', new Buffer('aaabbb'));
				console.log("Finished first onData; paused:", socket.paused);
				setTimeout(function() {
					console.log("Finished; paused:", socket.paused);
				}, 10);
			})
			output.should ==
				"onData called; paused: false\n" +
				"Data: aaabbb\n" +
				"Finished first onData; paused: true\n" +
				"onData called; paused: true\n" +
				"Data: bbb\n" +
				"onData called; paused: true\n" +
				"Data: bb\n" +
				"onData called; paused: true\n" +
				"Data: b\n" +
				"Finished; paused: false\n"
		end

		describe "if pause() is called after the data handler" do
			it "pauses the socket and doesn't re-emit remaining data events" do
				output, error = eval_js!(%Q{
					#{@header}
					wrapper.onData = function(data) {
						console.log("Data:", data.toString('ascii'));
						return 1;
					}
					socket.emit('data', new Buffer('aaabbb'));
					wrapper.pause();
					console.log("Paused:", socket.paused);
				})
				output.should ==
					"Data: aaabbb\n" +
					"Paused: true\n"
			end
			
			it "resumes the socket and re-emits remaining data one tick after resume() is called" do
				output, error = eval_js!(%Q{
					#{@header}
					wrapper.onData = function(data) {
						console.log("Data:", data.toString('ascii'));
						return 3;
					}
					socket.emit('data', new Buffer('aaabbb'));
					wrapper.pause();
					console.log("Paused:", socket.paused);
					wrapper.resume();
					console.log("Resumed; paused:", socket.paused);
					setTimeout(function() {
						console.log("Done; paused:", socket.paused);
					}, 5);
				})
				output.should ==
					"Data: aaabbb\n" +
					"Paused: true\n" +
					"Resumed; paused: true\n" +
					"Data: bbb\n" +
					"Done; paused: false\n"
			end
			
			it "doesn't re-emit remaining data if resume() is called, then pause() again" do
				output, error = eval_js!(%Q{
					#{@header}
					wrapper.onData = function(data) {
						console.log("Data:", data.toString('ascii'));
						return 3;
					}
					socket.emit('data', new Buffer('aaabbb'));
					wrapper.pause();
					console.log("Paused:", socket.paused);
					wrapper.resume();
					console.log("Resumed; paused:", socket.paused);
					wrapper.pause();
					console.log("Paused again; paused:", socket.paused);
					setTimeout(function() {
						console.log("Timeout; paused:", socket.paused);
					}, 5);
				})
				output.should ==
					"Data: aaabbb\n" +
					"Paused: true\n" +
					"Resumed; paused: true\n" +
					"Paused again; paused: true\n" +
					"Timeout; paused: true\n"
			end
		end
		
		describe "if pause() is called during the handler" do
			it "pauses the socket and doesn't re-emit remaining data" do
				output, error = eval_js!(%Q{
					#{@header}
					var counter = 0;
					wrapper.onData = function(data) {
						console.log("Data:", data.toString('ascii'));
						counter++;
						if (counter == 1) {
							wrapper.pause();
						}
						return 1;
					}
					socket.emit('data', new Buffer('aaabbb'));
					setTimeout(function() {
						console.log("Timeout; paused:", socket.paused);
					}, 5);
				})
				output.should ==
					"Data: aaabbb\n" +
					"Timeout; paused: true\n"
			end
			
			it "re-emits remaining data one tick after resume() is called" do
				output, error = eval_js!(%Q{
					#{@header}
					var counter = 0;
					wrapper.onData = function(data) {
						console.log("Data:", data.toString('ascii'));
						counter++;
						if (counter == 1) {
							wrapper.pause();
							wrapper.resume();
						}
						return 3;
					}
					socket.emit('data', new Buffer('aaabbb'));
					console.log("Handler done; paused:", socket.paused);
					setTimeout(function() {
						console.log("Timeout; paused:", socket.paused);
					}, 5);
				})
				output.should ==
					"Data: aaabbb\n" +
					"Handler done; paused: true\n" +
					"Data: bbb\n" +
					"Timeout; paused: false\n"
			end
			
			it "doesn't re-emit remaining data if resume() is called, then pause() again" do
				output, error = eval_js!(%Q{
					#{@header}
					var counter = 0;
					wrapper.onData = function(data) {
						console.log("Data:", data.toString('ascii'));
						counter++;
						if (counter == 1) {
							wrapper.pause();
							wrapper.resume();
							wrapper.pause();
						}
						return 3;
					}
					socket.emit('data', new Buffer('aaabbb'));
					console.log("Handler done; paused:", socket.paused);
					setTimeout(function() {
						console.log("Timeout; paused:", socket.paused);
					}, 5);
				})
				output.should ==
					"Data: aaabbb\n" +
					"Handler done; paused: true\n" +
					"Timeout; paused: true\n"
			end
		end
		
		describe "if the socket was disconnected" do
			it "doesn't re-emit the remaining data"
		end
	end
	
	it "pauses the underlying socket" do
		output, error = eval_js!(%Q{
			#{@header}
			wrapper.pause();
			console.log(socket.paused);
			wrapper.resume();
			console.log(socket.paused);
		})
		output.should == "true\nfalse\n"
	end
	
	it "doesn't emit data events if it's paused, but re-emits previously unemitted data events after resume" do
		output, error = eval_js!(%Q{
			#{@header}
			var counter = 0;
			wrapper.onData = function(data) {
				console.log('Data:', data.toString('ascii'));
				counter++;
				if (counter == 1) {
					wrapper.pause();
					return 3;
				} else {
					return 1;
				}
			}
			socket.emit('data', new Buffer('aaabbb'));
			console.log('Emitted');
			wrapper.resume();
		})
		output.should ==
			"Data: aaabbb\n" +
			"Emitted\n" +
			"Data: bbb\n" +
			"Data: bb\n" +
			"Data: b\n"
	end

	it "stops emitting unconsumed data once the socket is closed" do
		output, error = eval_js!(%Q{
			#{@header}
			var counter = 0;
			wrapper.onData = function(data) {
				console.log('Data:', data.toString('ascii'));
				counter++;
				if (counter <= 1) {
					return 2;
				} else {
					socket.destroy();
					return 0;
				}
			}
			socket.emit('data', new Buffer('aaabbb'));
		})
		output.should ==
			"Data: aaabbb\n" +
			"Data: abbb\n"
	end

	it "emits a 'close' event if the underlying socket is closed" do
		output, error = eval_js!(%Q{
			#{@header}
			var counter = 0;
			wrapper.onClose = function() {
				console.log('Closed');
			}
			wrapper.onData = function(data) {
				console.log('Data:', data.toString('ascii'));
				socket.destroy();
				return 2;
			}
			socket.emit('data', new Buffer('aaabbb'));
		})
		output.should ==
			"Data: aaabbb\n" +
			"Closed\n"
	end
end