# encoding: binary
require File.expand_path(File.dirname(__FILE__) + "/spec_helper")

describe "Database" do
	before :each do
		@dbpath = 'tmp/db'
		@header = %Q{
			var sys      = require('sys');
			var Database = require('zangetsu/database');
			var database = new Database.Database('#{@dbpath}');
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
	
	describe ".reload" do
		before :each do
			@code = @header + %q{
				database.reload();
				sys.print(database.groupCount, " groups\n");
				var groupName, group, timeEntryName, timeEntry;
				for (groupName in database.groups) {
					group = database.groups[groupName];
					sys.print("Group ", groupName, ": ",
						group.timeEntryCount, " time entries\n");
					for (timeEntryName in group.timeEntries) {
						timeEntry = group.timeEntries[timeEntryName];
						sys.print("Time entry ", groupName, "/",
							timeEntryName, ": size=",
							timeEntry.dataFileSize, "\n");
					}
				}
			}
		end
		
		it "correctly reads an empty database directory" do
			FileUtils.mkdir_p(@dbpath)
			output, error = eval_js!(@code)
			output.should include("0 groups")
		end
	
		it "correctly reads a populated database directory" do
			FileUtils.mkdir_p(@dbpath + "/foo/1")
			FileUtils.mkdir_p(@dbpath + "/foo/2")
			FileUtils.mkdir_p(@dbpath + "/bar/1")
			FileUtils.mkdir_p(@dbpath + "/bar/3")
			FileUtils.mkdir_p(@dbpath + "/bar/4")
			File.open(@dbpath + "/bar/3/data", "w") do |f|
				f.write("abc")
			end
			output, error = eval_js!(@code)
			output.should include("2 groups")
			output.should include("Group foo: 2 time entries")
			output.should include("Time entry foo/1: size=0")
			output.should include("Time entry foo/2: size=0")
			output.should include("Group bar: 3 time entries")
			output.should include("Time entry bar/1: size=0")
			output.should include("Time entry bar/3: size=3")
			output.should include("Time entry bar/4: size=0")
		end
	
		it "generates an error if the database directory does not exist" do
			status, output, error = eval_js(@code)
			error.should include("No such file or directory '#{@dbpath}'")
			error.should include("ENOENT")
		end
	
		it "generates an error if the database directory is not readable" do
			FileUtils.mkdir_p(@dbpath + "/foo")
			File.chmod(0300, @dbpath)
			status, output, error = eval_js(@code)
			error.should include("Permission denied '#{@dbpath}'")
			error.should include("EACCES")
		end
	
		it "generates an error if the database directory is not executable" do
			FileUtils.mkdir_p(@dbpath + "/foo")
			File.chmod(0600, @dbpath)
			status, output, error = eval_js(@code)
			error.should include("Permission denied '#{@dbpath}/foo'")
			error.should include("EACCES")
		end
	
		it "generates an error if a group in the database directory is not readable" do
			FileUtils.mkdir_p(@dbpath + "/foo/123")
			File.chmod(0300, @dbpath + "/foo")
			status, output, error = eval_js(@code)
			error.should include("Permission denied '#{@dbpath}/foo'")
			error.should include("EACCES")
		end
	
		it "generates an error if a group in the database directory is not executable" do
			FileUtils.mkdir_p(@dbpath + "/foo/123")
			File.chmod(0600, @dbpath + "/foo")
			status, output, error = eval_js(@code)
			error.should include("Permission denied '#{@dbpath}/foo/123'")
			error.should include("EACCES")
		end
	
		it "doesn't generate an error if a time entry in the database directory is not readable" do
			FileUtils.mkdir_p(@dbpath + "/foo/123")
			File.chmod(0300, @dbpath + "/foo/123")
			lambda { eval_js!(@code) }.should_not raise_error
		end
	
		it "generates an error if a time entry in the database directory is not executable" do
			FileUtils.mkdir_p(@dbpath + "/foo/123")
			File.chmod(0600, @dbpath + "/foo/123")
			status, output, error = eval_js(@code)
			error.should include("Permission denied '#{@dbpath}/foo/123/data'")
			error.should include("EACCES")
		end
		
		it "creates empty data files in empty time entry directories" do
			FileUtils.mkdir_p(@dbpath + "/foo/123")
			eval_js!(@code)
			File.exist?(@dbpath + "/foo/123/data").should be_true
		end
	end
	
	describe "._findOrCreateGroup" do
		before :each do
			FileUtils.mkdir_p(@dbpath)
			@find_or_create_foo_after_reload = %Q{
				#{@header}
				database.reload();
				try {
					database._findOrCreateGroup('foo');
					console.log("Created");
				} catch (err) {
					console.log("ERROR:", err);
					process.exit(1);
				}
			}
		end
		
		def count_line(str, line)
			result = 0
			str.split("\n").each do |l|
				if l == line
					result += 1
				end
			end
			return result
		end
		
		it "creates the given group if it doesn't exist" do
			eval_js!(@find_or_create_foo_after_reload)
			File.directory?(@dbpath + "/foo").should be_true
		end
		
		it "returns the given group if it does exist" do
			FileUtils.mkdir_p(@dbpath + "/foo")
			output, error = eval_js!(@find_or_create_foo_after_reload)
			File.directory?(@dbpath + "/foo").should be_true
			output.should include("Created")
		end
		
		it "returns the given group if mkdir fails with EEXIST" do
			output, error = eval_js!(%Q{
				#{@header}
				var fs = require('fs');
				fs.mkdirSync('tmp/db/foo', 0700);
				database._findOrCreateGroup('foo');
				console.log("Created");
			})
			File.directory?(@dbpath + "/foo").should be_true
			output.should include("Created")
		end
		
		it "works if invoked multiple times without waiting for the callback" do
			output, error = eval_js!(@header + %q{
				database.reload();
				database._findOrCreateGroup('foo');
				sys.print("Created\n");
				database._findOrCreateGroup('foo');
				sys.print("Created\n");
				database._findOrCreateGroup('foo');
				sys.print("Created\n");
			})
			File.directory?(@dbpath + "/foo").should be_true
			count_line(output, "Created").should == 3
		end
	end
	
	describe "._findOrCreateTimeEntry" do
		before :each do
			FileUtils.mkdir_p(@dbpath)
			@code = %Q{
				#{@header}
				database.reload();
				var timeEntry = database._findOrCreateTimeEntry('foo', 123);
				console.log("Created: size =", timeEntry.size);
			}
		end
		
		it "creates the given time entry and associated data file if it doesn't exist" do
			output, error = eval_js!(@code)
			File.directory?(@dbpath + "/foo/123").should be_true
			File.file?(@dbpath + "/foo/123/data").should be_true
		end
		
		it "returns the given time entry if it does exist" do
			FileUtils.mkdir_p(@dbpath + "/foo/123")
			File.open(@dbpath + "/foo/123/data", "w").close
			output, error = eval_js!(@code)
			File.directory?(@dbpath + "/foo/123").should be_true
			output.should include("Created")
		end
		
		it "queries the existing data file size if the time entry already exists" do
			FileUtils.mkdir_p(@dbpath + "/foo/123")
			File.open(@dbpath + "/foo/123/data", "w") do |f|
				f.write("abc")
			end
			output, error = eval_js!(%Q{
				#{@header}
				var timeEntry = database._findOrCreateTimeEntry('foo', 123);
				console.log("Created: size =", timeEntry.dataFileSize);
			})
			output.should include("Created: size = 3\n")
		end
	end
	
	describe ".findTimeEntry" do
		before :each do
			FileUtils.mkdir_p(@dbpath)
			@code = %Q{
				#{@header}
				database.reload();
				var timeEntry = database.findTimeEntry('foo', 123);
				if (timeEntry) {
					console.log("Found: size =", timeEntry.dataFileSize);
				} else {
					console.log("Not found");
				}
				console.log("Group in toc:", !!database.groups['foo']);
				console.log("TimeEntry in toc:",
					!!(database.groups['foo'] &&
						database.groups['foo'].timeEntries[123]));
			}
		end
		
		it "returns undefined if the given group directory is not in the table of contents" do
			output, error = eval_js!(@code)
			output.should include("Not found")
			output.should include("Group in toc: false")
			output.should include("TimeEntry in toc: false")
		end
		
		it "returns undefined if the given time entry directory is not in the table of contents" do
			FileUtils.mkdir_p("#{@dbpath}/foo")
			output, error = eval_js!(@code)
			output.should include("Not found")
			output.should include("Group in toc: true")
			output.should include("TimeEntry in toc: false")
		end
		
		it "returns a TimeEntry if it exists in the table of contents" do
			FileUtils.mkdir_p("#{@dbpath}/foo/123")
			File.open("#{@dbpath}/foo/123/data", "w") { |f| f.write("ab") }
			output, error = eval_js!(@code)
			output.should include("Found: size = 2\n")
			output.should include("Group in toc: true")
			output.should include("TimeEntry in toc: true")
		end
	end
	
	describe ".add" do
		before :each do
			FileUtils.mkdir_p(@dbpath)
		end
		
		it "writes the correct data to the filesystem" do
			output, error = eval_js!(%Q{
				#{@header}
				add("foo", 123, [new Buffer("hello "), new Buffer("world")],
					function(offset, size, buffers)
				{
					console.log("Added at", offset);
				});
			})
			contents = File.read(@dbpath + "/foo/123/data")
			contents.should ==
				# Header magic
				"ZaET" +
				# Length
				["hello world".size].pack('N') +
				# Data
				"hello world" +
				# Checksum
				"\x00\x00\x0d\x4a" +
				# Length
				["hello world".size].pack('N') +
				# Footer magic
				"TEaZ"
			output.should == "Added at 0\n"
		end
		
		it "updates various internal statistics" do
			output, error = eval_js!(%Q{
				#{@header}
				add("foo", 123, [new Buffer("hello "), new Buffer("world")],
					function(offset, size, buffers)
				{
					console.log("Added at", offset);
					console.log("WrittenSize",
						database.findTimeEntry('foo', 123).writtenSize);
					add("foo", 123, [new Buffer("hello "), new Buffer("world")],
						function(offset, size, buffers)
					{
						console.log("Added at", offset);
						console.log("DataFileSize",
							database.findTimeEntry('foo', 123).dataFileSize);
						console.log("WrittenSize",
							database.findTimeEntry('foo', 123).writtenSize);
					});
					console.log("DataFileSize",
						database.findTimeEntry('foo', 123).dataFileSize);
					console.log("WrittenSize",
						database.findTimeEntry('foo', 123).writtenSize);
				});
				console.log("DataFileSize",
					database.findTimeEntry('foo', 123).dataFileSize);
				console.log("WrittenSize",
					database.findTimeEntry('foo', 123).writtenSize);
			})
			output.should ==
				"DataFileSize 31\n" +
				"WrittenSize 0\n" +
				
				"Added at 0\n" +
				"WrittenSize 31\n" +
				"DataFileSize 62\n" +
				"WrittenSize 31\n" +
				
				"Added at 31\n" +
				"DataFileSize 62\n" +
				"WrittenSize 62\n"
		end
	end
	
	describe ".get" do
		before :each do
			FileUtils.mkdir_p(@dbpath)
			@add_code = %Q{
				#{@header}
				var buffers  = [new Buffer("hello "), new Buffer("world")];
				database.reload();
				add("foo", 123, buffers, function(offset) {
					console.log("Added at", offset);
				});
			}
		end
		
		def run_get_function(group, day_timestamp, offset)
			return eval_js!(%Q{
				#{@header}
				database.reload();
				database.get("#{group}", #{day_timestamp}, #{offset}, function(err, data) {
					if (err) {
						console.log("Error:", err);
					} else {
						console.log("Data:", data.toString('ascii'));
					}
				});
			})
		end
		
		it "works" do
			eval_js!(@add_code)
			output = eval_js!(@add_code).first
			offset = 4 + 4 + 'hello world'.size + 4 + 4 + 4
			output.should include("Added at #{offset}\n")
			
			output, error = run_get_function('foo', 123, offset)
			output.should include("Data: hello world\n")
		end
		
		it "returns a not-found error if the requested group does not exist" do
			eval_js!(@add_code)
			output, error = run_get_function('bar', 123, 0)
			output.should include("Error: not-found\n")
		end
		
		it "returns a not-found error if the requested time entry does not exist" do
			eval_js!(@add_code)
			FileUtils.mkdir_p("#{@dbpath}/foo/123")
			output, error = run_get_function('foo', 456, 0)
			output.should include("Error: not-found\n")
		end
		
		it "returns a not-found error if an invalid offset" do
			output, error = eval_js!(%Q{
				#{@header}
				add('foo', 1, 'hello world', function(offset) {
					database.get('foo', 1, 1, function(err) {
						console.log("Error: " + err);
					});
				});
			})
			output.should == "Error: Invalid offset or data file corrupted (invalid magic)\n"
		end
		
		it "returns a not-found error if the entry is corrupted" do
			eval_js!(%Q{
				#{@header}
				add('foo', 1, 'hello world')
			})
			File.open("#{@dbpath}/foo/1/data", "r+") do |f|
				f.seek(10, IO::SEEK_SET)
				f.write("x")
			end
			output, error = eval_js!(%Q{
				#{@header}
				database.reload();
				database.get('foo', 1, 0, function(err) {
					console.log("Error: " + err);
				});
			})
			output.should == "Error: Data file corrupted (invalid checksum in header)\n"
		end
	end
	
	describe ".remove" do
		before :each do
			FileUtils.mkdir_p(@dbpath + "/foo/123")
			FileUtils.mkdir_p(@dbpath + "/foo/124")
			FileUtils.mkdir_p(@dbpath + "/foo/125")
			FileUtils.mkdir_p(@dbpath + "/bar")
		end
		
		def dircount(dir)
			return Dir.entries(dir).size - 2 # Ignore '.' and '..'
		end
		
		it "supports deleting an entire group" do
			output, error = eval_js!(%Q{
				#{@header}
				database.reload();
				database.remove("foo", undefined, function() {
					console.log("Removed");
				});
			})
			output.should == "Removed\n"
			File.exist?(@dbpath + "/foo").should be_false
			File.exist?(@dbpath + "/bar").should be_true
			dircount(@dbpath).should == 1
		end
		
		it "can also delete all time entries older than the given timestamp inside a group" do
			output, error = eval_js!(%Q{
				#{@header}
				database.reload();
				database.remove("foo", 125, function() {
					console.log("Removed");
				});
			})
			output.should == "Removed\n"
			File.exist?(@dbpath + "/foo/123").should be_false
			File.exist?(@dbpath + "/foo/124").should be_false
			File.exist?(@dbpath + "/foo/125").should be_true
			File.exist?(@dbpath + "/bar").should be_true
			dircount(@dbpath + "/foo").should == 1
		end
	end
	
	describe ".each" do
		before :each do
			FileUtils.mkdir_p(@dbpath)
			
		end
		
		it "iterates through all entries" do
			output, error = eval_js!(%Q{
				#{@header}
				
				var counter = 4;
				function added() {
					counter--;
					if (counter == 0) {
						startTest();
					}
				}
				
				function startTest() {
					database.findTimeEntry('foo', 1).each(0,
						function(err, buf, rawSize, continueReading, stop)
					{
						if (err) {
							console.log(err);
							process.exit(1);
						}
						if (buf.length > 0) {
							console.log("Entry:", buf.toString('ascii'));
							continueReading();
						} else {
							console.log("EOF");
						}
					});
				}
				
				add('foo', 1, 'hello', added);
				add('foo', 1, 'world', added);
				add('foo', 1, 'another entry', added);
				add('foo', 2, 'test', added);
				add('bar', 2, 'some text', added);
			})
			output.should ==
				"Entry: hello\n" +
				"Entry: world\n" +
				"Entry: another entry\n" +
				"EOF\n"
		end
		
		it "aborts with an error if an invalid offset is given" do
			status, output, error = eval_js(%Q{
				#{@header}
				
				var counter = 3;
				function added() {
					counter--;
					if (counter == 0) {
						startTest();
					}
				}
				
				function startTest() {
					database.findTimeEntry('foo', 1).each(1,
						function(err, buf, continueReading, stop)
					{
						if (err) {
							console.log(err);
							process.exit(1);
						}
						if (buf.length > 0) {
							console.log("Entry:", buf.toString('ascii'));
							continueReading();
						} else {
							console.log("EOF");
						}
					});
				}
				
				add('foo', 1, 'hello', added);
				add('foo', 1, 'world', added);
				add('foo', 1, 'another entry', added);
			})
			status.should == 1
			output.should == "Invalid offset or data file corrupted (invalid magic)\n"
			error.should be_empty
		end
	end
	
	describe "locking" do
		before :each do
			FileUtils.mkdir_p(@dbpath)
			@header << %Q{
				var assert = require('assert');
				
				function afterShortWhile(callback) {
					setTimeout(callback, 10);
				}
			}
		end
		
		it "waits until all database additions are done" do
			output, error = eval_js!(%Q{
				#{@header}
				
				database.addSleepTime = 20;
				add('foo', 1, 'hello', function() {
					console.log("Added");
				});
				assert.ok(database.addOperations > 0);
				
				setTimeout(function() {
					database.lock(function() {
						console.log("Lock done");
						setTimeout(function() {
							database.unlock();
						}, 30);
					});
					console.log("Lock called");
					assert.equal(database.state, Database.LOCKING);
				}, 10);
			})
			output.should ==
				"Lock called\n" +
				"Lock done\n" +
				"Added\n"
		end
		
		it "locks immediately if there are no database additions and no other locks" do
			output, error = eval_js!(%Q{
				#{@header}
				database.lock(function() {
					console.log("Lock done");
				});
				console.log("Lock called");
			})
			output.should ==
				"Lock done\n" +
				"Lock called\n"
		end
		
		it "prevents any database additions until unlocked" do
			eval_js!(%Q{
			#{@header}
			add('foo', 1, 'hello', function() {
				var timeEntry = database.findTimeEntry('foo', 1);
				var dataFileSize = timeEntry.dataFileSize;
				var writtenSize = timeEntry.writtenSize;
				
				database.lock(function() {
					add('foo', 1, 'world');
					afterShortWhile(function() {
						assert.equal(timeEntry.dataFileSize, dataFileSize);
						assert.equal(timeEntry.writtenSize, writtenSize);
						database.unlock();
						
						afterShortWhile(function() {
							assert.notEqual(timeEntry.dataFileSize, dataFileSize);
							assert.notEqual(timeEntry.writtenSize, writtenSize);
						});
					});
				});
			});
			})
		end
		
		it "doesn't prevent removals" do
			output, error = eval_js!(%Q{
				#{@header}
				
				setTimeout(function() {
					database.remove('foo', undefined, function() {
						console.log("Removed");
					});
				}, 10);
				
				database.lock(function() {
					console.log("Locked");
					setTimeout(function() {
						console.log("Unlocking");
						database.unlock();
					}, 20);
				});
			})
			output.should ==
				"Locked\n" +
				"Removed\n" +
				"Unlocking\n"
		end
		
		it "ensures only one lock callback is called at a time until unlocked" do
			output, error = eval_js!(%Q{
				#{@header}
				
				function lockInNextTick(id) {
					process.nextTick(function() {
						database.lock(function() {
							console.log("Lock " + id);
							setTimeout(function() {
								console.log("Unlock " + id);
								database.unlock();
							}, 10);
						});
					});
				}
				
				for (var i = 0; i < 3; i++) {
					lockInNextTick(i + 1);
				}
			})
			output.should ==
				"Lock 1\n" +
				"Unlock 1\n" +
				"Lock 2\n" +
				"Unlock 2\n" +
				"Lock 3\n" +
				"Unlock 3\n"
		end
		
		it "resumes all database additions after unlock" do
			output, error = eval_js!(%Q{
				#{@header}
				
				var counter = 0;
				function added() {
					console.log("Added " + counter);
					counter++;
				}
				
				database.lock(function() {
					add('foo', 1, 'hello', added);
					add('foo', 1, 'hello', added);
					add('foo', 1, 'hello', added);
					
					setTimeout(function() {
						console.log("Unlock");
						database.unlock();
					}, 10);
				});
			})
			output.should ==
				"Unlock\n" +
				"Added 0\n" +
				"Added 1\n" +
				"Added 2\n"
		end
	end
end
