# encoding: binary
require File.expand_path(File.dirname(__FILE__) + "/spec_helper")

describe "Database" do
	before :each do
		@dbpath = 'tmp/db'
	end
	
	describe ".reload" do
		before :each do
			@code = %q{
				var sys = require('sys');
				var Database = require('optapdb/database').Database;
				var database = new Database("tmp/db");
				database.reload(function(err) {
					if (err) {
						sys.print(err, "\n");
						return;
					}
					
					sys.print("success\n");
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
				});
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
			output, error = eval_js!(@code)
			output.should include("Cannot read directory #{@dbpath}")
			output.should include("ENOENT")
		end
	
		it "generates an error if the database directory is not readable" do
			FileUtils.mkdir_p(@dbpath + "/foo")
			File.chmod(0300, @dbpath)
			output, error = eval_js!(@code)
			output.should include("Cannot read directory #{@dbpath}")
			output.should include("EACCES")
		end
	
		it "generates an error if the database directory is not executable" do
			FileUtils.mkdir_p(@dbpath + "/foo")
			File.chmod(0600, @dbpath)
			output, error = eval_js!(@code)
			output.should include("Cannot stat #{@dbpath}/foo")
			output.should include("EACCES")
		end
	
		it "generates an error if a group in the database directory is not readable" do
			FileUtils.mkdir_p(@dbpath + "/foo/123")
			File.chmod(0300, @dbpath + "/foo")
			output, error = eval_js!(@code)
			output.should include("Cannot read directory #{@dbpath}/foo")
			output.should include("EACCES")
		end
	
		it "generates an error if a group in the database directory is not executable" do
			FileUtils.mkdir_p(@dbpath + "/foo/123")
			File.chmod(0600, @dbpath + "/foo")
			output, error = eval_js!(@code)
			output.should include("Cannot stat directory #{@dbpath}/foo/123")
			output.should include("EACCES")
		end
	
		it "doesn't generate an error if a time entry in the database directory is not readable" do
			FileUtils.mkdir_p(@dbpath + "/foo/123")
			File.chmod(0300, @dbpath + "/foo/123")
			output, error = eval_js!(@code)
			output.should include("success")
		end
	
		it "generates an error if a time entry in the database directory is not executable" do
			FileUtils.mkdir_p(@dbpath + "/foo/123")
			File.chmod(0600, @dbpath + "/foo/123")
			output, error = eval_js!(@code)
			output.should include("Cannot stat data file #{@dbpath}/foo/123/data")
			output.should include("EACCES")
		end
		
		it "creates empty data files in empty time entry directories" do
			FileUtils.mkdir_p(@dbpath + "/foo/123")
			eval_js!(@code)
			File.exist?(@dbpath + "/foo/123/data").should be_true
		end
	end
	
	describe ".findOrCreateGroup" do
		before :each do
			FileUtils.mkdir_p(@dbpath)
			@find_or_create_foo_after_reload = %q{
				var Database = require('optapdb/database').Database;
				var database = new Database("tmp/db");
				database.reload(function(err) {
					if (err) {
						console.log("ERROR:", err);
						process.exit(1);
					}
					
					try {
						database.findOrCreateGroup('foo');
						console.log("Created");
					} catch (err) {
						console.log("ERROR:", err);
						process.exit(1);
					}
				});
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
			output, error = eval_js!(%q{
				var fs = require('fs');
				var Database = require('optapdb/database').Database;
				var database = new Database("tmp/db");
				fs.mkdirSync('tmp/db/foo', 0700);
				database.findOrCreateGroup('foo');
				console.log("Created");
			})
			File.directory?(@dbpath + "/foo").should be_true
			output.should include("Created")
		end
		
		it "works if invoked multiple times without waiting for the callback" do
			output, error = eval_js!(%q{
				var sys = require('sys');
				var Database = require('optapdb/database').Database;
				var database = new Database("tmp/db");
				database.reload(function(err) {
					if (err) {
						sys.print("ERROR: ", err, "\n");
						process.exit(1);
					}
					
					database.findOrCreateGroup('foo');
					sys.print("Created\n");
					database.findOrCreateGroup('foo');
					sys.print("Created\n");
					database.findOrCreateGroup('foo');
					sys.print("Created\n");
				});
			})
			File.directory?(@dbpath + "/foo").should be_true
			count_line(output, "Created").should == 3
		end
	end
	
	describe ".findOrCreateTimeEntry" do
		before :each do
			FileUtils.mkdir_p(@dbpath)
			@code = %q{
				var Database = require('optapdb/database').Database;
				var database = new Database("tmp/db");
				database.reload(function(err) {
					if (err) {
						console.log("ERROR:", err);
						process.exit(1);
					}
					var timeEntry = database.findOrCreateTimeEntry('foo', 123);
					console.log("Created: size =", timeEntry.size);
				});
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
			output, error = eval_js!(%q{
				var Database = require('optapdb/database').Database;
				var database = new Database("tmp/db");
				var timeEntry = database.findOrCreateTimeEntry('foo', 123);
				console.log("Created: size =", timeEntry.dataFileSize);
			})
			puts output
			output.should include("Created: size = 3\n")
		end
	end
	
	describe ".add" do
		before :each do
			FileUtils.mkdir_p(@dbpath + "/foo/123")
		end
		
		it "works" do
			output, error = eval_js!(%q{
				var Database = require('optapdb/database.js').Database;
				var database = new Database("tmp/db");
				var buffers  = [new Buffer("hello "), new Buffer("world")];
				var checksum = new Buffer("1234");
				database.add("foo", 123, buffers, checksum, function(err) {
					if (err) {
						console.log(err);
						process.exit(1);
					}
					console.log('Success');
				});
			})
			contents = File.read(@dbpath + "/foo/123/data")
			contents.should ==
				# Header
				"ET" +
				# Length
				["hello world".size].pack('N') +
				# Checksum
				"1234" +
				# Data
				"hello world"
		end
	end
	
	describe ".remove" do
		before :each do
			FileUtils.mkdir_p(@dbpath + "/foo")
		end
		
		it "works" do
			output, error = eval_js!(%q{
				var Database = require('optapdb/database.js').Database;
				var database = new Database("tmp/db");
				database.reload(function(err) {
					if (err) {
						console.log("Error:", err);
						process.exit(1);
					}
					database.remove("foo", undefined, function() {
						console.log("Removed");
					});
				});
			})
			output.should == "Removed\n"
			File.exist?(@dbpath + "/foo").should be_false
		end
	end
end
