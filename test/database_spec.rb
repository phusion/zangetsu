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
		
		it "throws an error if a group is being created while reloading" do
			FileUtils.mkdir_p(@dbpath)
			output, error = eval_js!(%q{
				var sys = require('sys');
				var fs  = require('fs');
				var Database = require('optapdb/database').Database;
				var database = new Database("tmp/db");
				database.findOrCreateGroup('foo', function(err) {
					if (err) {
						sys.print("ERROR: ", err, "\n");
						process.exit(1);
					}
				});
				try {
					database.reload(function(err) {
						if (err) {
							sys.print("ERROR: ", err, "\n");
							process.exit(1);
						}
					});
				} catch (e) {
					sys.print("Got expected error\n");
				}
			})
			output.should == "Got expected error\n"
		end
	end
	
	describe ".findOrCreateGroup" do
		before :each do
			FileUtils.mkdir_p(@dbpath)
			@find_or_create_foo_after_reload = %q{
				var sys = require('sys');
				var Database = require('optapdb/database').Database;
				var database = new Database("tmp/db");
				database.reload(function(err) {
					if (err) {
						sys.print("ERROR: ", err, "\n");
						process.exit(1);
					}
					
					database.findOrCreateGroup('foo', function(err) {
						if (err) {
							sys.print("ERROR: ", err, "\n");
							process.exit(1);
						}
						sys.print("Created\n");
					});
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
				var sys = require('sys');
				var Database = require('optapdb/database').Database;
				var database = new Database("tmp/db");
				require('fs').mkdirSync('tmp/db/foo', 0700);
				database.findOrCreateGroup('foo', function(err) {
					if (err) {
						sys.print("ERROR: ", err, "\n");
						process.exit(1);
					}
					sys.print("Created\n");
				});
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
					
					function callback(err) {
						if (err) {
							sys.print("ERROR: ", err, "\n");
							process.exit(1);
						}
						sys.print("Created\n");
					}
					
					database.findOrCreateGroup('foo', callback);
					database.findOrCreateGroup('foo', callback);
					database.findOrCreateGroup('foo', callback);
				});
			})
			File.directory?(@dbpath + "/foo").should be_true
			count_line(output, "Created").should == 3
		end
	end
end
