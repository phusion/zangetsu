require File.expand_path(File.dirname(__FILE__) + "/spec_helper")

describe "Toc" do
	before :each do
		@dbpath = 'tmp/db'
		@code = %q{
			var sys = require('sys');
			var Toc = require('mosadb/toc').Toc;
			var toc = new Toc("tmp/db");
			toc.reload(function(err) {
				if (err) {
					sys.print(err, "\n");
				} else {
					sys.print("success\n");
					sys.print(toc.groupCount, " groups\n");
					var groupName, group, timeEntryName, timeEntry;
					for (groupName in toc.groups) {
						group = toc.groups[groupName];
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
end
