function Group(name, path) {
	this.name = name;
	this.path = path;
	this.timeEntryCount = 0;
	this.timeEntries = {};
}

exports.Group = Group;
