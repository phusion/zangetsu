function TimeEntry(dayTimestamp, path, dataFileSize) {
	this.dayTimestamp = dayTimestamp;
	this.path = path;
	this.dataFileSize = dataFileSize;
}

exports.TimeEntry = TimeEntry;
