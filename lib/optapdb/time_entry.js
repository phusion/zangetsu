function TimeEntry(dayTimestamp, path, stream, dataFileSize) {
	this.dayTimestamp = dayTimestamp;
	this.path = path;
	this.stream = stream;
	this.dataFileSize = dataFileSize;
}

exports.TimeEntry = TimeEntry;
