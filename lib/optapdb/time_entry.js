function TimeEntry(dayTimestamp, path, stream, dataFileSize) {
	this.dayTimestamp = dayTimestamp;
	this.path = path;
	this.stream = stream;
	this.dataFileSize = dataFileSize;
	this.fdref = 0;
	this.onFdRefReachingZero = undefined;
}

TimeEntry.prototype.incFdUsage = function() {
	this.fdref++;
}

TimeEntry.prototype.decFdUsage = function() {
	this.fdref--;
	console.assert(this.fdref >= 0);
	if (this.fdref == 0 && this.onFdRefReachingZero) {
		this.onFdRefReachingZero();
	}
}

TimeEntry.prototype.destroy = function() {
	if (this.fdref == 0) {
		this.stream.destroySoon();
	} else {
		this.onFdRefReachingZero = function() {
			this.stream.destroySoon();
		}
	}
}

exports.TimeEntry = TimeEntry;
