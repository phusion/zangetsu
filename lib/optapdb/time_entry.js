/*
 * Each data file in the database consists of an array of entry records
 * with the following format:
 *
 *   Magic        2 bytes             Always equal to the value "ET"
 *   Data size    4 bytes             In big endian format.
 *   CRC32        4 bytes             Checksum of the data.
 *   Data         'Data size' bytes   The actual data.
 */

var fs          = require('fs');
var BufferUtils = require('./buffer_utils.js');
var CRC32       = require('./crc32.js');

const ENTRY_MAGIC       = new Buffer("ET");
const SIZE_ENTRY_SIZE   = 4;
const CRC32_BINARY_SIZE = 4;
const HEADER_SIZE       = ENTRY_MAGIC.length + SIZE_ENTRY_SIZE + CRC32_BINARY_SIZE;

const MAX_SIZE = 1024 * 1024;
const MAX_SIZE_DESCRIPTION = "1 MB";

function TimeEntry(dayTimestamp, path, stream, dataFileSize) {
	/*** Public read-only ***/
	
	this.dayTimestamp = dayTimestamp;
	this.path = path;
	this.stream = stream;
	this.dataFileSize = dataFileSize;
	
	/* Number of bytes in the file for which we know the data is done written.
	 * Invariant:
	 *   writtenSize <= dataFileSize
	 */
	this.writtenSize = dataFileSize;
	
	/*** Private ***/
	
	/* Whether this TimeEntry is being closed.
	 * Invariant:
	 *   if closed:
	 *     !closing
	 */
	this.closing = false;
	
	/* Whether this TimeEntry is closed.
	 * Invariant:
	 *   if closed:
	 *     readOperations == 0
	 *     writeOperations == 0
	 */
	this.closed = false;
	
	/* Number of unfinished read operations. */
	this.readOperations = 0;
	/* Number of unfinished write operations. */
	this.writeOperations = 0;
}

TimeEntry.prototype._verifyInvariants = function() {
	// !a || b: logical equivalent of a IMPLIES b.
	console.assert(this.writtenSize <= this.dataFileSize);
	console.assert(!(this.closed) || !this.closing);
	console.assert(!(this.closed) || (this.readOperations == 0 && this.writeOperations == 0));
}

TimeEntry.prototype._incReadOperations = function() {
	console.assert(!this.closed);
	this.readOperations++;
}

TimeEntry.prototype._decReadOperations = function() {
	console.assert(!this.closed);
	this.readOperations--;
	console.assert(this.readOperations >= 0);
	this._tryCloseNowIfRequested();
}

TimeEntry.prototype._incWriteOperations = function() {
	console.assert(!this.closed);
	this.writeOperations++;
}

TimeEntry.prototype._decWriteOperations = function() {
	console.assert(!this.closed);
	this.writeOperations--;
	console.assert(this.writeOperations >= 0);
	this._tryCloseNowIfRequested();
}

TimeEntry.prototype._tryCloseNowIfRequested = function() {
	console.assert(!this.closed);
	if (this.closing && this.readOperations == 0 && this.writeOperations == 0) {
		this._closeNow();
	}
}

TimeEntry.prototype._closeNow = function() {
	console.assert(!this.closed);
	this.stream.destroySoon();
	this.closing = false;
	this.closed = true;
	this._verifyInvariants();
}

TimeEntry.prototype.close = function() {
	if (!this.closed) {
		if (this.readOperations == 0 && this.writeOperations == 0) {
			this._closeNow();
		} else {
			this.closing = true;
		}
	}
}

TimeEntry.prototype.get = function(offset, callback) {
	console.assert(!this.closing && !this.closed);
	
	if (this.writtenSize < offset + HEADER_SIZE) {
		// Invalid offset.
		callback('not-found');
		return;
	}
	
	/* Preallocate slightly larger buffer so that we don't have to allocate two
	 * buffers in case the entry is < ~4 KB. The '- 3 * 8' is to account for
	 * malloc overhead so that a single allocation fits in a single page.
	 */
	console.assert(HEADER_SIZE <= 1024 * 4 - 3 * 8);
	var self = this;
	var buf = new Buffer(1024 * 4 - 3 * 8);
	
	this._incReadOperations();
	fs.read(this.stream.fd, buf, 0, HEADER_SIZE, offset, function(err, bytesRead) {
		console.assert(!self.closed);
		var i, size, storedChecksum;
		
		if (err) {
			self._decReadOperations();
			self._verifyInvariants();
			callback(err);
			return;
		} else if (bytesRead != HEADER_SIZE) {
			// Invalid offset or corrupt file.
			self._decReadOperations();
			self._verifyInvariants();
			callback('not-found');
			return;
		}
		
		// Check magic.
		for (i = 0; i < ENTRY_MAGIC.length; i++) {
			if (buf[i] != ENTRY_MAGIC[i]) {
				// Invalid offset or corrupt file.
				self._decReadOperations();
				self._verifyInvariants();
				callback('not-found');
				return;
			}
		}
		
		// Check size.
		size = BufferUtils.bufferToUint(buf, ENTRY_MAGIC.length);
		if (size > MAX_SIZE) {
			// Probably corrupt file..
			self._decReadOperations();
			self._verifyInvariants();
			callback('not-found');
			return;
		}
		
		storedChecksum = BufferUtils.bufferToUint(buf,
			ENTRY_MAGIC.length + SIZE_ENTRY_SIZE);
		
		if (size > buf.length) {
			buf = new Buffer(size);
		}
		fs.read(self.stream.fd, buf, 0, size, offset + HEADER_SIZE,
			function(err, bytesRead)
		{
			self._decReadOperations();
			self._verifyInvariants();
			
			if (err) {
				callback(err);
				return;
			} else if (bytesRead != size) {
				// What's going on?
				callback('not-found');
				return;
			}
			
			var checksum = CRC32.calculate(buf, 0, size);
			if (checksum == storedChecksum) {
				if (buf.length != size) {
					buf = buf.slice(0, size);
				}
				callback(undefined, buf);
			} else {
				// Probably corrupt file.
				callback('not-found');
			}
		});
	});
}

TimeEntry.prototype.add = function(buffers, checksumBuffer, callback) {
	console.assert(!this.closing && !this.closed);
	console.assert(checksumBuffer.length == CRC32.BINARY_SIZE);
	
	var totalSize = 0;
	for (i = 0; i < buffers.length; i++) {
		totalSize += buffers[i].length;
	}
	console.assert(totalSize < MAX_SIZE);
	
	var flushed, i;
	var prevOffset = this.dataFileSize;
	
	var self = this;
	function written(err) {
		console.assert(!this.closed);
		self.writtenSize += HEADER_SIZE + totalSize;
		self._decWriteOperations();
		self._verifyInvariants();
		if (err) {
			callback(err);
		} else {
			callback(undefined, prevOffset);
		}
	}
	
	var buf = new Buffer(HEADER_SIZE);
	ENTRY_MAGIC.copy(buf);
	BufferUtils.uintToBuffer(totalSize, buf, ENTRY_MAGIC.length);
	checksumBuffer.copy(buf, ENTRY_MAGIC.length + SIZE_ENTRY_SIZE);
	
	this._incWriteOperations();
	flushed = this.stream.write(buf,
		// Install callback on last buffer.
		(buffers.length == 0) ? written : undefined);
	this.dataFileSize += HEADER_SIZE;
	console.assert(!flushed, "Code assumes that file I/O is never flushed immediately");
	
	for (i = 0; i < buffers.length; i++) {
		flushed = this.stream.write(buffers[i],
			// Install callback on last buffer.
			(i == buffers.length - 1) ? written : undefined);
		console.assert(!flushed, "Code assumes that file I/O is never flushed immediately");
	}
	this.dataFileSize += totalSize;
}

exports.TimeEntry = TimeEntry;
exports.MAX_SIZE  = MAX_SIZE;
exports.MAX_SIZE_DESCRIPTION = MAX_SIZE_DESCRIPTION;
