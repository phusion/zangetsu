/*
 * Each data file in the database consists of an array of entry records
 * with the following format:
 *
 *   Magic        4 bytes             Always equal to the value "ET"
 *   Data size    4 bytes             In big endian format.
 *   Data         'Data size' bytes   The actual data.
 *   CRC32        4 bytes             Checksum of the data.
 *   Reserved     8 bytes             Reserved for future use.
 *   Data size    4 bytes             In big endian format.
 *   Magic        4 bytes             Always equal to the value "TE"
 *
 * The fact that the checksum comes after the data is to allow adding
 * data in a streaming manner without buffering it in memory first and
 * without seeking backwards into the file.
 * The fact that the data size is duplicated in the footer is to allow
 * reading entries in reverse order.
 */

var fs      = require('fs');
var IOUtils = require('./io_utils.js');
var CRC32   = require('./crc32.js');
var NativeSupport = require('./native_support.node');

var min = Math.min;

const HEADER_MAGIC      = new Buffer("ZaET");
const SIZE_ENTRY_SIZE   = 4;
const HEADER_SIZE       = HEADER_MAGIC.length + SIZE_ENTRY_SIZE;

const FOOTER_MAGIC      = new Buffer("TEaZ");
const CRC32_BINARY_SIZE = 4;
const RESERVED_DATA     = new Buffer("\0\0\0\0\0\0\0\0");
const FOOTER_SIZE       = FOOTER_MAGIC.length + CRC32_BINARY_SIZE + RESERVED_DATA.length + SIZE_ENTRY_SIZE;

const MAX_SIZE = 512 * 1024;
const MAX_SIZE_DESCRIPTION = "512 KB";


function TimeEntry(database, objectId, dayTimestamp, path, stream, dataFileSize) {
	/****** Public read-only ******/
	
	this.database = database;
	this.objectId = objectId;
	this.dayTimestamp = dayTimestamp;
	this.path = path;
	this.stream = stream;
	
	/* Size of the data file including any pending writes. Whenever a write
	 * operation starts, this value is incremented by the expected size
	 * growth. Thus, the actual file size may be temporarily less than
	 * this value. This value can only increase.
	 */
	this.dataFileSize = dataFileSize;
	
	/* Number of bytes in the file for which we know the data is done written.
	 * Unlike dataFileSize, writtenSize is increment after a write operation
	 * is done, not before. Eventually writtenSize will 'catch up' to
	 * dataFileSize. This value can only increase.
	 *
	 * Invariant:
	 *   writtenSize <= dataFileSize
	 */
	this.writtenSize = dataFileSize;
	
	/****** Private ******/
	
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
	
	/** To be called when close() is finished. */
	this.closeCallback = undefined;
	
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
	if (this.closeCallback) {
		var cb = this.closeCallback;
		delete this.closeCallback;
		cb();
	}
}

TimeEntry.prototype.incReadOperations = function() {
	console.assert(!this.closed);
	this.readOperations++;
}

TimeEntry.prototype.decReadOperations = function() {
	console.assert(!this.closed);
	this.readOperations--;
	console.assert(this.readOperations >= 0);
	this._tryCloseNowIfRequested();
}

TimeEntry.prototype.incWriteOperations = function() {
	console.assert(!this.closed);
	this.writeOperations++;
}

TimeEntry.prototype.decWriteOperations = function() {
	console.assert(!this.closed);
	this.writeOperations--;
	console.assert(this.writeOperations >= 0);
	this._tryCloseNowIfRequested();
}

TimeEntry.prototype.equals = function(other) {
	return this.objectId == other.objectId;
}

TimeEntry.prototype.hashCode = function() {
	return this.objectId;
}

TimeEntry.prototype.isClosed = function() {
	return this.closed || this.closing;
}

TimeEntry.prototype.close = function(callback) {
	if (!this.closed && !this.closing) {
		delete this.database;
		this.closeCallback = callback;
		if (this.readOperations == 0 && this.writeOperations == 0) {
			this._closeNow();
		} else {
			this.closing = true;
		}
	}
}

TimeEntry.prototype.get = function(offset, callback) {
	console.assert(!this.isClosed());
	
	if (offset + HEADER_SIZE + FOOTER_SIZE > this.writtenSize) {
		// Invalid offset.
		callback('Invalid offset');
		return;
	}
	
	/* Preallocate slightly larger buffer so that we don't have to allocate two
	 * buffers in case the entry is < ~4 KB. The '- 3 * 8' is to account for
	 * malloc overhead so that a single allocation fits in a single page.
	 */
	console.assert(HEADER_SIZE + FOOTER_SIZE <= 1024 * 4 - 3 * 8);
	var self = this;
	var buf = new Buffer(1024 * 4 - 3 * 8);
	
	this.incReadOperations();
	fs.read(this.stream.fd, buf, 0, HEADER_SIZE, offset, function(err, bytesRead) {
		console.assert(!self.closed);
		var i, size;
		
		if (err) {
			self.decReadOperations();
			self._verifyInvariants();
			callback(err);
			return;
		} else if (bytesRead != HEADER_SIZE) {
			// Invalid offset or corrupt file.
			self.decReadOperations();
			self._verifyInvariants();
			callback('Invalid offset or data file corrupted (entry too small)');
			return;
		}
		
		// Check header magic.
		for (i = 0; i < HEADER_MAGIC.length; i++) {
			if (buf[i] != HEADER_MAGIC[i]) {
				// Invalid offset or corrupt file.
				self.decReadOperations();
				self._verifyInvariants();
				callback('Invalid offset or data file corrupted (invalid magic)');
				return;
			}
		}
		
		// Check size.
		size = IOUtils.bufferToUint(buf, HEADER_MAGIC.length);
		if (size > MAX_SIZE) {
			// Probably corrupt file.
			self.decReadOperations();
			self._verifyInvariants();
			callback('Data file corrupted (invalid size field in header)');
			return;
		}
		
		if (size > buf.length) {
			buf = new Buffer(size);
		}
		fs.read(self.stream.fd, buf, 0, size + FOOTER_SIZE, offset + HEADER_SIZE,
			function(err, bytesRead)
		{
			self.decReadOperations();
			self._verifyInvariants();
			
			if (err) {
				callback(err);
				return;
			} else if (bytesRead != size + FOOTER_SIZE) {
				// Probably corrupted file.
				callback('Data file corrupted (last record appears to be truncated)');
				return;
			}
			
			var checksum = CRC32.calculate(buf, 0, size);
			var storedChecksum = IOUtils.bufferToUint(buf,
				size, CRC32_BINARY_SIZE);
			if (checksum != storedChecksum) {
				// Probably corrupt file.
				callback('Data file corrupted (invalid checksum in header)');
				return;
			}
			
			var storedSize = IOUtils.bufferToUint(buf,
				size + CRC32_BINARY_SIZE + RESERVED_DATA.length,
				SIZE_ENTRY_SIZE);
			if (storedSize != size) {
				// Probably corrupt file.
				callback('Data file corrupted (invalid size field in footer)');
				return;
			}
			
			// Check footer magic.
			for (i = 0; i < FOOTER_MAGIC.length; i++) {
				if (buf[size + CRC32_BINARY_SIZE + RESERVED_DATA.length + SIZE_ENTRY_SIZE + i] != FOOTER_MAGIC[i]) {
					// Probably corrupt file.
					callback('Data file corrupted (invalid checksum in footer)');
					return;
				}
			}
			
			callback(undefined, buf.slice(0, size),
				HEADER_SIZE + size + FOOTER_SIZE,
				offset + HEADER_SIZE + size + FOOTER_SIZE);
		});
	});
}

TimeEntry.prototype.add = function(buffers, checksumBuffer, callback) {
	console.assert(!this.isClosed());
	console.assert(checksumBuffer.length == CRC32.BINARY_SIZE);
	
	var totalDataSize = 0;
	var i;
	for (i = 0; i < buffers.length; i++) {
		totalDataSize += buffers[i].length;
	}
	console.assert(totalDataSize < MAX_SIZE);
	
	var self = this;
	var prevOffset = this.dataFileSize;
	var headerAndFooter = new Buffer(HEADER_SIZE + FOOTER_SIZE);
	var header = headerAndFooter.slice(0, HEADER_SIZE);
	var footer = headerAndFooter.slice(HEADER_SIZE);
	var totalSize = HEADER_SIZE + totalDataSize + FOOTER_SIZE;
	
	function written(err) {
		console.assert(!self.closed);
		if (err) {
			self.dataFileSize -= totalSize;
		} else {
			self.writtenSize += totalSize;
		}
		if (err) {
			self.decWriteOperations();
			self._verifyInvariants();
			callback(err);
		} else {
			self.decWriteOperations();
			self._verifyInvariants();
			callback(undefined, prevOffset, totalSize, buffers);
		}
	}
	
	HEADER_MAGIC.copy(header);
	IOUtils.uintToBuffer(totalDataSize, header, HEADER_MAGIC.length);
	
	checksumBuffer.copy(footer);
	RESERVED_DATA.copy(footer, CRC32_BINARY_SIZE);
	IOUtils.uintToBuffer(totalDataSize, footer,
		CRC32_BINARY_SIZE + RESERVED_DATA.length);
	FOOTER_MAGIC.copy(footer,
		CRC32_BINARY_SIZE + RESERVED_DATA.length + SIZE_ENTRY_SIZE);
	
	this.incWriteOperations();
	this.dataFileSize += totalSize;
	this.stream.write(header);
	for (i = 0; i < buffers.length; i++) {
		this.stream.write(buffers[i]);
	}
	this.stream.write(footer, written);
}

TimeEntry.prototype.each = function(startOffset, callback) {
	console.assert(!this.isClosed());
	var self = this;
	
	function readNext(pos) {
		if (pos >= self.writtenSize) {
			self.evict();
			self.decReadOperations();
			self._verifyInvariants();
			callback(undefined, new Buffer(0));
			return;
		}
		
		self.get(pos, function(err, buf, rawSize, nextOffset) {
			if (err) {
				self.evict();
				self.decReadOperations();
				self._verifyInvariants();
				callback(err);
			} else {
				var stopped = false;
				
				function continueReading() {
					if (!stopped) {
						stopped = true;
						readNext(nextOffset);
					}
				}
				
				function stop() {
					if (!stopped) {
						stopped = true;
						self.evict();
						self.decReadOperations();
						self._verifyInvariants();
					}
				}
				
				callback(undefined, buf, rawSize, continueReading, stop);
			}
		});
	}
	
	this.incReadOperations();
	readNext(startOffset);
}

TimeEntry.prototype.sync = function(callback) {
	console.assert(!this.isClosed());
	var self = this;
	
	function synched(err) {
		self.decWriteOperations();
		self._verifyInvariants();
		callback(err);
	}
	
	this.incWriteOperations();
	if (NativeSupport.sync_file_range) {
		NativeSupport.sync_file_range(
			this.stream.fd,
			0,
			this.writtenSize,
			NativeSupport.SYNC_FILE_RANGE_WAIT_BEFORE |
				NativeSupport.SYNC_FILE_RANGE_WRITE |
				NativeSupport.SYNC_FILE_RANGE_WAIT_AFTER,
			synched);
	} else {
		NativeSupport.fdatasync(this.stream.fd, synched);
	}
}

TimeEntry.prototype.evict = function(callback) {
	console.assert(!this.isClosed());
	var self = this;
	
	function evicted(err) {
		self.decWriteOperations();
		self._verifyInvariants();
		if (callback) {
			callback(err);
		}
	}
	
	this.incWriteOperations();
	if (NativeSupport.posix_fadvise) {
		NativeSupport.posix_fadvise(
			self.stream.fd,
			0,
			this.writtenSize,
			NativeSupport.POSIX_FADV_DONTNEED,
			evicted);
	} else {
		evicted();
	}
}

exports.calculateRecordSize = function(buffers) {
	var i;
	var size = 0;

	size += HEADER_SIZE;
	for (i = 0; i < buffers.length; i++) {
		size += buffers[i].length;
	}
	size += FOOTER_SIZE;
	
	return size;
}

exports.TimeEntry   = TimeEntry;
exports.MAX_SIZE    = MAX_SIZE;
exports.MAX_SIZE_DESCRIPTION = MAX_SIZE_DESCRIPTION;
exports.HEADER_SIZE = HEADER_SIZE;
exports.FOOTER_SIZE = FOOTER_SIZE;
