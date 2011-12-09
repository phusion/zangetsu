/*
 * Each data file in the database consists of an array of records
 * with the following format:
 *
 *   Magic        4 bytes             Always equal to the value "ZaET"
 *   Data size    4 bytes             In big endian format.
 *   Flags        1 byte              See below for more nfo.
 *   Data         'Data size' bytes   The actual data.
 *   CRC32        4 bytes             Checksum of the data.
 *   Reserved     8 bytes             Reserved for future use.
 *   Data size    4 bytes             In big endian format.
 *   Magic        4 bytes             Always equal to the value "TEaZ"
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


/*
 * The flags field is normally set to 0, indicating a normal (healthy)
 * record. If the corruption flag is set then it means that the record
 * is corrupted, and its data should be considered unusable, although
 * the metadata (header and footer fields) are still correct, meaning
 * that one can use the 'size' field to jump to the previous or the
 * next record.
 */
const CORRUPTION_FLAG   = exports.CORRUPTION_FLAG   = 1;


/* Error codes. Make sure that these do not overlap with the error codes in database.js! */
exports.ERR_INVALID_OFFSET                     = 1;
exports.ERR_INVALID_OFFSET_OR_CORRUPTED_RECORD = 2;
exports.ERR_CORRUPTED                          = 3;


const HEADER_MAGIC      = new Buffer("ZaET");
const SIZE_FIELD_SIZE   = 4;
const FLAGS_FIELD_SIZE  = 1;
const HEADER_SIZE       = HEADER_MAGIC.length + SIZE_FIELD_SIZE + FLAGS_FIELD_SIZE;

const FOOTER_MAGIC      = new Buffer("TEaZ");
const CRC32_BINARY_SIZE = 4;
const RESERVED_DATA     = new Buffer("\0\0\0\0\0\0\0\0");
const FOOTER_SIZE       = FOOTER_MAGIC.length + CRC32_BINARY_SIZE + RESERVED_DATA.length + SIZE_FIELD_SIZE;

const MAX_SIZE = 512 * 1024;
const MAX_SIZE_DESCRIPTION = "512 KB";


function createError(message, code) {
	var e = new Error(message);
	e.code = code;
	e.codeno = exports[code];
	console.assert(e.codeno != undefined);
	return e;
}


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
		callback(createError('Invalid offset', 'ERR_INVALID_OFFSET'));
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
		var i, size, flags;
		
		if (err) {
			self.decReadOperations();
			self._verifyInvariants();
			callback(err);
			return;
		} else if (bytesRead != HEADER_SIZE) {
			// Invalid offset or corrupt file.
			self.decReadOperations();
			self._verifyInvariants();
			callback(createError('Invalid offset or data file corrupted (entry too small)',
				'ERR_INVALID_OFFSET_OR_CORRUPTED_RECORD'));
			return;
		}
		
		// Check header magic.
		for (i = 0; i < HEADER_MAGIC.length; i++) {
			if (buf[i] != HEADER_MAGIC[i]) {
				// Invalid offset or corrupt file.
				self.decReadOperations();
				self._verifyInvariants();
				callback(createError('Invalid offset or data file corrupted (invalid magic)',
					'ERR_INVALID_OFFSET_OR_CORRUPTED_RECORD'));
				return;
			}
		}
		
		// Check size.
		size = IOUtils.bufferToUint(buf, HEADER_MAGIC.length);
		if (size > MAX_SIZE) {
			// Probably corrupt file.
			self.decReadOperations();
			self._verifyInvariants();
			callback(createError('Data file corrupted (invalid size field in header)',
				'ERR_CORRUPTED'));
			return;
		}

		// Check flags.
		flags = buf[HEADER_MAGIC.length + SIZE_FIELD_SIZE];
		
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
				callback(createError('Data file corrupted (last record appears to be truncated)',
					'ERR_CORRUPTED'));
				return;
			}
			
			var checksum = CRC32.calculate(buf, 0, size);
			var storedChecksum = IOUtils.bufferToUint(buf,
				size, CRC32_BINARY_SIZE);
			if (checksum != storedChecksum) {
				// Probably corrupt file.
				callback(createError('Data file corrupted (invalid checksum in header)',
					'ERR_CORRUPTED'));
				return;
			}
			
			var storedSize = IOUtils.bufferToUint(buf,
				size + CRC32_BINARY_SIZE + RESERVED_DATA.length,
				SIZE_FIELD_SIZE);
			if (storedSize != size) {
				// Probably corrupt file.
				callback(createError('Data file corrupted (invalid size field in footer)',
					'ERR_CORRUPTED'));
				return;
			}
			
			// Check footer magic.
			for (i = 0; i < FOOTER_MAGIC.length; i++) {
				if (buf[size + CRC32_BINARY_SIZE + RESERVED_DATA.length + SIZE_FIELD_SIZE + i] != FOOTER_MAGIC[i]) {
					// Probably corrupt file.
					callback(createError('Data file corrupted (invalid checksum in footer)',
						'ERR_CORRUPTED'));
					return;
				}
			}
			
			var record = {
				data: buf.slice(0, size),
				dataSize: size,
				recordSize: HEADER_SIZE + size + FOOTER_SIZE,
				nextOffset: offset + HEADER_SIZE + size + FOOTER_SIZE
			};
			if (flags & CORRUPTION_FLAG) {
				record.corrupted = true;
			}
			callback(undefined, record);
		});
	});
}

TimeEntry.prototype.add = function(buffers, checksumBuffer, options, callback) {
	console.assert(!this.isClosed());
	console.assert(checksumBuffer.length == CRC32.BINARY_SIZE);

	if (typeof(options) == 'function') {
		callback = options;
		options = undefined;
	}
	
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
	if (options && options.corrupted) {
		header[HEADER_MAGIC.length + SIZE_FIELD_SIZE] = CORRUPTION_FLAG;
	} else {
		header[HEADER_MAGIC.length + SIZE_FIELD_SIZE] = 0;
	}
	
	checksumBuffer.copy(footer);
	RESERVED_DATA.copy(footer, CRC32_BINARY_SIZE);
	IOUtils.uintToBuffer(totalDataSize, footer,
		CRC32_BINARY_SIZE + RESERVED_DATA.length);
	FOOTER_MAGIC.copy(footer,
		CRC32_BINARY_SIZE + RESERVED_DATA.length + SIZE_FIELD_SIZE);
	
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
			callback(undefined, {
				eof: true,
				readNext: function() {},
				stop: function() {}
			});
			return;
		}
		
		self.get(pos, function(err, record) {
			if (err) {
				self.evict();
				self.decReadOperations();
				self._verifyInvariants();
				callback(err);
			} else {
				record.readNext = function() {
					if (!record._stopped) {
						record._stopped = true;
						readNext(record.nextOffset);
					}
				}
				record.stop = function() {
					if (!record._stopped) {
						record._stopped = true;
						self.evict();
						self.decReadOperations();
						self._verifyInvariants();
					}
				}
				callback(undefined, record);
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
