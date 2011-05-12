/*
 * Each data file in the database consists of an array of entry records
 * with the following format:
 *
 *   Magic        2 bytes             Always equal to the value "ET"
 *   Data size    4 bytes             In big endian format.
 *   Data         'Data size' bytes   The actual data.
 *   CRC32        4 bytes             Checksum of the data.
 *   Data size    4 bytes             In big endian format.
 *   Magic        2 bytes             Always equal to the value "TE"
 *
 * The fact that the checksum comes after the data is to allow adding
 * data in a streaming manner without buffering it in memory first and
 * without seeking backwards into the file.
 * The fact that the data size is duplicated in the footer is to allow
 * reading entries in reverse order.
 */

var fs      = require('fs');
var Utils   = require('./utils.js');
var IOUtils = require('./io_utils.js');
var CRC32   = require('./crc32.js');
var NativeSupport = require('./native_support.node');

var min = Utils.min;

const HEADER_MAGIC      = new Buffer("ET");
const SIZE_ENTRY_SIZE   = 4;
const HEADER_SIZE       = HEADER_MAGIC.length + SIZE_ENTRY_SIZE;

const FOOTER_MAGIC      = new Buffer("TE");
const CRC32_BINARY_SIZE = 4;
const FOOTER_SIZE       = FOOTER_MAGIC.length + CRC32_BINARY_SIZE + SIZE_ENTRY_SIZE;

const MAX_SIZE = 1024 * 1024;
const MAX_SIZE_DESCRIPTION = "1 MB";


function TimeEntry(database, objectId, dayTimestamp, path, stream, dataFileSize) {
	/****** Public read-only ******/
	
	this.database = database;
	this.objectId = objectId;
	this.dayTimestamp = dayTimestamp;
	this.path = path;
	this.stream = stream;
	this.dataFileSize = dataFileSize;
	
	/* Number of bytes in the file for which we know the data is done written.
	 * Invariant:
	 *   writtenSize <= dataFileSize
	 */
	this.writtenSize = dataFileSize;
	
	/****** Private ******/
	
	/* Number of bytes in the file that have been synced to the disk and
	 * possibly also evicted from the OS page cache.
	 * Invariant:
	 *   flushedSize <= writtenSize
	 */
	this.flushedSize = 0;
	
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
	
	/** To be called when done flushing. */
	this.afterFlushCallbacks = [];
}

TimeEntry.prototype._verifyInvariants = function() {
	// !a || b: logical equivalent of a IMPLIES b.
	console.assert(this.writtenSize <= this.dataFileSize);
	console.assert(this.flushedSize <= this.writtenSize);
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

TimeEntry.prototype.equals = function(other) {
	return this.objectId == other.objectId;
}

TimeEntry.prototype.hashCode = function() {
	return this.objectId;
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
	console.assert(!this.closing && !this.closed);
	
	if (offset + HEADER_SIZE + FOOTER_SIZE > this.writtenSize) {
		// Invalid offset.
		callback('not-found');
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
			callback('not-found');
			return;
		}
		
		// Check header magic.
		for (i = 0; i < HEADER_MAGIC.length; i++) {
			if (buf[i] != HEADER_MAGIC[i]) {
				// Invalid offset or corrupt file.
				self.decReadOperations();
				self._verifyInvariants();
				callback('not-found');
				return;
			}
		}
		
		// Check size.
		size = IOUtils.bufferToUint(buf, HEADER_MAGIC.length);
		if (size > MAX_SIZE) {
			// Probably corrupt file..
			self.decReadOperations();
			self._verifyInvariants();
			callback('not-found');
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
				// What's going on?
				callback('not-found');
				return;
			}
			
			var checksum = CRC32.calculate(buf, 0, size);
			var storedChecksum = IOUtils.bufferToUint(buf,
				size, CRC32_BINARY_SIZE);
			if (checksum != storedChecksum) {
				// Probably corrupt file.
				callback('not-found');
				return;
			}
			
			var storedSize = IOUtils.bufferToUint(buf,
				size + CRC32_BINARY_SIZE, SIZE_ENTRY_SIZE);
			if (storedSize != size) {
				// Probably corrupt file.
				callback('not-found');
				return;
			}
			
			// Check footer magic.
			for (i = 0; i < FOOTER_MAGIC.length; i++) {
				if (buf[size + CRC32_BINARY_SIZE + SIZE_ENTRY_SIZE + i] != FOOTER_MAGIC[i]) {
					// Probably corrupt file.
					callback('not-found');
					return;
				}
			}
			
			callback(undefined, buf.slice(0, size));
		});
	});
}

TimeEntry.prototype.add = function(buffers, checksumBuffer, callback) {
	console.assert(!this.closing && !this.closed);
	console.assert(checksumBuffer.length == CRC32.BINARY_SIZE);
	
	var totalSize = 0;
	var i;
	for (i = 0; i < buffers.length; i++) {
		totalSize += buffers[i].length;
	}
	console.assert(totalSize < MAX_SIZE);
	
	var self = this;
	var prevOffset = this.dataFileSize;
	var headerAndFooter = new Buffer(HEADER_SIZE + FOOTER_SIZE);
	var header = headerAndFooter.slice(0, HEADER_SIZE);
	var footer = headerAndFooter.slice(HEADER_SIZE);
	
	function allocateSpace() {
		self.dataFileSize += HEADER_SIZE + totalSize + FOOTER_SIZE;
		if (NativeSupport.posix_fallocate) {
			NativeSupport.posix_fallocate(self.stream.fd, self.dataFileSize,
				HEADER_SIZE + totalSize + FOOTER_SIZE, spaceAllocated);
		} else {
			spaceAllocated();
		}
	}
	
	function spaceAllocated(err) {
		console.assert(!self.closed);
		if (err) {
			self.dataFileSize -= HEADER_SIZE + totalSize + FOOTER_SIZE;
			self._decWriteOperations();
			self._verifyInvariants();
			callback(err);
			return;
		}
		
		var i;
		self.stream.write(header);
		for (i = 0; i < buffers.length; i++) {
			self.stream.write(buffers[i]);
		}
		self.stream.write(footer, written);
	}
	
	function written(err) {
		console.assert(!self.closed);
		if (err) {
			self.dataFileSize -= HEADER_SIZE + totalSize + FOOTER_SIZE;
		} else {
			self.writtenSize += HEADER_SIZE + totalSize + FOOTER_SIZE;
		}
		self._decWriteOperations();
		self._verifyInvariants();
		if (err) {
			callback(err);
		} else {
			var rawBuffers = [header];
			for (i = 0; i < buffers.length; i++) {
				rawBuffers.push(buffers[i]);
			}
			rawBuffers.push(footer);
			if (self.database) {
				self.database.registerDataGrowth(self,
					HEADER_SIZE + totalSize + FOOTER_SIZE);
			}
			callback(undefined,
				prevOffset,
				HEADER_SIZE + totalSize + FOOTER_SIZE,
				rawBuffers);
		}
	}
	
	HEADER_MAGIC.copy(header);
	IOUtils.uintToBuffer(totalSize, header, HEADER_MAGIC.length);
	
	checksumBuffer.copy(footer);
	IOUtils.uintToBuffer(totalSize, footer, CRC32_BINARY_SIZE);
	FOOTER_MAGIC.copy(footer, CRC32_BINARY_SIZE + SIZE_ENTRY_SIZE);
	
	this._incWriteOperations();
	if (this.flushing) {
		this.afterFlushCallbacks.push(allocateSpace);
	} else {
		allocateSpace();
	}
}

TimeEntry.prototype.streamRead = function(offset, callback) {
	console.assert(!this.closing && !this.closed);
	var buf  = new Buffer(1024 * 32);
	var self = this;
	
	function readNext(pos) {
		if (pos >= self.writtenSize) {
			// EOF reached.
			self.decReadOperations();
			self._verifyInvariants();
			callback(undefined, buf.slice(0, 0));
		} else {
			var bytesToRead = min(buf.length, self.writtenSize - pos);
			var stopped = false;
			
			function stop() {
				if (!stopped) {
					self.decReadOperations();
					self._verifyInvariants();
					stopped = true;
				}
			}
			
			fs.read(self.stream.fd, buf, 0, bytesToRead, pos, function(err, bytesRead) {
				if (err) {
					stop();
					callback(err);
				} else if (bytesRead == 0) {
					stop();
					callback(undefined, buf.slice(0, 0));
				} else {
					callback(undefined, buf.slice(0, bytesRead),
						// Continue reading
						function() {
							if (!stopped) {
								stopped = true;
								readNext(pos + bytesRead);
							}
						},
						stop);
				}
			});
		}
	}
	
	this.incReadOperations();
	readNext(offset);
}

TimeEntry.prototype.addRaw = function(buf, callback) {
	console.assert(!this.closing && !this.closed);
	var self = this;
	this._incWriteOperations();
	
	function perform() {
		self.dataFileSize += buf.length;
		self.stream.write(buf, function(err) {
			self._decWriteOperations();
			self._verifyInvariants();
			if (err) {
				self.dataFileSize -= buf.length;
				callback(err);
			} else {
				self.writtenSize += buf.length;
				if (self.database) {
					self.database.registerDataGrowth(self,
						buf.length);
				}
				callback();
			}
		});
	}
	
	if (this.flushing) {
		this.afterFlushCallbacks.push(perform);
	} else {
		perform();
	}
}

TimeEntry.prototype.flush = function() {
	// database.js assumes that this function doesn't call callbacks immediately.
	console.assert(!this.closing && !this.closed);
	if (!this.flushing && this.writtenSize != this.flushedSize) {
		var self = this;
		// Some writes may be in progress right now so remember
		// the original writtenSize.
		var origWrittenSize = this.writtenSize;
		this.flushing = true;
		
		function synched(err) {
			if (err) {
				self.decReadOperations();
				self._verifyInvariants();
			} else if (NativeSupport.posix_fadvise) {
				NativeSupport.posix_fadvise(self.stream.fd,
					self.flushedSize,
					origWrittenSize - self.flushedSize,
					NativeSupport.POSIX_FADV_DONTNEED,
					evicted);
			} else {
				evicted();
			}
		}
		
		function evicted(err) {
			self.flushedSize = origWrittenSize;
			self.flushing = false;
			self.decReadOperations();
			self._verifyInvariants();
			
			var callbacks = self.afterFlushCallbacks;
			self.afterFlushCallbacks = [];
			for (var i = 0; i < callbacks.length; i++) {
				callbacks[i](err);
			}
		}
		
		this.incReadOperations();
		if (NativeSupport.sync_file_range) {
			NativeSupport.sync_file_range(this.stream.fd,
				this.flushedSize,
				origWrittenSize - this.flushedSize,
				NativeSupport.SYNC_FILE_RANGE_WAIT_BEFORE |
					NativeSupport.SYNC_FILE_RANGE_WRITE |
					NativeSupport.SYNC_FILE_RANGE_WAIT_AFTER,
				synched);
		} else {
			NativeSupport.fdatasync(this.stream.fd, synched);
		}
	}
}

exports.TimeEntry   = TimeEntry;
exports.MAX_SIZE    = MAX_SIZE;
exports.MAX_SIZE_DESCRIPTION = MAX_SIZE_DESCRIPTION;
exports.HEADER_SIZE = HEADER_SIZE;
exports.FOOTER_SIZE = FOOTER_SIZE;
