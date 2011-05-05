var Utils   = require('./utils.js');
var IOUtils = require('./io_utils.js');

var getType = Utils.getType;
var parseJsonObjectFromStream = IOUtils.parseJsonObjectFromStream;


// Roles
const MASTER  = 0,
      SLAVE   = 1,
      UNKNOWN = 3;

// States
const UNINITIALIZED = 0,
      SYNCHRONIZING = 1,
      READY         = 2,
      DISCONNECTED  = 3;


const EXPECTING_JSON_OBJECT = 1;


function ReplicaMember(server, socket, input, output, id) {
	this.server = server;
	this.socket = socket;
	this.input  = input;
	this.output = output;
	this.id     = id;
	this.role   = UNKNOWN;
	this.state  = UNINITIALIZED;
	this.buffer = '';
	
	this.database = server.database;
	
	return this;
}

ReplicaMember.prototype._disconnectWithError = function(message) {
	if (this.connected()) {
		IOUtils.disconnectWithError(this.socket, message);
		this.state = DISCONNECTED;
	}
}

ReplicaMember.prototype._disconnect = function() {
	if (this.connected()) {
		this.socket.destroy();
		this.state = DISCONNECTED;
	}
}

ReplicaMember.prototype._log = function(message) {
	var args = ["[ReplicaMember %d] " + message, this.id];
	for (var i = 1; i < arguments.length; i++) {
		args.push(arguments[i]);
	}
	console.log.apply(console, args);
}

ReplicaMember.prototype._logDebug = function(message) {
	var args = ["[ReplicaMember %d DEBUG] " + message, this.id];
	for (var i = 1; i < arguments.length; i++) {
		args.push(arguments[i]);
	}
	console.log.apply(console, args);
}

ReplicaMember.prototype._logError = function(message) {
	var args = ["[ReplicaMember %d ERROR] " + message, this.id];
	for (var i = 1; i < arguments.length; i++) {
		args.push(arguments[i]);
	}
	console.error.apply(console, args);
}

ReplicaMember.prototype._write = function(buf, callback) {
	return this.output.write(buf, callback);
}

ReplicaMember.prototype._writeJSON = function(buf, callback) {
	return this.output.writeJSON(buf, callback);
}

ReplicaMember.prototype.connected = function() {
	return this.state != DISCONNECTED && typeof(this.socket.fd) == 'number';
}

ReplicaMember.prototype.ready = function() {
	return this.state == READY;
}

/**
 * Called when a replica member has connected to this server.
 */
ReplicaMember.prototype.initialize = function() {
	var reply = { status: 'ok' };
	var self  = this;
	if (this.server.role == MASTER) {
		this.role = SLAVE;
		this._writeJSON({
			status   : 'ok',
			your_role: 'slave',
			my_role  : 'master'
		});
		if (this.connected()) {
			this._startReplication();
		}
	} else if (this.server.role == SLAVE) {
		this.role = SLAVE;
		this._writeJSON({
			status   : 'ok',
			your_role: 'slave',
			my_role  : 'slave'
		});
	} else {
		this._writeJSON({
			status   : 'ok',
			your_role: 'unknown',
			my_role  : 'unknown',
			negotiate_role: true
		});
	}
}


ReplicaMember.prototype.onData = function(data) {
	var consumed = 0, found = false;
	var i, result;
	
	while (consumed < data.length && this.connected()) {
		switch (this.expectedInput) {
		case EXPECTING_JSON_OBJECT:
			try {
				result = parseJsonObjectFromStream(this.buffer, data, consumed);
			} catch (e) {
				this._disconnectWithError(
					"Cannot parse JSON object " +
					JSON.stringify(this.buffer) +
					": " + e.message);
				return consumed;
			}
			if (getType(result.object) != 'object') {
				this._disconnectWithError("Expected a JSON object.");
				return consumed;
			}
			consumed = result.pos;
			if (result.done) {
				this.buffer = '';
				this.onJSON(result.object);
				return consumed;
			} else {
				this.buffer += result.slice;
			}
			break;
			
		default:
			throw Error('Unexpected expectedInput state ' + this.expectedInput);
		}
	}
	
	return consumed;
}


ReplicaMember.prototype._startReplication = function() {
	console.assert(this.state == UNINITIALIZED);
	
	/* Ask the member what his table of contents is.
	 * Upon receiving a reply, begin removing nonexistant groups.
	 */
	var self = this;
	this._writeJSON({ command: 'getToc' });
	this.state = SYNCHRONIZING;
	this.expectedInput = EXPECTING_JSON_OBJECT;
	this.onJSON = function(toc) {
		delete self.expectedInput;
		delete self.onJSON;
		self.input.pause();
		self._continueReplication(toc);
	}
}

ReplicaMember.prototype._continueReplication = function(toc) {
	console.assert(this.state == SYNCHRONIZING);
	this.toc = toc;
	this.input.pause();
	
	/* Replication consists of two phases. The first phase is the
	 * synchronization phase (state == SYNCHRONIZING) during which
	 * the master synchronizes the slave's contents with that of
	 * the master's.
	 *
	 * Once that is done, replication will enter the second phase,
	 * the replication phase (state == SLAVE). The master will
	 * forward all database mutations to the slave.
	 */
	
	var self       = this;
	var database   = this.database;
	var pruneQueue = [];
	var fillQueue  = [];
	var groupName, localGroup, groupOnReplica, dst,
		localTimeEntry, timeEntryOnReplica;
	
	
	/* The following variable is only meaningful during phase 2.
	 * It indicates whether the pruner or the filler is currently
	 * started. Only when it's started will pruneQueue and fillQueue
	 * be eventually processed.
	 */
	var prunerOrFillerActive = false;
	
	function startPruningOrFilling() {
		if (!prunerOrFillerActive && self.state != SYNCHRONIZING && self.connected()) {
			prunerOrFillerActive = true;
			process.nextTick(pruneNext);
		}
	}
	
	
	/********** Phase 1 **********/
	
	/* Find groups or time entries that exist on this replica member but not in
	 * our database, and schedule them for pruning on the replica member.
	 *
	 * Time entries that are larger on the replica member than in our database
	 * are also scheduled for pruning.
	 *
	 * Check whether the remaining time entries' sizes on the replica member
	 * match those in our database, and if not, schedule them for filling.
	 *
	 * We explicitly check against TimeEntry.writtenSize instead of
	 * TimeEntry.dataFileSize because if there are any writes in progress
	 * then we already notified of their completion by the database events,
	 * which will eventually result in synchronization updates.
	 */
	for (groupName in toc) {
		localGroup = database.groups[groupName];
		if (localGroup) {
			groupOnReplica = toc[groupName];
			
			if (!groupOnReplica) {
				groupOnReplica = toc[groupName] = {};
			}
			
			for (dst in groupOnReplica) {
				dst = parseInt(dst);
				localTimeEntry = localGroup.timeEntries[dst];
				if (localTimeEntry) {
					timeEntryOnReplica = groupOnReplica[dst];
					if (!timeEntryOnReplica) {
						groupOnReplica[dst] = { size: 0 };
					}
					if (localTimeEntry.writtenSize < timeEntryOnReplica.size) {
						this._logDebug("Scheduling prune:", groupName, dst,
							"(size on master smaller than on slave)");
						delete groupOnReplica[dst];
						pruneQueue.push({
							groupName: groupName,
							dayTimestamp: dst
						});
					}
					
				} else {
					this._logDebug("Scheduling prune:", groupName, dst,
						"(time entry doesn't exist on master)");
					delete groupOnReplica[dst];
					pruneQueue.push({
						groupName: groupName,
						dayTimestamp: dst
					});
				}
			}
			
		} else {
			this._logDebug("Scheduling prune:", groupName, "(group doesn't exist on master)");
			delete toc[groupName];
			pruneQueue.push({ groupName: groupName });
		}
	}
	for (groupName in database.groups) {
		localGroup = database.groups[groupName];
		groupOnReplica = toc[groupName];
		
		if (!groupOnReplica) {
			groupOnReplica = toc[groupName] = {};
		}
		for (dst in localGroup.timeEntries) {
			dst = parseInt(dst);
			localTimeEntry = localGroup.timeEntries[dst];
			timeEntryOnReplica = groupOnReplica[dst];
			
			if (timeEntryOnReplica) {
				if (localTimeEntry.writtenSize > timeEntryOnReplica.size) {
					this._logDebug("Scheduling fill:", groupName, dst,
						"(size on master larger than on slave)");
					fillQueue.push({
						groupName: groupName,
						dayTimestamp: dst
					});
				}
			} else {
				groupOnReplica[dst] = { size: 0 };
				if (localTimeEntry.writtenSize > 0) {
					this._logDebug("Scheduling fill:", groupName, dst,
						"(time entry doesn't exist on slave)");
					fillQueue.push({
						groupName: groupName,
						dayTimestamp: dst
					});
				}
			}
		}
	}
	
	
	/* Then tell the replica member to prune the scheduled groups
	 * or time entries.
	 */
	
	function pruneNext() {
		var details = pruneQueue.pop();
		if (details) {
			var message;
			self._logDebug("Prune next:", details);
			if (details.dayTimestamp) {
				console.assert(toc[details.groupName][details.dayTimestamp] === undefined);
				message = {
					command: 'removeOne',
					group: details.groupName,
					dayTimestamp: details.dayTimestamp
				};
			} else {
				console.assert(toc[details.groupName] === undefined);
				message = {
					command: 'remove',
					group: details.groupName
				};
			}
			self._writeJSON(message, sentRemoveCommand);
			
		} else {
			self._logDebug("Done pruning, continue with filling");
			fillNext();
		}
	}
	
	function sentRemoveCommand(err) {
		if (self.connected() && !err) {
			self.input.resume();
			self.expectedInput = EXPECTING_JSON_OBJECT;
			self.onJSON = receivedReplyForRemoveMessage;
		}
	}
	
	function receivedReplyForRemoveMessage(reply) {
		delete self.expectedInput;
		delete self.onJSON;
		self.input.pause();
		
		if (reply.status == 'ok') {
			pruneNext();
		} else {
			self._disconnect();
		}
	}
	
	
	/* When all nonexistant groups and time entries have been pruned
	 * from the replica member, fill up the remaining time entries on
	 * the replica member with data that only exists in our database.
	 */
	
	function fillNext() {
		var details = fillQueue.pop();
		if (details) {
			self._logDebug("Fill next:", details);
			
			var groupOnReplica = toc[details.groupName];
			console.assert(groupOnReplica !== undefined);
			var timeEntryOnReplica = groupOnReplica[details.dayTimestamp];
			console.assert(timeEntryOnReplica !== undefined);
			var localTimeEntry = database.findTimeEntry(details.groupName,
				details.dayTimestamp);
			console.assert(localTimeEntry !== undefined);
			
			if (details.buffers) {
				console.assert(self.state == READY);
				fillBySendingBuffers(details.groupName,
					details.dayTimestamp,
					details.buffers);
			} else {
				console.assert(self.state == SYNCHRONIZING);
				fillByStreamingDataFile(details, localTimeEntry, timeEntryOnReplica);
			}
			
		} else if (pruneQueue.length > 0) {
			/* Some things have been removed in our database in the
			 * mean time go back to the pruning state.
			 */
			self._logDebug("Done filling, going back to pruning");
			pruneNext();
			
		} else if (self.state == SYNCHRONIZING) {
			self._logDebug("Done filling, finalizing");
			prunerOrFillerActive = false;
			done();
			
		} else {
			console.assert(self.state == READY);
			self._logDebug("Done filling");
			prunerOrFillerActive = false;
		}
	}
	
	function fillBySendingBuffers(groupName, dayTimestamp, buffers) {
		console.assert(self.state == READY);
		
		var i;
		var totalSize = 0;
		for (i = 0; i < buffers.length; i++) {
			totalSize += buffers[i].length;
		}
		if (totalSize == 0) {
			fillNext();
			return;
		}
		
		var message = {
			command: 'addRaw',
			group: groupName,
			dayTimestamp: dayTimestamp,
			size: totalSize
		};
		self._writeJSON(message);
		if (self.connected()) {
			for (i = 0; i < buffers.length; i++) {
				if (i == buffers.length - 1) {
					self._write(buffers[i], function(err) {
						sentAllBuffers(err, totalSize);
					});
				} else {
					self._write(buffers[i]);
				}
			}
		}
	}
	
	function sentAllBuffers(err, totalSize) {
		if (err || !self.connected()) {
			return;
		}
		console.assert(self.state == READY);
		
		self.expectedInput = EXPECTING_JSON_OBJECT;
		self.onJSON = function(reply) {
			delete self.expectedInput;
			delete self.onJSON;
			self.input.pause();
			
			if (reply.status == 'ok') {
				timeEntryOnReplica.size += totalSize;
				fillNext();
			} else {
				self._disconnect();
			}
		}
		self.input.resume();
	}
	
	function fillByStreamingDataFile(details, localTimeEntry, timeEntryOnReplica) {
		console.assert(self.state == SYNCHRONIZING);
		
		localTimeEntry.streamRead(timeEntryOnReplica.size,
			function(err, buf, continueReading, stop)
		{
			if (!self.connected()) {
				stop();
			} else if (err) {
				self._logError("Cannot read data file on master: " + err);
				self._disconnectWithError("Cannot read data file on master: " + err);
			} else if (buf.length > 0) {
				var message = {
					command: 'addRaw',
					group: details.groupName,
					dayTimestamp: details.dayTimestamp,
					size: buf.length
				};
				self._writeJSON(message);
				self._write(buf, function(err) {
					sentPieceOfDataFile(err,
						timeEntryOnReplica,
						buf, stop, continueReading);
				});
			} else {
				fillNext();
			}
		});
	}
	
	function sentPieceOfDataFile(err, timeEntryOnReplica, buf,
		stop, continueReading)
	{
		if (err || !self.connected()) {
			stop();
			return;
		}
		console.assert(self.state == SYNCHRONIZING);
		
		self.expectedInput = EXPECTING_JSON_OBJECT;
		self.receivedReplyForAddRawMessage = function(reply) {
			delete self.expectedInput;
			delete self.onJSON;
			delete self.receivedReplyForAddRawMessage;
			self.input.pause();
			
			if (reply && reply.status == 'ok') {
				timeEntryOnReplica.size += buf.length;
				continueReading();
			} else {
				stop();
				self._disconnect();
			}
		}
		self.onJSON = self.receivedReplyForAddRawMessage;
		self.input.resume();
	}
	
	
	/* If any groups or time entries are removed from our database while
	 * pruning or filling is in progress then schedule them for pruning too.
	 * pruneNext() will eventually handle it.
	 */
	function onRemoveFromOurDatabase(groupName, dayTimestamp) {
		if (!self.connected()) {
			return;
		}
		
		var groupOnReplica = toc[groupName];
		if (self.state == SYNCHRONIZING) {
			if (!groupFromReplica) {
				return;
			}
		} else {
			console.assert(groupOnReplica !== undefined);
		}
		
		if (dayTimestamp) {
			var dayTimestampsToDelete = [];
			var dst, i;
			for (dst in groupOnReplica) {
				dst = parseInt(dst);
				if (dst < dayTimestamp) {
					dayTimestampsToDelete.push(dst);
				}
			}
			for (i = 0; i < dayTimestampsToDelete.length; i++) {
				dst = dayTimestampsToDelete[i];
				if (self.state == SYNCHRONIZING) {
					self._logDebug("Concurrent remove, scheduling prune: %s/%d",
						groupName, dst);
				} else {
					self._logDebug("Scheduling prune: %s/%d",
						groupName, dst);
				}
				delete groupOnReplica[dst];
				pruneQueue.push({
					group: groupName,
					dayTimestamp: dst
				});
			}
		} else {
			if (self.state == SYNCHRONIZING) {
				self._logDebug("Concurrent remove, scheduling prune:",
					groupName);
			} else {
				self._logDebug("Scheduling prune:", groupName);
			}
			delete toc[groupName];
			pruneQueue.push({ group: groupName });
		}
		startPruningOrFilling();
	}
	
	/* If any time entries in our database have been written to
	 * while pruning or filling is in progress then schedule them
	 * for filling too. fillNext() will eventually handle it.
	 */
	function onAddToOurDatabase(groupName, dayTimestamp, offset, size, rawBuffers) {
		if (!self.connected()) {
			return;
		}
		
		var groupOnReplica = toc[groupName];
		if (!groupOnReplica) {
			groupOnReplica = toc[groupName] = {};
		}
		var timeEntryOnReplica = groupOnReplica[dayTimestamp];
		if (!timeEntryOnReplica) {
			groupOnReplica[dayTimestamp] = { size: 0 };
		}
		if (self.state == SYNCHRONIZING) {
			self._logDebug("Concurrent add, scheduling fill: %s/%d",
				groupName, dayTimestamp);
			fillQueue.push({ groupName: groupName, dayTimestamp: dayTimestamp });
		} else {
			self._logDebug("Scheduling fill: %s/%d", groupName, dayTimestamp);
			fillQueue.push({
				groupName: groupName,
				dayTimestamp: dayTimestamp,
				buffers: rawBuffers
			});
		}
		startPruningOrFilling();
	}
	
	function onConnectionClose() {
		database.removeListener('remove', onRemoveFromOurDatabase);
		database.removeListener('add', onAddToOurDatabase);
		if (self.receivedReplyForAddRawMessage) {
			// Stop streamRead() call
			self.receivedReplyForAddRawMessage(undefined);
		}
	}
	
	
	/********** Phase 2 **********/
	/* Eventually synchronization will be done. Here we switch to
	 * phase 2 of the replication code.
	 */
	function done() {
		console.assert(pruneQueue.length == 0);
		console.assert(fillQueue.length == 0);
		console.assert(self.receivedReplyForAddRawMessage === undefined);
		
		/* There may be add/remove commands in progress, but that's okay.
		 * The database event handlers will eventually take care of them.
		 */
		
		self.state = READY;
		self._log("Synchronization done, switching to replication phase");
	}
	
	database.on('remove', onRemoveFromOurDatabase);
	database.on('add', onAddToOurDatabase);
	this.socket.on('close', onConnectionClose);
	pruneNext();
}


exports.ReplicaMember = ReplicaMember;
exports.READY = READY;
