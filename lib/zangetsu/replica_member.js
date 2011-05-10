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

// Input types
const EXPECTING_JSON_OBJECT = 1;

// Replication command types
const PRUNE_ONE_COMMAND = 0,
      PRUNE_ALL_COMMAND = 1,
      FILL_COMMAND      = 2;


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
	var workQueue  = [];
	var currentWorkItem;
	
	
	/* The following variable is only meaningful during phase 2.
	 * It indicates whether the replicator currently started.
	 * Only when it's started will the workQueue be eventually processed.
	 */
	var replicatorStarted = false;
	
	function startReplicator() {
		if (!replicatorStarted && self.state != SYNCHRONIZING && self.connected()) {
			replicatorStarted = true;
			process.nextTick(processNextReplicationCommand);
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
	var groupName, localGroup, groupOnReplica, dst,
		localTimeEntry, timeEntryOnReplica;
	for (groupName in toc) {
		localGroup = database.groups[groupName];
		if (localGroup) {
			groupOnReplica = toc[groupName];
			for (dst in groupOnReplica) {
				dst = parseInt(dst);
				localTimeEntry = localGroup.timeEntries[dst];
				if (localTimeEntry) {
					timeEntryOnReplica = groupOnReplica[dst];
					if (timeEntryOnReplica
					 && localTimeEntry.writtenSize < timeEntryOnReplica.size) {
						this._logDebug("Scheduling prune: %s/%d (%s)",
							groupName, dst,
							"size on master smaller than on slave");
						workQueue.push({
							command: PRUNE_ONE_COMMAND,
							groupName: groupName,
							dayTimestamp: dst
						});
					}
					
				} else {
					this._logDebug("Scheduling prune: %s/%d (%s)",
						groupName, dst,
						"time entry doesn't exist on master");
					workQueue.push({
						command: PRUNE_ONE_COMMAND,
						groupName: groupName,
						dayTimestamp: dst
					});
				}
			}
			
		} else {
			this._logDebug("Scheduling prune: %s (%s)",
				groupName,
				"group doesn't exist on master");
			workQueue.push({
				command: PRUNE_ONE_COMMAND,
				groupName: groupName
			});
		}
	}
	for (groupName in database.groups) {
		localGroup = database.groups[groupName];
		groupOnReplica = toc[groupName];
		
		for (dst in localGroup.timeEntries) {
			dst = parseInt(dst);
			localTimeEntry = localGroup.timeEntries[dst];
			if (groupOnReplica) {
				timeEntryOnReplica = groupOnReplica[dst];
			} else {
				timeEntryOnReplica = undefined;
			}
			
			if (timeEntryOnReplica) {
				if (localTimeEntry.writtenSize > timeEntryOnReplica.size) {
					this._logDebug("Scheduling fill: %s/%d (%s)",
						groupName, dst,
						"size on master larger than on slave");
					workQueue.push({
						command: FILL_COMMAND,
						groupName: groupName,
						dayTimestamp: dst,
						timeEntry: localTimeEntry
					});
					localTimeEntry.incReadOperations();
				}
			} else {
				if (localTimeEntry.writtenSize > 0) {
					this._logDebug("Scheduling fill: %s/%d (%s)",
						groupName, dst,
						"time entry doesn't exist on slave");
					workQueue.push({
						command: FILL_COMMAND,
						groupName: groupName,
						dayTimestamp: dst,
						timeEntry: localTimeEntry
					});
					localTimeEntry.incReadOperations();
				}
			}
		}
	}
	
	
	/* Main entry point for the replicator (processing scheduled work items). */
	function processNextReplicationCommand() {
		if (!self.connected()) {
			return;
		}
		
		var details = workQueue.shift();
		if (details) {
			currentWorkItem = details;
			if (details.command == PRUNE_ONE_COMMAND) {
				pruneOne(details);
			} else if (details.command == PRUNE_ALL_COMMAND) {
				pruneAll(details);
			} else {
				console.assert(details.command == FILL_COMMAND);
				fill(details);
			}
			
		} else if (self.state == SYNCHRONIZING) {
			console.assert(!replicatorStarted);
			self._log("Done synchronizing");
			currentWorkItem = undefined;
			doneSynchronizing();
			
		} else {
			console.assert(self.state == READY);
			currentWorkItem = undefined;
			replicatorStarted = false;
		}
	}
	
	function cleanupWorkItem(details) {
		if (!details.cleaned && details.command == FILL_COMMAND) {
			details.cleaned = true;
			details.timeEntry.decReadOperations();
		}
	}
	
	function cleanupWorkQueue() {
		var queue = workQueue;
		workQueue = [];
		var currentItem = currentWorkItem;
		currentWorkItem = undefined;
		
		for (var i = 0; i < queue.length; i++) {
			cleanupWorkItem(queue[i]);
		}
		if (currentItem) {
			cleanupWorkItem(currentItem);
		}
	}
	
	
	function pruneAll(details) {
		self._logDebug("Pruning all:", details);
		
		var groupOnReplica = toc[details.group];
		if (groupOnReplica) {
			var dayTimestampsToPrune = [];
			var dst, i;
			for (dst in groupOnReplica) {
				dst = parseInt(dst);
				if (dst < details.dayTimestamp) {
					dayTimestampsToPrune.push(dst);
				}
			}
			for (i = 0; i < dayTimestampsToPrune.length; i++) {
				dst = dayTimestampsToPrune[i];
				delete groupOnReplica[dst];
			}
			
			pruneNextTimestamp(details, dayTimestampsToPrune);
		} else {
			cleanupWorkItem(details);
			processNextReplicationCommand();
		}
	}
	
	function pruneNextTimestamp(details, dayTimestampsToPrune) {
		var dst = dayTimestampsToPrune.pop();
		if (dst !== undefined) {
			var message = {
				command: 'removeOne',
				group: details.group,
				dayTimestamp: dst
			};
			self._writeJSON(message, function(err) {
				sentRemoveOneCommand(err, details, dayTimestampsToPrune);
			});
		} else {
			cleanupWorkItem(details);
			processNextReplicationCommand();
		}
	}
	
	function sentRemoveOneCommand(err, details, dayTimestampsToPrune) {
		if (self.connected() && !err) {
			self.input.resume();
			self.expectedInput = EXPECTING_JSON_OBJECT;
			self.onJSON = function(reply) {
				receivedReplyForRemoveOneCommand(reply, details, dayTimestampsToPrune);
			}
		}
	}
	
	function receivedReplyForRemoveOneCommand(reply, details, dayTimestampsToPrune) {
		delete self.expectedInput;
		delete self.onJSON;
		self.input.pause();
		
		if (reply && reply.status == 'ok') {
			pruneNextTimestamp(details, dayTimestampsToPrune);
		} else {
			self._disconnect();
		}
	}
	
	
	function pruneOne(details) {
		self._logDebug("Pruning one:", details);
		
		var groupOnReplica = toc[details.groupName];
		console.assert(groupOnReplica !== undefined);
		var message;
		
		if (details.dayTimestamp !== undefined) {
			message = {
				command: 'removeOne',
				group: details.groupName,
				dayTimestamp: details.dayTimestamp
			};
			console.assert(groupOnReplica[details.dayTimestamp] !== undefined);
			delete groupOnReplica[details.dayTimestamp];
		} else {
			message = {
				command: 'remove',
				group: details.groupName
			};
			delete toc[details.groupName];
		}
		self._writeJSON(message, sentRemoveCommand);
	}
	
	function sentRemoveCommand(err) {
		if (self.connected() && !err) {
			self.input.resume();
			self.expectedInput = EXPECTING_JSON_OBJECT;
			self.onJSON = receivedReplyForRemoveCommand;
		}
	}
	
	function receivedReplyForRemoveCommand(reply) {
		delete self.expectedInput;
		delete self.onJSON;
		self.input.pause();
		
		if (reply && reply.status == 'ok') {
			processNextReplicationCommand();
		} else {
			self._disconnect();
		}
	}
	
	
	function fill(details) {
		if (details.buffers) {
			self._logDebug("Fill (with buffers):", {
				groupName: details.groupName,
				dayTimestamp: details.dayTimestamp
			});
		} else {
			self._logDebug("Fill (with streaming):", {
				groupName: details.groupName,
				dayTimestamp: details.dayTimestamp
			});
		}
		
		var groupOnReplica = toc[details.groupName];
		if (!groupOnReplica) {
			groupOnReplica = toc[details.groupName] = {};
		}
		details.timeEntryOnReplica = groupOnReplica[details.dayTimestamp];
		if (!details.timeEntryOnReplica) {
			details.timeEntryOnReplica = groupOnReplica[details.dayTimestamp] = { size: 0 };
		}
		
		if (details.buffers) {
			console.assert(self.state == READY);
			fillBySendingBuffers(details);
		} else {
			console.assert(self.state == SYNCHRONIZING);
			fillByStreamingDataFile(details);
		}
	}
	
	function fillBySendingBuffers(details) {
		console.assert(self.state == READY);
		
		var i;
		details.totalSize = 0;
		for (i = 0; i < details.buffers.length; i++) {
			details.totalSize += details.buffers[i].length;
		}
		if (details.totalSize == 0) {
			cleanupWorkItem(details);
			processNextReplicationCommand();
			return;
		}
		
		var message = {
			command: 'addRaw',
			group: details.groupName,
			dayTimestamp: details.dayTimestamp,
			size: details.totalSize
		};
		self._writeJSON(message, function(err) {
			if (err || !self.connected()) {
				return;
			}
			for (i = 0; i < details.buffers.length; i++) {
				if (i == details.buffers.length - 1) {
					self._write(details.buffers[i], function(err) {
						sentAllBuffers(err, details);
					});
				} else {
					self._write(details.buffers[i]);
				}
			}
		});
	}
	
	function sentAllBuffers(err, details) {
		if (err || !self.connected()) {
			return;
		}
		console.assert(self.state == READY);
		self.expectedInput = EXPECTING_JSON_OBJECT;
		self.onJSON = function(reply) {
			delete self.expectedInput;
			delete self.onJSON;
			self.input.pause();
			
			if (reply && reply.status == 'ok') {
				details.timeEntryOnReplica.size += details.totalSize;
				cleanupWorkItem(details);
				processNextReplicationCommand();
			} else {
				self._disconnect();
			}
		}
		self.input.resume();
	}
	
	function fillByStreamingDataFile(details) {
		console.assert(self.state == SYNCHRONIZING);
		
		details.timeEntry.streamRead(details.timeEntryOnReplica.size,
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
					sentPieceOfDataFile(err, details,
						buf, stop, continueReading);
				});
			} else {
				cleanupWorkItem(details);
				processNextReplicationCommand();
			}
		});
	}
	
	function sentPieceOfDataFile(err, details, buf, stop, continueReading) {
		if (err || !self.connected()) {
			stop();
			return;
		}
		console.assert(self.state == SYNCHRONIZING);
		
		self.expectedInput = EXPECTING_JSON_OBJECT;
		self.onJSON = function(reply) {
			delete self.expectedInput;
			delete self.onJSON;
			self.input.pause();
			
			if (reply && reply.status == 'ok') {
				details.timeEntryOnReplica.size += buf.length;
				continueReading();
			} else {
				stop();
				self._disconnect();
			}
		}
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
		
		if (dayTimestamp) {
			if (self.state == SYNCHRONIZING) {
				self._logDebug("Concurrent remove, scheduling prune: %s/%d",
					groupName, dayTimestamp);
			} else {
				self._logDebug("Scheduling prune: %s/%d",
					groupName, dayTimestamp);
			}
			workQueue.push({
				command: PRUNE_ALL_COMMAND,
				group: groupName,
				dayTimestamp: dayTimestamp
			});
		} else {
			if (self.state == SYNCHRONIZING) {
				self._logDebug("Concurrent remove, scheduling prune:",
					groupName);
			} else {
				self._logDebug("Scheduling prune:", groupName);
			}
			workQueue.push({
				command: PRUNE_ONE_COMMAND,
				group: groupName
			});
		}
		startReplicator();
	}
	
	/* If any time entries in our database have been written to
	 * while pruning or filling is in progress then schedule them
	 * for filling too. fillNext() will eventually handle it.
	 */
	function onAddToOurDatabase(groupName, dayTimestamp, offset, size, rawBuffers) {
		if (!self.connected()) {
			return;
		}
		
		var localTimeEntry = database.findTimeEntry(groupName, dayTimestamp);
		localTimeEntry.incReadOperations();
		
		if (self.state == SYNCHRONIZING) {
			self._logDebug("Concurrent add, scheduling fill: %s/%d",
				groupName, dayTimestamp);
			workQueue.push({
				command: FILL_COMMAND,
				groupName: groupName,
				dayTimestamp: dayTimestamp,
				timeEntry: localTimeEntry
			});
		} else {
			self._logDebug("Scheduling fill: %s/%d", groupName, dayTimestamp);
			workQueue.push({
				command: FILL_COMMAND,
				groupName: groupName,
				dayTimestamp: dayTimestamp,
				timeEntry: localTimeEntry,
				buffers: rawBuffers
			});
		}
		startReplicator();
	}
	
	function onConnectionClose() {
		database.removeListener('remove', onRemoveFromOurDatabase);
		database.removeListener('add', onAddToOurDatabase);
		if (self.onJSON) {
			// Stop streamRead() call and decrease TimeEntry
			// read operations counter
			self.onJSON(undefined);
		}
		cleanupWorkQueue();
	}
	
	
	/********** Phase 2 **********/
	/* Eventually synchronization will be done. Here we switch to
	 * phase 2 of the replication code.
	 */
	function doneSynchronizing() {
		console.assert(workQueue.length == 0);
		console.assert(self.onJSON === undefined);
		console.assert(currentWorkItem === undefined);
		
		/* There may be add/remove commands in progress, but that's okay.
		 * The database event handlers will eventually take care of them.
		 */
		
		self.state = READY;
		self._log("Synchronization done, switching to replication phase");
	}
	
	database.on('remove', onRemoveFromOurDatabase);
	database.on('add', onAddToOurDatabase);
	this.socket.on('close', onConnectionClose);
	processNextReplicationCommand();
}


exports.ReplicaMember = ReplicaMember;
exports.READY = READY;
