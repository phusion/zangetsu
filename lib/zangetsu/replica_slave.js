/**
 * Represents a connection to a slave server.
 * Implements the logic pertaining replication to a slave server.
 */

var Server    = require('./server.js');
var Constants = require('./constants.js');
var Utils     = require('./utils.js');
var IOUtils   = require('./io_utils.js');

var getType = Utils.getType;
var parseJsonObjectFromStream = IOUtils.parseJsonObjectFromStream;


// States
const UNINITIALIZED = 0,
      BACKGROUND_SYNCHRONIZING = 1,
      LOCKED_SYNCHRONIZING     = 2,
      READY         = 3,
      DISCONNECTED  = 4;

// Input types
const EXPECTING_JSON_OBJECT = 1;

// Replication command types
const PRUNE_ONE_COMMAND = 0,
      PRUNE_ALL_COMMAND = 1,
      FILL_COMMAND      = 2;


function ReplicaSlave(server, socket, input, output, id) {
	this.server = server;
	this.socket = socket;
	this.input  = input;
	this.output = output;
	this.id     = id;
	this.role   = Constants.ROLE_UNKNOWN;
	this.state  = UNINITIALIZED;
	this.buffer = '';
	
	this.database = server.database;
	
	return this;
}

ReplicaSlave.prototype._disconnectWithError = function(message) {
	if (this.connected()) {
		this._logError(message);
		IOUtils.disconnectWithError(this.socket, message);
		this.state = DISCONNECTED;
	}
}

ReplicaSlave.prototype._disconnect = function() {
	if (this.connected()) {
		if (this.socket.bufferSize > 0) {
			var self = this;
			this.socket.once('drain', function() {
				self.socket.destroy();
			});
		} else {
			this.socket.destroy();
		}
		this.state = DISCONNECTED;
	}
}

ReplicaSlave.prototype._log = function(message) {
	var args = ["[ReplicaSlave %d] " + message, this.id];
	for (var i = 1; i < arguments.length; i++) {
		args.push(arguments[i]);
	}
	console.log.apply(console, args);
}

ReplicaSlave.prototype._logDebug = function(message) {
	var args = ["[ReplicaSlave %d DEBUG] " + message, this.id];
	for (var i = 1; i < arguments.length; i++) {
		args.push(arguments[i]);
	}
	console.log.apply(console, args);
}

ReplicaSlave.prototype._logError = function(message) {
	var args = ["[ReplicaSlave %d ERROR] " + message, this.id];
	for (var i = 1; i < arguments.length; i++) {
		args.push(arguments[i]);
	}
	console.error.apply(console, args);
}

ReplicaSlave.prototype._write = function(buf, callback) {
	return this.output.write(buf, callback);
}

ReplicaSlave.prototype._writeJSON = function(buf, callback) {
	return this.output.writeJSON(buf, callback);
}

ReplicaSlave.prototype.connected = function() {
	return this.state != DISCONNECTED && typeof(this.socket.fd) == 'number';
}

ReplicaSlave.prototype.ready = function() {
	return this.state == READY;
}

ReplicaSlave.prototype.close = function() {
	this._disconnect();
}

/**
 * Called when a replica slave has connected to this server.
 */
ReplicaSlave.prototype.initialize = function() {
	var reply = { status: 'ok' };
	var self  = this;
	if (this.server.role == Constants.ROLE_MASTER) {
		this.role = Constants.ROLE_SLAVE;
		this._writeJSON({
			status   : 'ok',
			your_role: 'slave',
			my_role  : 'master'
		});
		if (this.connected()) {
			this._startReplication();
		}
	} else {
		console.assert(this.server.role == Constants.ROLE_SLAVE);
		this.role = Constants.ROLE_SLAVE;
		this._writeJSON({
			status     : 'not-master',
			master_host: this.server.masterHostName,
			master_port: this.server.masterPort
		});
	}
}


ReplicaSlave.prototype.onData = function(data) {
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


ReplicaSlave.prototype._startReplication = function() {
	console.assert(this.state == UNINITIALIZED);
	
	/* Ask the member what his table of contents is.
	 * Upon receiving a reply, begin synchronizing.
	 */
	var self = this;
	this._writeJSON({ command: 'getToc' });
	this.state = BACKGROUND_SYNCHRONIZING;
	this.expectedInput = EXPECTING_JSON_OBJECT;
	this.onJSON = function(toc) {
		delete self.expectedInput;
		delete self.onJSON;
		self.input.pause();
		self._continueReplication(toc);
	}
}

ReplicaSlave.prototype._continueReplication = function(toc) {
	console.assert(this.state == BACKGROUND_SYNCHRONIZING);
	this.toc = toc;
	this.input.pause();
	
	/* Replication consists of three phases.
	 *
	 * Phase 1: The background synchronization phase
	 * (state == BACKGROUND_SYNCHRONIZING). The master synchronizes
	 * the slave's contents with that of the master's. This happens
	 * in the background.
	 *
	 * Phase 2: The locked synchronization phase
	 * (state == LOCKED_SYNCHRONIZING). When phase 1 is done there may
	 * still be database modification operations in progress. So the
	 * database is locked and the final changes are synchronzied to
	 * the slave, and then the database is unlocked.
	 *
	 * Phase 3: The replication phase (state == READY). The master will
	 * forward all database modifications to the slave.
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
		if (!replicatorStarted
		 && self.state != BACKGROUND_SYNCHRONIZING
		 && self.state != LOCKED_SYNCHRONIZING
		 && self.connected())
		{
			replicatorStarted = true;
			process.nextTick(processNextReplicationCommand);
		}
	}
	
	
	/* Checks in what way the slave differs from the master and schedule
	 * appropriate commands to synchronize the slave with the master.
	 */
	function scheduleSlaveSynchronizationCommands() {
		/* Find groups or time entries that exist on this replica slave but not in
		 * our database, and schedule them for pruning on the replica slave.
		 *
		 * Time entries that are larger on the replica slave than in our database
		 * are also scheduled for pruning.
		 *
		 * Check whether the remaining time entries' sizes on the replica slave
		 * match those in our database, and if not, schedule them for filling.
		 *
		 * We explicitly check against TimeEntry.writtenSize instead of
		 * TimeEntry.dataFileSize because we don't want to replicate data that's
		 * still being written to the filesystem.
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
							self._logDebug("Scheduling refill: %s/%d (%s)",
								groupName, dst,
								"size on master smaller than on slave");
							workQueue.push({
								command: PRUNE_ONE_COMMAND,
								groupName: groupName,
								dayTimestamp: dst
							});
							workQueue.push({
								command: FILL_COMMAND,
								groupName: groupName,
								dayTimestamp: dst,
								timeEntry: localTimeEntry
							});
							localTimeEntry.incReadOperations();
						}

					} else {
						self._logDebug("Scheduling prune: %s/%d (%s)",
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
				self._logDebug("Scheduling prune: %s (%s)",
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
						self._logDebug("Scheduling fill: %s/%d (%s)",
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
						self._logDebug("Scheduling fill: %s/%d (%s)",
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
	}
	
	
	/*************** Replication protocol event handlers ***************/
	
	/* Main entry point for the replicator (processing scheduled work items). */
	function processNextReplicationCommand() {
		if (!self.connected()) {
			return;
		}
		
		var details = workQueue.shift();
		if (details) {
			currentWorkItem = details;
			if (details.command == PRUNE_ONE_COMMAND) {
				delete details.command;
				pruneOne(details);
			} else if (details.command == PRUNE_ALL_COMMAND) {
				delete details.command;
				pruneAll(details);
			} else {
				console.assert(details.command == FILL_COMMAND);
				delete details.command;
				fill(details);
			}
			
		} else if (self.state == BACKGROUND_SYNCHRONIZING) {
			console.assert(!replicatorStarted);
			currentWorkItem = undefined;
			
			self._log("Background synchronization almost done; checking concurrent modifications");
			scheduleSlaveSynchronizationCommands();
			if (workQueue.length > 0) {
				self._log("Restarting background synchronization because of concurrent modifications");
				processNextReplicationCommand();
			} else {
				self._log("Background synchronization done");
				doneBackgroundSynchronizing();
			}
		
		} else if (self.state == LOCKED_SYNCHRONIZING) {
			console.assert(!replicatorStarted);
			currentWorkItem = undefined;
			
			// Extra bug check.
			scheduleSlaveSynchronizationCommands();
			console.assert(workQueue.length == 0);
			
			self._log("Locked synchronization done");
			doneLockedSynchronizing();
			
		} else {
			console.assert(self.state == READY);
			console.assert(replicatorStarted);
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
		if (details.dataBuffers) {
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
		
		if (details.dataBuffers) {
			console.assert(self.state == READY);
			fillBySendingBuffers(details);
		} else {
			console.assert(self.state == BACKGROUND_SYNCHRONIZING
				|| self.state == LOCKED_SYNCHRONIZING);
			fillByStreamingDataFile(details);
		}
	}
	
	function fillBySendingBuffers(details) {
		console.assert(self.state == READY);
		
		var i, totalDataSize = 0;
		for (i = 0; i < details.dataBuffers.length; i++) {
			totalDataSize += details.dataBuffers[i].length;
		}
		
		var message = {
			command: 'add',
			group: details.groupName,
			dayTimestamp: details.dayTimestamp,
			size: totalDataSize
		};
		self._writeJSON(message);
		for (i = 0; i < details.dataBuffers.length; i++) {
			self._write(details.dataBuffers[i]);
		}
		self._writeJSON({ command: 'results' }, function(err) {
			sentAllBuffers(err, details);
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
				details.timeEntryOnReplica.size += details.size;
				cleanupWorkItem(details);
				processNextReplicationCommand();
			} else {
				self._disconnect();
			}
		}
		self.input.resume();
	}
	
	function fillByStreamingDataFile(details) {
		console.assert(self.state == BACKGROUND_SYNCHRONIZING
			|| self.state == LOCKED_SYNCHRONIZING);
		
		details.timeEntry.each(details.timeEntryOnReplica.size,
			function(err, buf, rawSize, continueReading, stop)
		{
			if (!self.connected()) {
				stop();
			} else if (err) {
				self._logError("Cannot read data file on master: " + err);
				self._disconnectWithError("Cannot read data file on master: " + err);
			} else if (buf.length > 0) {
				var message = {
					command: 'add',
					group: details.groupName,
					timestamp: details.dayTimestamp * 24 * 60 * 60,
					size: buf.length,
					opid: 1
				};
				self._writeJSON(message);
				self._write(buf);
				self._writeJSON({ command: 'results' }, function(err) {
					sentPieceOfDataFile(err, details,
						buf, rawSize, continueReading, stop);
				});
			} else {
				cleanupWorkItem(details);
				processNextReplicationCommand();
			}
		});
	}
	
	function sentPieceOfDataFile(err, details, buf, rawSize, continueReading, stop) {
		if (err || !self.connected()) {
			stop();
			return;
		}
		console.assert(self.state == BACKGROUND_SYNCHRONIZING
			|| self.state == LOCKED_SYNCHRONIZING);
		
		self.expectedInput = EXPECTING_JSON_OBJECT;
		self.onJSON = function(reply) {
			delete self.expectedInput;
			delete self.onJSON;
			self.input.pause();
			
			if (reply && reply.status == 'ok') {
				details.timeEntryOnReplica.size += rawSize;
				continueReading();
			} else {
				stop();
				self._disconnect();
			}
		}
		self.input.resume();
	}
	
	
	/*************** Database and connection event handlers ***************/
	
	function onAddToOurDatabase(groupName, dayTimestamp, offset, size, dataBuffers, done) {
		if (!self.connected()) {
			done();
			return;
		}
		console.assert(self.state != LOCKED_SYNCHRONIZING);
		if (self.state == READY) {
			var localTimeEntry = database.findTimeEntry(groupName, dayTimestamp);
			localTimeEntry.incReadOperations();
			self._logDebug("Scheduling fill: %s/%d", groupName, dayTimestamp);
			workQueue.push({
				command: FILL_COMMAND,
				groupName: groupName,
				dayTimestamp: dayTimestamp,
				timeEntry: localTimeEntry,
				dataBuffers: dataBuffers,
				size: size
			});
			startReplicator();
		}
		done();
	}
	
	function onRemoveFromOurDatabase(groupName, dayTimestamp) {
		if (!self.connected()) {
			return;
		}
		console.assert(self.state != LOCKED_SYNCHRONIZING);
		if (self.state == READY) {
			if (dayTimestamp) {
				self._logDebug("Scheduling prune: %s/%d",
					groupName, dayTimestamp);
				workQueue.push({
					command: PRUNE_ALL_COMMAND,
					group: groupName,
					dayTimestamp: dayTimestamp
				});
			} else {
				self._logDebug("Scheduling prune:", groupName);
				workQueue.push({
					command: PRUNE_ONE_COMMAND,
					group: groupName
				});
			}
			startReplicator();
		}
	}
	
	function onConnectionClose() {
		database.removeListener('add', onAddToOurDatabase);
		database.removeListener('remove', onRemoveFromOurDatabase);
		if (self.onJSON) {
			// Stop streamRead() call and decrease TimeEntry
			// read operations counter
			self.onJSON(undefined);
		}
		cleanupWorkQueue();
	}
	
	
	/*********************************************/
	
	function doneBackgroundSynchronizing() {
		console.assert(workQueue.length == 0);
		console.assert(self.onJSON === undefined);
		console.assert(currentWorkItem === undefined);
		self._log("Switching to locked synchronization phase");
		self.state = LOCKED_SYNCHRONIZING;
		database.lock(function() {
			console.assert(workQueue.length == 0);
			scheduleSlaveSynchronizationCommands();
			processNextReplicationCommand();
		});
	}
	
	function doneLockedSynchronizing() {
		console.assert(workQueue.length == 0);
		console.assert(self.onJSON === undefined);
		console.assert(currentWorkItem === undefined);
		self._log("Done synchronizing; pinging replica slave");
		
		self._writeJSON({ command: 'ping' });
		self.expectedInput = EXPECTING_JSON_OBJECT;
		self.onJSON = function(reply) {
			delete self.expectedInput;
			delete self.onJSON;
			delete this.input.onData;
			
			if (reply) {
				self._log("Replica slave responded and is now READY");
				self.state = READY;
				database.on('add', onAddToOurDatabase);
				database.on('remove', onRemoveFromOurDatabase);
				database.unlock();
			}
		}
		self.input.resume();
	}
	
	this.socket.on('close', onConnectionClose);
	scheduleSlaveSynchronizationCommands();
	processNextReplicationCommand();
}


exports.ReplicaSlave = ReplicaSlave;
exports.READY = READY;
