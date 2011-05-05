var IOUtils = require('./io_utils.js');
var writeMessage              = IOUtils.writeMessage;
var parseJsonObjectFromStream = IOUtils.parseJsonObjectFromStream;


// Roles
const MASTER  = 0,
      SLAVE   = 1,
      UNKNOWN = 3;

// States
const UNINITIALIZED = 0,
      READY         = 1,
      DISCONNECTED  = 2,
      SYNC_WAITING_FOR_TOC = 10,
      SYNC_PRUNING         = 11,
      SYNC_FILLING         = 12,
      SYNCHRONIZED         = 13;


function ReplicaMember(server, socket, input, output, id) {
	this.server = server;
	this.socket = socket;
	this.input  = input;
	this.output = output;
	this.id     = id;
	this.role   = UNKNOWN;
	this.state  = UNINITIALIZED;
	this.buffer = '';
	
	/****** Slave-specific ******/
	
	
	
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
			this.synchronize(this.server.database, function() {
				self.state = READY;
			});
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
		switch (this.state) {
		case SYNC_WAITING_FOR_TOC:
			try {
				result = parseJsonObjectFromStream(this.buffer, data, consumed);
			} catch (e) {
				this._disconnectWithError(
					"Cannot parse TOC description " +
					JSON.stringify(this.buffer) +
					": " + e.message);
				return consumed;
			}
			consumed = result.pos;
			if (result.done) {
				this.buffer = '';
				this.input.pause();
				this._reallySynchronize(result.object);
				return consumed;
			} else {
				this.buffer += result.slice;
			}
			break;
		
		case SYNC_PRUNING:
			try {
				result = parseJsonObjectFromStream(this.buffer, data, consumed);
			} catch (e) {
				this._disconnectWithError(
					"Cannot parse removal message reply " +
					JSON.stringify(this.buffer) +
					": " + e.message);
				return consumed;
			}
			consumed = result.pos;
			if (result.done) {
				this.buffer = '';
				this.input.pause();
				this.receivedReplyForRemoveMessage(result.object);
				return consumed;
			} else {
				this.buffer += result.slice;
			}
			break;
		
		case SYNC_FILLING:
			try {
				result = parseJsonObjectFromStream(this.buffer, data, consumed);
			} catch (e) {
				this._disconnectWithError(
					"Cannot parse addRaw message reply " +
					JSON.stringify(this.buffer) +
					": " + e.message);
				return consumed;
			}
			consumed = result.pos;
			if (result.done) {
				this.buffer = '';
				this.input.pause();
				this.receivedReplyForAddRawMessage(result.object);
				return consumed;
			} else {
				this.buffer += result.slice;
			}
			break;
		
		default:
			throw Error('Unexpected state ' + this.state);
		}
	}
	
	return consumed;
}


/**
 * Synchronize this replica member with the given database.
 */
ReplicaMember.prototype.synchronize = function(database, callback) {
	console.assert(this.state == UNINITIALIZED || this.state == READY);
	
	/* Ask the member what his table of contents is.
	 * Upon receiving a reply, begin removing nonexistant groups.
	 */
	this._writeJSON({ command: 'getToc' });
	this.state = SYNC_WAITING_FOR_TOC;
	this.syncDatabase = database;
	this.syncReadyCallback = callback;
}

ReplicaMember.prototype._reallySynchronize = function(toc) {
	this.state = SYNC_PRUNING;
	this.toc   = toc;
	this.input.pause();
	
	var self       = this;
	var database   = this.syncDatabase;
	var pruneQueue = [];
	var fillQueue  = [];
	var groupName, localGroup, groupOnReplica, dst,
		localTimeEntry, timeEntryOnReplica;
	
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
		console.assert(self.state == SYNC_PRUNING);
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
			self.state = SYNC_FILLING;
			fillNext();
		}
	}
	
	function sentRemoveCommand(err) {
		if (self.connected() && !err) {
			self.input.resume();
		}
	}
	
	this.receivedReplyForRemoveMessage = function(reply) {
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
		console.assert(self.state == SYNC_FILLING);
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
			
			localTimeEntry.streamRead(timeEntryOnReplica.size,
				function(err, buf, continueReading, stop)
			{
				if (err) {
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
						sentAddRawCommand(err, timeEntryOnReplica,
							buf, stop, continueReading);
					});
				} else {
					fillNext();
				}
			});
			
		} else if (pruneQueue.length > 0) {
			/* Some things have been removed in our database in the
			 * mean time go back to the pruning state.
			 */
			self._logDebug("Done filling, going back to pruning");
			self.state = SYNC_PRUNING;
			pruneNext();
			
		} else {
			self._logDebug("Done filling, finalizing");
			done();
		}
	}
	
	function sentAddRawCommand(err, timeEntryOnReplica, buf, stop, continueReading) {
		if (err || !self.connected()) {
			stop();
		} else {
			self.receivedReplyForAddRawMessage = function(reply) {
				delete self.receivedReplyForAddRawMessage;
				if (reply && reply.status == 'ok') {
					timeEntryOnReplica.size += buf.length;
					continueReading();
				} else {
					stop();
					self._disconnect();
				}
			}
			self.input.resume();
		}
	}
	
	
	/* If any groups or time entries are removed from our database while
	 * pruning or filling is in progress then schedule them for pruning too.
	 * pruneNext() will eventually handle it.
	 */
	function onRemoveFromOurDatabase(groupName, dayTimestamp) {
		var groupOnReplica = toc[groupName];
		if (!groupFromReplica) {
			return;
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
				delete groupOnReplica[dst];
				pruneQueue.push({
					group: groupName,
					dayTimestamp: dst
				});
			}
		} else {
			self._logDebug("Concurrent remove, scheduling prune:",
				groupName, dayTimestamp);
			delete toc[groupName];
			pruneQueue.push({ group: groupName });
		}
	}
	
	/* If any time entries in our database have been written to
	 * while pruning or filling is in progress then schedule them
	 * for filling too. fillNext() will eventually handle it.
	 */
	function onAddToOurDatabase(groupName, dayTimestamp, offset, size, rawBuffers) {
		var groupOnReplica = toc[groupName];
		if (!groupOnReplica) {
			groupOnReplica = toc[groupName] = {};
		}
		var timeEntryOnReplica = groupOnReplica[dayTimestamp];
		if (!timeEntryOnReplica) {
			groupOnReplica[dayTimestamp] = { size: 0 };
		}
		self._logDebug("Concurrent add, scheduling fill:",
			groupName, dayTimestamp);
		fillQueue.push({ groupName: groupName, dayTimestamp: dayTimestamp });
	}
	
	function onConnectionClose() {
		if (self.receivedReplyForAddRawMessage) {
			// Stop streamRead() call
			self.receivedReplyForAddRawMessage(undefined);
		}
	}
	
	
	function done() {
		console.assert(pruneQueue.length == 0);
		console.assert(fillQueue.length == 0);
		database.removeListener('remove', onRemoveFromOurDatabase);
		database.removeListener('add', onAddToOurDatabase);
		self.socket.removeListener('close', onConnectionClose);
		delete self.receivedReplyForRemoveMessage;
		console.assert(self.receivedReplyForAddRawMessage === undefined);
		
		// TODO:
		// there may be add/remove commands in progress so we need
		// to make sure those are finished before we're done
		
		var callback = self.syncReadyCallback;
		self.state = SYNCHRONIZED;
		delete self.syncDatabase;
		delete self.syncReadyCallback;
		callback();
		self._disconnect();
	}
	
	database.on('remove', onRemoveFromOurDatabase);
	database.on('add', onAddToOurDatabase);
	this.socket.on('close', onConnectionClose);
	pruneNext();
}


exports.ReplicaMember = ReplicaMember;
exports.READY = READY;
