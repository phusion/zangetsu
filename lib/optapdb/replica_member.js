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
      SYNC_REMOVING_NONEXISTANT_ENTRIES = 11;


function ReplicaMember(server, socket, id) {
	this.server = server;
	this.socket = socket;
	this.id     = id;
	this.role   = UNKNOWN;
	this.state  = UNINITIALIZED;
	this.buffer = '';
	
	/****** Slave-specific ******/
	
	
	
	return this;
}

ReplicaMember.prototype._disconnectWithError = function(message) {
	IOUtils.disconnectWithError(this.socket, message);
	this.state = DISCONNECTED;
}

ReplicaMember.prototype._log = function(message) {
	var args = ["[ReplicaMember %d] " + message, this.id];
	for (var i = 2; i < arguments.length; i++) {
		args.push(arguments[i]);
	}
	console.log.apply(console, args);
}

ReplicaMember.prototype._logError = function(message) {
	var args = ["[ReplicaMember %d ERROR] " + message, this.id];
	for (var i = 2; i < arguments.length; i++) {
		args.push(arguments[i]);
	}
	console.error.apply(console, args);
}

ReplicaMember.prototype.connected = function() {
	return this.state != DISCONNECTED && typeof(this.socket.fd) == 'number';
}

ReplicaMember.prototype.ready = function() {
	return this.type == READY;
}

ReplicaMember.prototype.onData = function(data) {
	var pos = 0, found = false;
	var i, result;
	
	while (pos < data.length && this.connected()) {
		switch (this.state) {
		case SYNC_WAITING_FOR_TOC:
			try {
				result = parseJsonObjectFromStream(this.buffer, data, pos);
			} catch (e) {
				this._disconnectWithError(
					"Cannot parse TOC description " +
					JSON.stringify(this.buffer) +
					": " + e.message);
				return;
			}
			pos = result.pos;
			if (result.done) {
				this.buffer = '';
				this.socket.pause();
				this.remainingData = data.slice(pos);
				this._removeNonExistantGroups(result.object);
				return;
			} else {
				this.buffer += result.slice;
			}
			break;
		
		case DELETING_STALE_GROUPS:
			try {
				result = parseJsonObjectFromStream(this.buffer, data, pos);
			} catch (e) {
				this._disconnectWithError(
					"Cannot parse remove result " +
					JSON.stringify(this.buffer) +
					": " + e.message);
				return;
			}
			pos = result.pos;
			if (result.done) {
				this.buffer = '';
				// Invoke deleteNextGroup().
				this.nextCallback();
			} else {
				this.buffer += result.slice;
			}
			break;
		
		default:
			throw Error('Unexpected state ' + this.state);
		}
	}
}

if (false) {
	timeEntry.streamRead(function(err, buf, continueReading, stop) {
		if (buf.length > 0) {
			writeMessage(slave.socket, {
				command: 'addRaw',
				group: group.name,
				dayTimestamp: entry.dayTimestamp,
				size: buf.length
			});
			slave.socket.write(buf, continueReading);
		}
	});
}


/**
 * Synchronize this replica member with the given database.
 */
ReplicaMember.prototype.synchronize = function(database, callback) {
	/* Ask the member what his table of contents is.
	 * Upon receiving a reply, begin removing nonexistant groups.
	 */
	writeMessage(this.socket, { command: 'getToc' });
	this.state = SYNC_WAITING_FOR_TOC;
	this.syncDatabase = database;
	this.syncReadyCallback = callback;
}

ReplicaMember.prototype._removeNonExistantGroups = function(toc) {
	this.state = SYNC_REMOVING_NONEXISTANT_ENTRIES;
	this.toc = toc;
	
	/* Find groups that exist on this replica member but not in our database. */
	var database = this.syncDatabase;
	var groupsToDelete = database.findNonexistantGroups(toc);
	var operations = groupsToDelete.length;
	var i, groupName, message;
	var self = this;
	
	/* Now tell the replica member to remove these groups. If any groups
	 * are removed from our database while this is in progress then
	 * delete those too.
	 * When done, remove time entries that exist on this replica member
	 * but not in our database.
	 */
	
	function done() {
		database.removeListener('remove', onRemoveFromOurDatabase);
		if (self.socket.bufferSize == 0) {
			self._removeNonExistantTimeEntries();
		} else {
			self.socket.once('drain', function() {
				self._removeNonExistantTimeEntries();
			});
		}
	}
	
	function onRemoveFromOurDatabase(groupName, dayTimestamp) {
		var groupInToc = toc[groupName];
		if (!groupInToc) {
			return;
		}
		
		operations++;
		if (dayTimestamp) {
			var dayTimestampsToDelete = [];
			var dst, i;
			for (dst in groupInToc) {
				if (parseInt(dst) < dayTimestamp) {
					dayTimestampsToDelete.push(dst);
				}
			}
			for (i = 0; i < dayTimestampsToDelete.length; i++) {
				delete groupInToc[dayTimestampsToDelete[i]];
			}
		} else {
			delete toc[groupName];
		}
		var message = { command: 'remove', group: groupName, dayTimestamp: dayTimestamp };
		writeMessage(self.socket, message, sentRemoveCommand);
	}
	
	function sentRemoveCommand(err) {
		if (!err) {
			operations--;
			if (operations == 0) {
				done();
			}
		}
	}
	
	if (operations == 0) {
		done();
	} else {
		database.on('remove', onRemoveFromOurDatabase);
		for (i = 0; i < groupsToDelete.length; i++) {
			groupName = groupsToDelete[i];
			delete toc[groupName];
			var message = { command: 'remove', group: groupName };
			writeMessage(this.socket, message, sentRemoveCommand);
		}
	}
}

ReplicaMember.prototype._removeNonExistantTimeEntries = function() {
	/* Remove time entries that exist on this replica member
	 * but not in our database.
	 * When done, begin creating time entries that exist in our
	 * database but not on the replica member.
	 */
	
	var database = this.syncDatabase;
	var groupName, localGroup, groupOnReplica, dst;
	var removalQueue = [];
	var self = this;
	
	for (groupName in this.toc) {
		localGroup = database.groups[groupName];
		console.assert(localGroup !== undefined);
		groupOnReplica = this.toc[groupName];
		
		for (dst in groupOnReplica) {
			if (!localGroup.timeEntries[dst]) {
				removalQueue.push({ group: groupName, dayTimestamp: parseInt(dst) });
			}
		}
	}
	
	function removeNext() {
		var details = removalQueue.pop();
		if (details) {
			var message = {
				command: 'removeOne',
				group: details.group,
				dayTimestamp: details.dayTimestamp
			};
			writeMessage(self.socket, message, removeNext);
		} else if (self.socket.bufferSize == 0) {
			self._createNonexistantTimeEntries();
		} else {
			self.socket.once('drain', function() {
				self._createNonexistantTimeEntries();
			})
		}
	}
	
	removeNext();
}

ReplicaMember.prototype._createNonexistantTimeEntries = function() {
	this.socket.destroy();
}


exports.ReplicaMember = ReplicaMember;
