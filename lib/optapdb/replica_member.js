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
      SYNC_WAITING_FOR_TOC = 10;


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
				this._removeNonExistantGroups(result.object, data.slice(pos));
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
	// Ask the member what his table of contents is.
	// Upon receiving a reply, begin removing nonexistant groups.
	writeMessage(this.socket, { command: 'getToc' });
	this.state = SYNC_WAITING_FOR_TOC;
	this.syncDatabase = database;
	this.syncReadyCallback = callback;
}

ReplicaMember.prototype._removeNonExistantGroups = function(toc, remainingData) {
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
	 * When done, begin creating time entries that exist in our
	 * database but not on the replica member.
	 */
	
	function done() {
		database.removeListener('remove', onRemoveFromOurDatabase);
		self._createNonexistantTimeEntries(remainingData);
	}
	
	function onRemoveFromOurDatabase(groupName, dayTimestamp) {
		var groupInToc = toc[groupName];
		if (!groupInToc) {
			return;
		}
		
		operations++;
		var message = { command: 'remove', group: groupName, dayTimestamp: dayTimestamp };
		writeMessage(self.socket, message, sentRemoveCommand);
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
			var message = { command: 'remove', group: groupName };
			writeMessage(this.socket, message, sentRemoveCommand);
		}
	}
}

ReplicaMember.prototype._createNonexistantTimeEntries = function(remainingData) {
	this.socket.destroy();
}


exports.ReplicaMember = ReplicaMember;
