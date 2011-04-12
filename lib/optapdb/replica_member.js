var IOUtils = require('./io_utils.js');
for (var field in IOUtils) {
	this[field] = IOUtils[field];
}


// Roles
const MASTER  = 0,
      SLAVE   = 1,
      UNKNOWN = 3;

// States
const UNINITIALIZED         = 0,
      RECEIVING_TOC         = 1,
      DELETING_STALE_GROUPS = 2,
      SYNCHING      = 3,
      READY         = 4,
      DISCONNECTED  = 5;


function ReplicaMember(server, socket, id) {
	this.server = server;
	this.socket = socket;
	this.id     = id;
	this.role   = UNKNOWN;
	this.state  = UNINITIALIZED;
	this.buffer = '';
	return this;
}

ReplicaMember.prototype.connected = function() {
	return state != DISCONNECTED && typeof(this.socket.fd) == 'number';
}

ReplicaMember.prototype.ready = function() {
	return this.type == READY;
}

ReplicaMember.prototype.initializeAsSlave = function(remainingData) {
	this.role = SLAVE;
	writeMessage(this.socket, { command: 'toc' });
	this.state = RECEIVING_TOC;
}

ReplicaMember.prototype.onData = function(data) {
	var pos = 0, found = false;
	var i, result;
	
	while (pos < data.length && this.connected()) {
		switch (this.state) {
		case RECEIVING_TOC:
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
				this._beginSynchronizingSlave(this.object);
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

ReplicaMember.prototype._beginSynchronizingSlave = function(toc) {
	this.toc = toc;
	
	// Find groups that exist on this slave but not in our
	// (master) database.
	var groupsToDelete = findNonexistantGroups(server.database, toc);
	var self = this;
	
	function doneDeleting() {
		
	}
	
	function deleteNextGroup() {
		var group = groupsToDelete.pop();
		if (group) {
			var message = { command: 'remove', group: group.name };
			writeMessage(self.socket, message, function(err) {
				if (err) {
					self.state = DISCONNECTED;
					self._logError(err.message);
				}
			});
		} else {
			doneDeleting();
		}
	}
	
	// Now tell replica slave to delete these groups.
	this.state = DELETING_STALE_GROUPS;
	this.nextCallback = deleteNextGroup;
	deleteNextGroup();
}

ReplicaMember.prototype._disconnectWithError = function(message) {
	disconnectWithError(this.socket, message);
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

exports.ReplicaMember = ReplicaMember;
