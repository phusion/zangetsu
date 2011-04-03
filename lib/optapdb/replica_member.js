const MASTER = 0,
      SLAVE  = 1;

const DELETING_STALE_DATA = 0,
      SYNCHING = 1,
      READY    = 2;

function ReplicaMember(socket, id) {
	this.socket = socket;
	this.id     = id;
	this.state  = MASTER;
	this.type   = DELETING_STALE_DATA;
	this.toc    = {};
	return this;
}

ReplicaMember.prototype.ready = function() {
	return this.type == READY;
}

exports.ReplicaMember = ReplicaMember;
