var util   = require('util');
var events = require('events');

var log           = require('./default_log.js').log;
var TimeEntry     = require('./time_entry.js');
var Group         = require('./group.js');
var HashSet       = require('./hashset.js').HashSet;

function Shard(configuration) {
	this.hostname = configuration.hostname
}

Shard.prototype.getTOC = function(callback) {
	callback({});
}

exports.Shard = Shard;
