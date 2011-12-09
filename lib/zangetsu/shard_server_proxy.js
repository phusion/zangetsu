var util   = require('util');
var events = require('events');
var http = require('http');
var net = require('net');

var log                 = require('./default_log.js').log;
var TimeEntry           = require('./time_entry.js');
var Group               = require('./group.js');
var HashSet             = require('./hashset.js').HashSet;
var SocketOutputWrapper = require('./socket_output_wrapper.js').SocketOutputWrapper;
var SocketInputWrapper  = require('./socket_input_wrapper.js').SocketInputWrapper;
var ioutils             = require('./io_utils.js');

function ShardServerProxy(configuration) {
	this.hostname = configuration.hostname;
	this.port = configuration.port;
	this.connected = false;
}

exports.ShardServerProxy = ShardServerProxy;
