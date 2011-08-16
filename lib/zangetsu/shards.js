/*
 *  Zangetsu - high-performance append-only database
 *  Copyright (C) 2011  Phusion
 *
 *  This program is free software: you can redistribute it and/or modify
 *  it under the terms of the GNU Affero General Public License as published by
 *  the Free Software Foundation, either version 3 of the License, or
 *  (at your option) any later version.
 *
 *  This program is distributed in the hope that it will be useful,
 *  but WITHOUT ANY WARRANTY; without even the implied warranty of
 *  MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 *  GNU Affero General Public License for more details.
 *
 *  You should have received a copy of the GNU Affero General Public License
 *  along with this program.  If not, see <http://www.gnu.org/licenses/>.
 */

var sys  = require('sys');
var fs   = require('fs');
var path = require('path');
var net  = require('net');
var util = require('util');
var SocketInputWrapper  = require('./socket_input_wrapper.js');
var SocketOutputWrapper = require('./socket_output_wrapper.js');
var Constants       = require('./constants.js');
var HashSet         = require('./hashset.js');
var CommandHandler  = require('./command_handler.js');
var ReplicaSlave    = require('./replica_slave.js');
var ReplicationMode = require('./replication_mode.js');
var Client          = require('./client.js');
var Utils           = require('./utils.js');
var IOUtils         = require('./io_utils.js');
var log             = require('./default_log.js').log;
var AbstractServer  = require('./abstract_server.js');

var logInput  = IOUtils.logInput;
var logOutput = IOUtils.logOutput;
var writeMessage        = IOUtils.writeMessage;
var disconnectWithError = IOUtils.disconnectWithError;
var parseJsonObject     = IOUtils.parseJsonObject;
var parseJsonObjectFromStream = IOUtils.parseJsonObjectFromStream;

function ShardServer() {
	var self = this;
	this.replicaSlaves = {};
	this.replicaSlaveCount = 0;
	this.unidentifiedClients = new HashSet.HashSet();
	this.clients = {};
	this.clientCount = 1;
	this.lastClientId = 0;
	this.role  = Constants.SHARD_SERVER;
	this.state = Constants.SS_UNINITIALIZED;
}

for (var method in ReplicationMode) {
	ShardServer.prototype[method] = ReplicationMode[method];
}

for (var method in AbstractServer) {
	ShardServer.prototype[method] = AbstractServer[method];
}

ShardServer.prototype.startWithFD = function(fd, publicHostName) {
	this._start(publicHostname, function(server) {
		server.listenFD(fd);
	});
}

ShardServer.prototype.start = function(host, port, publicHostName) {
	this._start(publicHostName, function(server) {
		server.listen(port, host);
	});
}

ShardServer.prototype._start = function(publicHostName, initialize) {
	console.assert(this.server === undefined);

	this.server = net.createServer(function(socket) {
		this._onNewClientSocket(socket);
	}.bind(this));
	initialize(this.server);

	var addr = this.server.address();
	this.state = Constants.SS_READY;
	this.host  = Utils.determinePublicHostName(addr.address, publicHostName);
	this.port  = addr.port;
	this.database.start();
}

ShardServer.prototype.getRoleName = function() {
	return "shard-server";
}

exports.ShardServer = ShardServer;
