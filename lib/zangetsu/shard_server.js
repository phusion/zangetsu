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

var util = require('util');
var net  = require('net');
var Constants       = require('./constants.js');
var HashSet         = require('./hashset.js');
var Utils           = require('./utils.js');
var log             = require('./default_log.js').log;
var AbstractServer  = require('./abstract_server.js');
var ShardedDatabase = require('./sharded_database.js');

function ShardServer(configFile) {
	var self = this;
	this.replicaSlaves = {};
	this.replicaSlaveCount = 0;
	this.unidentifiedClients = new HashSet.HashSet();
	this.clients = {};
	this.clientCount = 1;
	this.lastClientId = 0;
	this.role  = Constants.ROLE_SHARD_ROUTER;
	this.state = Constants.SS_UNINITIALIZED;
	this.database = new ShardedDatabase.Database(configFile);
}

for (var method in AbstractServer) {
	ShardServer.prototype[method] = AbstractServer[method];
}

ShardServer.prototype.start = function(host, port, publicHostName) {
	var self = this;
	console.assert(this.server === undefined);

	this.server = net.createServer(function(socket) {
		this._onNewClientSocket(socket);
	}.bind(this));
	this.server.listen(port, host, function() {
		var addr   = self.server.address();
		self.state = Constants.SS_READY;
		self.host  = Utils.determinePublicHostName(addr.address, publicHostName);
		self.port  = addr.port;
		self.database.start();
	});
}

ShardServer.prototype.getRoleName = function() {
	return "shard-router";
}

ShardServer.prototype.inspect = function() {
	return util.inspect(this);
}

exports.ShardServer = ShardServer;
