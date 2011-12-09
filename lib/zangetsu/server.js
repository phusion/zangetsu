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
var Database        = require('./database.js');
var Utils           = require('./utils.js');
var IOUtils         = require('./io_utils.js');
var log             = require('./default_log.js').log;
var AbstractServer  = require('./abstract_server.js');

var logInput  = IOUtils.logInput;
var logOutput = IOUtils.logOutput;
var readJsonObject      = IOUtils.readJsonObject;
var writeMessage        = IOUtils.writeMessage;
var disconnectWithError = IOUtils.disconnectWithError;

function Server(dbpath) {
	var self = this;
	this.dbpath = dbpath;
	this.replicaSlaves = {};
	this.replicaSlaveCount = 0;
	this.unidentifiedClients = new HashSet.HashSet();
	this.clients = {};
	this.clientCount = 0;
	this.lastClientId = 0;
	this.role = Constants.ROLE_UNKNOWN;
	this.state = Constants.SS_UNINITIALIZED;
	this.resultCheckThreshold = Constants.DEFAULT_REPLICATION_RESULT_CHECK_THRESHOLD;
	this.database = new Database.Database(dbpath);
}

for (var method in ReplicationMode) {
	Server.prototype[method] = ReplicationMode[method];
}

for (var method in AbstractServer) {
	Server.prototype[method] = AbstractServer[method];
}

Server.prototype.startAsMaster = function(host, port, publicHostName) {
	var self = this;
	console.assert(this.server === undefined);

	this._startAndReloadDatabase();
	this.server = net.createServer(function(socket) {
		this._onNewClientSocket(socket);
	}.bind(this));
	this.server.listen(port, host, function() {
		var addr   = self.server.address();
		self.role  = Constants.ROLE_MASTER;
		self.state = Constants.SS_READY;
		self.host  = Utils.determinePublicHostName(addr.address, publicHostName);
		self.port  = addr.port;
		log.info("Server started on %s:%d PID %d", self.host, self.port, process.pid);
	});
}

Server.prototype.inspect = function() {
	var i, client, clients = [];
	for (i = 0; i < this.clientCount; i++) {
		client = this.clients[i];
		clients.push({
			state: client.state,
			closeRequested: client.closeRequested,
			operationsInProgress: client.operationsInProgress
		});
	}

	var replicaSlave, replicaSlaves = [];
	for (i = 0; i < this.replicaSlaveCount; i++) {
		replicaSlave = this.replicaSlaves[i];
		replicaSlaves.push(replicaSlave);
	}

	var result = {
		dbpath: this.database.dbpath,
		role: this.getRoleName(),
		replicaSlaveCount: this.replicaSlaveCount,
		replicaSlaves: this.replicaSlaves,
		clientCount: this.clientCount,
		clients: clients
	};
	return util.inspect(result);
}

Server.prototype.getRoleName = function() {
	if (this.role == Constants.ROLE_MASTER) {
		return 'master';
	} else if (this.role == Constants.ROLE_SLAVE) {
		return 'slave';
	} else {
		return 'unknown';
	}
}

Server.prototype.getTopology = function() {
	var id, replicaSlave;
	var topology = {
		replica_members: [{
			host: this.host,
			port: this.port,
			role: this.getRoleName(),
			state: this.getStateName()
		}]
	};
	
	for (id in this.replicaSlaves) {
		replicaSlave = this.replicaSlaves[id];
		topology.replica_members.push({
			host: replicaSlave.host,
			port: replicaSlave.port,
			role: replicaSlave.getRoleName(),
			state: replicaSlave.getStateName()
		});
	}
	
	return topology;
}

exports.Server = Server;
