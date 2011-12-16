/**
 * Represents a connection from a normal client to a shard router.
 */

var util           = require('util');
var Client         = require('./client.js').Client;
var CommandHandler = require('./command_handler.js');
var log            = require('./default_log.js').log;
var ShardedDatabase = require('./sharded_database.js').Database;

function ShardRouterClient(server, socket, input, output, mode, id) {
	var shardedDatabase = new ShardedDatabase(server.database);
	Client.call(this, shardedDatabase, socket, input, output, mode);
}

util.inherits(ShardRouterClient, Client);

exports.ShardRouterClient = ShardRouterClient;
exports.DISCONNECTED = CommandHandler.DISCONNECTED;
