var argv   = require('./optimist.js').argv;
var log    = require('./default_log.js').log;

var Constants = require('./constants.js');

var server;

if(argv['shard-server']) {
	var ShardServer = require('./shards.js');
	var Server = require('./server.js');
	server = new ShardServer.ShardServer();
	var port   = argv.port || Constants.DEFAULT_PORT;
	server.start(argv.bind, port);
} else {
	var Server = require('./server.js');
	server = new Server.Server(argv.dbpath || 'db');
	var port   = argv.port || Constants.DEFAULT_PORT;
	if (argv.slave) {
		server.startAsSlave(argv.bind, port,
			argv['public-host-name'],
			argv['master-host'],
			argv['master-port'] || Constants.DEFAULT_PORT);
	} else {
		server.startAsMaster(argv.bind, port);
	}
}

var stopCount = 0;

function stop() {
	stopCount++;
	if (stopCount == 1) {
		log.notice('[Server] Stopping server...');
		server.close();
	} else if (stopCount == 2) {
		log.notice('[Server] Stopping in progress...');
	} else {
		log.emergency('[Server] Forcing stop!');
		process.exit(1);
	}
}

process.on('SIGINT', stop);
process.on('SIGTERM', stop);

process.on('uncaughtException', function(e) {
	try {
		if (e.stack) {
			console.error(e.stack);
		} else {
			console.error("Uncaught exception:");
			console.error(e);
		}
		console.error("-------- Server state --------");
		console.error(server.inspect());
	} catch (e2) {
		console.error("\n***** EXCEPTION IN EXCEPTION HANDLER *****");
		if (e2.stack) {
			console.error(e2.stack);
		} else {
			console.error(e2);
		}
	}
	process.exit(1);
});
