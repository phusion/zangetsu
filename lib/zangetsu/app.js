var Const  = require('./constants.js');
var argv   = require('./optimist.js').
	default('dbpath', 'db').
	default('port', Const.DEFAULT_PORT).
	default('master-port', Const.DEFAULT_PORT).
	argv;
var Server = require('./server.js');
var log    = require('./default_log.js').log;

if (argv.repair) {
	log.emergency('--repair is not yet implemented.');
	process.exit(1);
}

if(argv['shard-server']) {
	var ShardServer = require('./shard_server.js');
	var server = new ShardServer.ShardServer();
	server.start(argv.bind, arg.port);
} else {
	var server = new Server.Server(argv.dbpath);
	if (argv.slave) {
		server.startAsSlave(argv.bind, argv.port,
			argv['public-host-name'],
			argv['master-host'],
			argv['master-port']);
	} else {
		server.startAsMaster(argv.bind, argv.port);
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

function inspect() {
	console.error("-------- Server state --------");
	console.error(server.inspect());
}

process.on('SIGINT', stop);
process.on('SIGTERM', stop);
process.on('SIGQUIT', inspect);

process.on('uncaughtException', function(e) {
	try {
		if (e.stack) {
			console.error(e.stack);
		} else {
			console.error("Uncaught exception:");
			console.error(e);
		}
		inspect();
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
