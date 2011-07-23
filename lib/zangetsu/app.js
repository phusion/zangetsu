var argv   = require('./optimist.js').argv;
var Server = require('./server.js');

var server = new Server.Server(argv.dbpath || 'db');
var port   = argv.port || Server.DEFAULT_PORT;
if (argv.slave) {
	server.startAsSlave(argv.bind, port,
		argv['public-host-name'],
		argv['master-host'],
		argv['master-port'] || Server.DEFAULT_PORT);
} else {
	server.startAsMaster(argv.bind, port);
}

function stop() {
	console.log('stop');
	server.close();
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
