var Server = require('./server.js');
var server = new Server.Server("db");
//server.startAsMaster(undefined, 3000);
server.startAsSlave(undefined, 3000, 'Minato.local', '127.0.0.1', 4000);

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
