var Log = require('./log.js');

exports.log    = new Log(Log.DEBUG, process.stdout);
exports.errlog = new Log(Log.DEBUG, process.stderr);
