var NativeSupport = require('./native_support.node');
var Server = require('./server.js');
NativeSupport.disableStdioBuffering();
new Server.Server("db").listen(3000);
