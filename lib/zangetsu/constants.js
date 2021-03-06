exports.DEFAULT_PORT   = 6420;

exports.PROTOCOL_MAJOR = 1;
exports.PROTOCOL_MINOR = 0;

// Server roles
exports.ROLE_UNKNOWN = 0;
exports.ROLE_MASTER  = 1;
exports.ROLE_SLAVE   = 2;
exports.ROLE_SHARD_ROUTER = 3;

// Server states
exports.SS_UNINITIALIZED = 0;
exports.SS_READY         = 2;
exports.SS_CLOSED        = 3;

// Replication settings
exports.DEFAULT_REPLICATION_RESULT_CHECK_THRESHOLD = 100;
