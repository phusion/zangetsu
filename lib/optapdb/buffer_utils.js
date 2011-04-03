/** Converts a 32-bit unsigned integer into a 32-bit binary buffer, big endian encoding. */
exports.uintToBuffer = function(i, buf, start) {
	if (!buf) {
		buf = new Buffer(4);
	}
	if (!start) {
		start = 0;
	}
	buf[start + 0] = (i & 0xff000000) >> 24;
	buf[start + 1] = (i & 0xff0000) >> 16;
	buf[start + 2] = (i & 0xff00) >> 8;
	buf[start + 3] = (i & 0xff);
	return buf;
}

/** Converts a 32-bit binary buffer, big endian encoding, into a 32-bit unsigned integer. **/
exports.bufferToUint = function(buf) {
	return buf[0] << 24 | buf[1] << 16 | buf[2] << 8 | buf[3];
}
