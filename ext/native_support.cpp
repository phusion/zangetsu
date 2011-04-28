#include <v8.h>

using namespace v8;

static Handle<Value>
disableStdioBuffering(const Arguments &args) {
	HandleScope scope;
	setvbuf(stdout, NULL, _IONBF, 0);
	setvbuf(stderr, NULL, _IONBF, 0);
	return args.This();
}

static Handle<Value>
flushStdio(const Arguments &args) {
	HandleScope scope;
	fflush(stdout);
	fflush(stderr);
	return args.This();
}

extern "C" void
init(Handle<Object> target) {
	HandleScope scope;
	target->Set(String::NewSymbol("disableStdioBuffering"),
		FunctionTemplate::New(disableStdioBuffering)->GetFunction());
	target->Set(String::NewSymbol("flushStdio"),
		FunctionTemplate::New(flushStdio)->GetFunction());
}
