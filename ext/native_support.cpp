#include <v8.h>
#include <eio.h>
#include <node.h>
#include <cstdio>
#include <cerrno>
#include <fcntl.h>

#include <config.h>

using namespace v8;
using namespace node;


struct FunctionCallData {
	Persistent<Function> *callback;
	
	virtual ~FunctionCallData() { }
};

static int
eioFunctionCallDone(eio_req *req) {
	HandleScope scope;
	
	FunctionCallData *data = (FunctionCallData *) req->data;
	Persistent<Function> *callback = cb_unwrap(data->callback);
	ev_unref(EV_DEFAULT_UC);
	
	Local<Value> arg;
	if (req->result == -1) {
		arg = ErrnoException(req->errorno);
	} else {
		arg = Local<Value>::New(Undefined());
	}
	
	TryCatch try_catch;
	(*callback)->Call(Context::GetCurrent()->Global(), 1, &arg);
	if (try_catch.HasCaught()) {
		FatalException(try_catch);
	}
	
	cb_destroy(callback);
	delete data;
	
	return 0;
}


static Handle<Value>
disableStdioBuffering(const Arguments &args) {
	HandleScope scope;
	setvbuf(stdout, NULL, _IONBF, 0);
	setvbuf(stderr, NULL, _IONBF, 0);
	return Undefined();
}

static Handle<Value>
flushStdio(const Arguments &args) {
	HandleScope scope;
	fflush(stdout);
	fflush(stderr);
	return Undefined();
}


struct PosixFadviseData: public FunctionCallData {
	int   fd;
	int   advise;
	off_t offset;
	off_t len;
};

static int
posixFadviseWrapper(eio_req *req) {
	#ifdef HAVE_POSIX_FADVISE
		req->result = posix_fadvise(data.fd, data.offset, data.len, data.advise);
	#else
		req->result = -1;
		errno = ENOSYS;
	#endif
	return 0;
}

// fd, offset, len, advise, callback
static Handle<Value>
posixFadvise(const Arguments &args) {
	HandleScope scope;
	PosixFadviseData *data = new PosixFadviseData();
	eio_req *req;
	
	if (args.Length() < 4 || !args[0]->IsInt32() || !args[1]->IsInt32()
	 || !args[2]->IsInt32() || !args[3]->IsInt32()) {
		return ThrowException(Exception::TypeError(String::New("Bad argument")));
	}
	
	data->fd     = args[0]->Int32Value();
	data->offset = (off_t) args[1]->Int32Value();
	data->len    = (off_t) args[2]->Int32Value();
	data->advise = args[3]->Int32Value();
	
	if (args[4]->IsFunction()) {
		data->callback = cb_persist(args[4]);
		req = eio_custom(posixFadviseWrapper, EIO_PRI_DEFAULT, eioFunctionCallDone, data);
		assert(req);
		ev_ref(EV_DEFAULT_UC);
		return Undefined();
	} else {
		#ifdef HAVE_POSIX_FADVISE
			int ret = posix_fadvise(data->fd, data->offset, data->len, data->advise);
			int e = errno;
			delete data;
			if (ret == -1) {
				return ThrowException(ErrnoException(errno));
			} else {
				return Undefined();
			}
		#else
			delete data;
			return ThrowException(ErrnoException(ENOSYS));
		#endif
	}
}


struct PosixFallocateData: public FunctionCallData {
	int   fd;
	off_t offset;
	off_t len;
};

static int
posixFallocateWrapper(eio_req *req) {
	#ifdef HAVE_POSIX_FALLOCATE
		req->result = posix_fallocate(data.fd, data.offset, data.len);
	#else
		req->result = -1;
		errno = ENOSYS;
	#endif
	return 0;
}

// fd, offset, len, callback
static Handle<Value>
posixFallocate(const Arguments &args) {
	HandleScope scope;
	PosixFallocateData *data = new PosixFallocateData();
	eio_req *req;
	
	if (args.Length() < 3 || !args[0]->IsInt32() || !args[1]->IsInt32() || !args[2]->IsInt32()) {
		return ThrowException(Exception::TypeError(String::New("Bad argument")));
	}
	
	data->fd     = args[0]->Int32Value();
	data->offset = (off_t) args[1]->Int32Value();
	data->len    = (off_t) args[2]->Int32Value();
	
	if (args[3]->IsFunction()) {
		data->callback = cb_persist(args[3]);
		req = eio_custom(posixFallocateWrapper, EIO_PRI_DEFAULT, eioFunctionCallDone, data);
		assert(req);
		ev_ref(EV_DEFAULT_UC);
		return Undefined();
	} else {
		#ifdef HAVE_POSIX_FALLOCATE
			int ret = posix_fallocate(data->fd, data->offset, data->len);
			int e = errno;
			delete data;
			if (ret == -1) {
				return ThrowException(ErrnoException(errno));
			} else {
				return Undefined();
			}
		#else
			delete data;
			return ThrowException(ErrnoException(ENOSYS));
		#endif
	}
}


extern "C" void
init(Handle<Object> target) {
	HandleScope scope;
	target->Set(String::NewSymbol("disableStdioBuffering"),
		FunctionTemplate::New(disableStdioBuffering)->GetFunction());
	target->Set(String::NewSymbol("flushStdio"),
		FunctionTemplate::New(flushStdio)->GetFunction());
	
	target->Set(String::NewSymbol("posix_fadvise"),
		FunctionTemplate::New(posixFadvise)->GetFunction());
	#ifdef HAVE_POSIX_FADVISE
		target->Set(String::NewSymbol("POSIX_FADV_NORMAL"),
			Integer::New(POSIX_FADV_NORMAL));
		target->Set(String::NewSymbol("POSIX_FADV_SEQUENTIAL"),
			Integer::New(POSIX_FADV_SEQUENTIAL));
		target->Set(String::NewSymbol("POSIX_FADV_RANDOM"),
			Integer::New(POSIX_FADV_RANDOM));
		target->Set(String::NewSymbol("POSIX_FADV_WILLNEED"),
			Integer::New(POSIX_FADV_WILLNEED));
		target->Set(String::NewSymbol("POSIX_FADV_DONTNEED"),
			Integer::New(POSIX_FADV_DONTNEED));
		target->Set(String::NewSymbol("POSIX_FADV_NOREUSE"),
			Integer::New(POSIX_FADV_NOREUSE));
		target->Set(String::NewSymbol("hasPosixFadvise"),
			Boolean::New(true));
	#endif
	
	target->Set(String::NewSymbol("posix_fallocate"),
		FunctionTemplate::New(posixFallocate)->GetFunction());
	#ifdef HAVE_POSIX_FALLOCATE
		target->Set(String::NewSymbol("hasPosixFallocate"),
			Boolean::New(true));
	#endif
}
