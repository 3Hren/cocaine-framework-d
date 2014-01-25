import std.stdio;

import vibe.vibe;
import vibe.core.log;

import cocaine.service;
import cocaine.repository;


@CocaineService("storage")
interface Storage {	
	string read(string collection, string key);
}

int main() { 
	setLogLevel(LogLevel.trace);
	setLogFormat(FileLogger.Format.thread);	

	runTask({					
		Storage storage = Repository.create!Storage;
		string blob = storage.read("secrets", "secret1");
		writeln("Blob: "~blob);
	});

	logDiagnostic("Running event loop...");	
	return runEventLoop();
}
