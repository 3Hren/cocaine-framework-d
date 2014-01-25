import std.stdio;
import std.typecons;
import std.typetuple;

import msgpack;

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

	// Query locator
	//runTask({			
	//	Locator locator = Locator.get();
	//	auto info = locator.resolve("echo");
	//	writeln(info);
	//});	

	// Query storage
	runTask({					
		Storage storage = Repository.create!Storage;
		string blob = storage.read("secrets", "secret1");
		writeln("Blob: "~blob);
	});

	logDiagnostic("Running event loop...");	
	return runEventLoop();
}
