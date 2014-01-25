module cocaine.protocol;

import std.typecons;

enum MessageType : uint {
	CHUNK = 4,
	ERROR = 5,
	CHOKE = 6
}

struct Chunk {
	MessageType id;
	ulong session;
	string[] data;	
}


struct Error {
	MessageType id;
	ulong session;
	Tuple!(uint, "errno", string, "message") reason;
}

struct Choke {
	MessageType id;
	ulong session;
	uint unused;
}