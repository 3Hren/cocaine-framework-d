module cocaine.protocol;

import std.typecons;

enum MessageType : uint {
    CHUNK = 4,
    ERROR = 5,
    CHOKE = 6
}

struct CocaineChunk {
    MessageType id;
    ulong session;
    string[] data;
}

struct CocaineError {
    struct Reason {
        uint errno;
        string message;
    }

    MessageType id;
    ulong session;
    Reason reason;
}

struct CocaineChoke {
    MessageType id;
    ulong session;
    uint unused;
}
