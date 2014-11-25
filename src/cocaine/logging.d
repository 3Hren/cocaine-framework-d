module cocaine.logging;

import std.array;
import std.format;
import std.stdio;

enum Level {
    TRACE,
    DEBUG,
    NOTICE,
    INFO,
    WARNING,
    ERROR
}

struct Logger {
    public void log(Level level, Args...)(in string message, lazy Args args) {
        auto writer = appender!string();
        formattedWrite(writer, message, args);
        auto format = "[%s %s %s]: %s";
        writefln(writer.data);
    }
}

static Logger getLogger(in string name) {
    return Logger();
}
