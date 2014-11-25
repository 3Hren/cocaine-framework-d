import std.concurrency;
import std.datetime;
import std.stdio;
import std.typecons;

import vibe.vibe;
import vibe.core.log;

import cocaine.detail.util;
import cocaine.protocol;
import cocaine.repository;
import cocaine.service;

static this() {
    setLogLevel(LogLevel.trace);
    setLogFormat(FileLogger.Format.threadTime);
}

void main() {}

version (FunctionalTesting) {

struct Apps {
    string[] name;
}

unittest {
    auto node = new Service("node");
//    auto manager = new ServiceManager();
//    auto storage = manager.create!Storage;

    runTask({
        auto control = node.invoke(2, "list");
        auto list = control.downstream.read!(Apps);

//        control.upstream.push("le message");
//        while (true) {
//            auto message = downstream.read().visit!(
//                (Write m) { return m; },
//                (Error e) { throw new Exception(e); },
//                (Close)   { break; }
//            )();
//        }

//        auto control = storage.read("profiles", "profile@test");
//        auto msg = control.downstream.read!string;

        logDebug("apps=%s", list);
        exitEventLoop();
    });

    runEventLoop();
}

} // FunctionalTesting
