import std.concurrency;
import std.datetime;
import std.stdio;
import std.typecons;

import msgpack;

import vibe.vibe;
import vibe.core.log;

import cocaine.detail.util;
import cocaine.protocol;
import cocaine.repository;
import cocaine.service;

template isCocaineMessage(T) {
    enum isCocaineMessage = is(T == CocaineChunk) || is(T == CocaineError);
}

struct CocaineStream {
    private TCPConnection connection;
    private ulong session;

    public this(TCPConnection connection, ulong session) {
        this.connection = connection;
        this.session = session;
    }

    public void chunk(Args...)(Args args) if (Args.length > 0 && isMsgpackable!Args) {
        sendMessage!(MessageType.CHUNK)(session, msgpack.pack(args));
    }

    public void choke() {
        sendMessage!(MessageType.CHOKE)(session);
    }

    public void sendMessage(MessageType type, Args...)(ulong session, Args args) {
        auto packer = Packer();
        packer.beginArray(3);
        packer.pack(type);
        packer.pack(session);
        packer.beginArray(Args.length);
        static if (Args.length > 0) {
            packer.pack(args);
        }
        connection.write(packer.stream.data);
    }
}

class ServiceMock {
    public const string name;
    public Nullable!(void delegate()) onConnect;
    private void delegate(ref CocaineStream)[string] matchers;
    bool closed;

    public this(string name) {
        this.name = name.idup;
    }

    public void match(T, Args...)(Args args, void delegate(ref CocaineStream) callback) if (Args.length > 0 && isCocaineMessage!T && isMsgpackable!Args) {
        auto pattern = to!string(msgpack.pack(args));
        matchers[pattern] = callback;
    }

    public void serve(TCPConnection connection) {
        logDiagnostic("[RUNTIME]: new connection - %s", connection);
        if (!onConnect.isNull) {
            auto callback = onConnect.get();
            callback();
        }
        closed = false;
        auto unpacker = StreamingUnpacker(cast(ubyte[])null);

        while (connection.connected) {
            if (closed) {
                connection.close();
            }
            ubyte[] message = new ubyte[connection.leastSize];
            connection.read(message);
            unpacker.feed(message);
            foreach (unpacked; unpacker) {
                logDiagnostic("[RUNTIME]: server received message: %s", unpacked);
                ulong session = unpacked[1].as!ulong;
                auto raw = to!string(msgpack.pack(unpacked));

                if (raw in matchers) {
                    auto stream = CocaineStream(connection, session);
                    matchers[raw](stream);
                }
            }
        }
    }

    public void stop() {
        logDiagnostic("[RUNTIME]: stopping '%s' service mock ...", name);
        closed = true;
    }
}

struct RuntimeMock {
    private ServiceMock[ushort] services;
    private TCPListener listener;

    ~this() {
        stop();
    }

    public ServiceMock addService(string name, ushort port) {
        services[port] = new ServiceMock(name);
        return services[port];
    }

    public void run() {
        logDiagnostic("[RUNTIME]: starting runtime mock ...");
        runTask({
            foreach (port, service; services) {
                logDiagnostic("[RUNTIME]: starting service '%s' on port %d ...", service.name, port);
                listener = listenTCP(port, connection => service.serve(connection), "0.0.0.0");
            }
        });
    }

    public void stop() {
        logDiagnostic("[RUNTIME]: stopping runtime mock ...");
        listener.stopListening();
        foreach (port, service; services) {
            service.stop();
        }
    }
}

static this() {
    setLogLevel(LogLevel.trace);
    setLogFormat(FileLogger.Format.threadTime);
}

void main() {}

enum NEW_LINE = "================================================================================\n";

void runEventLoopWithTimeout(int timeout = 1) {
    bool fired;
    setTimer(dur!"seconds"(timeout), {
        fired = true;
        exitEventLoop();
    });

    runEventLoop();

    if (fired) {
        throw new Exception("Timeout");
    }
}

version (UnitTesting) {

unittest {
    auto runtime = RuntimeMock();
    auto locatorMock = runtime.addService("locator", 10053);
    locatorMock.match!CocaineChunk(0, 0, ["echo"], (ref stream){
        stream.chunk(ResolveInfo.Endpoint("localhost", 10054), 1, [0: "enqueue", 1: "info"]);
        stream.choke();
    });
    runtime.run();

    runTask({
        Locator locator = new Locator();
        auto resolveInfo = locator.resolve("echo");
        assert("localhost" == resolveInfo.endpoint.host);
        assert(10054 == resolveInfo.endpoint.port);
        assert(1 == resolveInfo.version_);
        assert("enqueue" == resolveInfo.api[0]);
        assert("info" == resolveInfo.api[1]);
        exitEventLoop();
    });

    runEventLoopWithTimeout();
    writeln(NEW_LINE);
}

unittest {
    // Locator connects and resolves hostname, but then disconnects. It should try to reconnect.
    auto runtime = RuntimeMock();
    auto locatorMock = runtime.addService("locator", 10053);
    locatorMock.onConnect = {
        writeln("!!!");
        locatorMock.stop();
    };
    locatorMock.match!CocaineChunk(0, 0, ["echo"], (ref stream){
        writeln("4");
        stream.chunk(ResolveInfo.Endpoint("localhost", 10054), 1, [0: "enqueue", 1: "info"]);
        stream.choke();
    });
    runtime.run();

    runTask({
        Locator locator = new Locator();
        auto resolveInfo = locator.resolve("echo");
        assert("localhost" == resolveInfo.endpoint.host);
        assert(10054 == resolveInfo.endpoint.port);
        assert(1 == resolveInfo.version_);
        assert("enqueue" == resolveInfo.api[0]);
        assert("info" == resolveInfo.api[1]);
        exitEventLoop();
    });

    runEventLoopWithTimeout();
}

}

version (FunctionalTesting) {

unittest {
    Locator locator = new Locator();
    runTask({
        auto resolveInfo = locator.resolve("echo");
        logDebug("%s", resolveInfo);
        exitEventLoop();
    });

    runEventLoop();
    logDebug(NEW_LINE);
}

unittest {
    ServiceManager manager = new ServiceManager();
    auto storage = manager.create!Storage;

    runTask({
        string blob = storage.read("manifests", "echo");
        logDebug("%s", blob);
        exitEventLoop();
    });

    runEventLoop();
    logDebug(NEW_LINE);
}

unittest {
    ServiceManager manager = new ServiceManager();
    auto storage = manager.create!Storage;

    runTask({
        storage.write("secrets", "secret", "42");
        logDebug("secret key has been written");
        exitEventLoop();
    });

    runEventLoop();
    logDebug(NEW_LINE);
}

unittest {
    auto echo = new Service("echo");

    runTask({
        auto stream = echo.enqueue("ping", "Hello World!");
        logDebug("%s", stream.read!string);
        exitEventLoop();
    });

    runEventLoop();
    logDebug(NEW_LINE);
}

}
