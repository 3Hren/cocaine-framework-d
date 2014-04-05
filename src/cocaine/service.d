module cocaine.service;

import std.container;
import std.stdio;
import std.variant;

import msgpack;

import vibe.vibe;
import vibe.core.log;

import cocaine.protocol;

T popFront(T)(ref DList!T container) {
    T value = container.front();
    container.removeFront();
    return value;
}

T as(T)(ref CocaineChunk chunk) {
    T value;
    msgpack.unpack(cast(ubyte[])chunk.data[0], value);
    return value;
}

class ServiceError : Exception {
    public this(Args...)(string reason, Args args) {
        super(format(reason, args));
    }
}

class Downstream {
    struct Message {
        Algebraic!(CocaineChunk, CocaineError) message;

        public this(CocaineChunk message) {
            this.message = message;
        }

        public this(CocaineError message) {
            this.message = message;
        }

        public T as(T)() {
            if (message.convertsTo!CocaineError) {
                auto error = message.get!CocaineError;
                throw new Exception(error.reason.message);
            }

            auto chunk = message.get!CocaineChunk;
            return chunk.as!T;
        }
    }

    private DList!Message messages;
    private ManualEvent event;
    private bool closed;

    public this() {
        this.event = createManualEvent();
    }

    public void wait() {
        if (messages.empty && closed) {
            throw new Exception("stream is closed");
        }
        event.wait();
    }

    public void waitAll() {}

    public T read(T)() {
        if (messages.empty && closed) {
            throw new Exception("stream is closed");
        }

        if (messages.empty) {
            event.wait();
            if (closed && messages.empty) {
                throw new Exception("choke");
            }
        }

        Message message = messages.popFront();
        return message.as!T;
    }

    private void chunk(CocaineChunk chunk) {
        messages.insertBack(Message(chunk));
        event.emit();
    }

    private void error(CocaineError error) {
        messages.insertBack(Message(error));
        event.emit();
    }

    private void close() {
        closed = true;
        event.emit();
    }
}

struct Defaults {
    struct Locator {
        string host = "localhost";
        ushort port = 10053;
    }

    static Locator LOCATOR = Locator();
}


struct ResolveInfo {
    struct Endpoint {
        string host;
        ushort port;
    }

    Endpoint endpoint;
    uint version_;
    string[uint] api;
}

class BaseService {
    private TCPConnection connection;
    private ulong session;
    private Downstream[ulong] sessions;

    public bool isConnected() @property const {
        return connection !is null && connection.connected;
    }

    private Downstream sendMessage(Args...)(uint id, Args data) {
        auto packer = Packer();
        packer.beginArray(3);
        packer.pack(id);
        packer.pack(session);
        packer.beginArray(data.length);
        packer.pack(data);

        logTrace("[Cocaine]: sending message '%s'", packer.stream.data);

        Downstream downstream = new Downstream();
        sessions[session] = downstream;
        session++;

        connection.write(packer.stream.data);
        return downstream;
    }

    private void run() {
        // Need guard to be sure that only one task per service is running.
        runTask({
            auto unpacker = StreamingUnpacker(cast(ubyte[])null);

            while (isConnected) {
                ubyte[] response = new ubyte[connection.leastSize];
                connection.read(response);
                unpacker.feed(response);

                foreach (unpacked; unpacker) {
                    logDiagnostic("[Cocaine]: message %s", unpacked);
                    MessageType messageId = unpacked[0].as!MessageType;
                    ulong session = unpacked[1].as!ulong;
                    logDiagnostic("[Cocaine]: message id=%d, session=%d", messageId, session);

                    auto downstream = sessions[session];
                    final switch (messageId) {
                        case MessageType.CHUNK: {
                            auto chunk = unpacked.as!CocaineChunk;
                            downstream.chunk(chunk);
                            break;
                        }
                        case MessageType.ERROR: {
                            auto error = unpacked.as!CocaineError;
                            downstream.error(error);
                            break;
                        }
                        case MessageType.CHOKE: {
                            auto choke = unpacked.as!CocaineChoke;
                            downstream.close();
                            sessions.remove(session);
                            break;
                        }
                    }
                }
            }
        });
    }
}

class Locator : BaseService {
    public this() {
        connect(Defaults.LOCATOR.host, Defaults.LOCATOR.port);
    }

    public void connect(string host, ushort port) {
        // Lock here.
        if (isConnected) {
            return;
        }

        logDiagnostic("[Cocaine]: connecting to the Locator service at %s:%d", host, port);
        connection = connectTCP(host, port);
        run();
    }

    public ResolveInfo resolve(string name) {
        auto downstream = sendMessage(0, name);
        return downstream.read!ResolveInfo();
    }
}

class Service : BaseService {
    private const string name;
    private uint[string] api;

    public this(string name) {
        this.name = name.dup;
    }

    public void connect() {
        // Lock here.
        logDebug("[Cocaine]: resolving");
        if (isConnected) {
            return;
        }

        Locator locator = new Locator();
        auto resolveInfo = locator.resolve(name);
        logDebug("[Cocaine]: service '%s' resolved - %s", name, resolveInfo);
        connection = connectTCP(resolveInfo.endpoint.host, resolveInfo.endpoint.port);
        foreach (methodId, methodName; resolveInfo.api) {
            api[methodName] = methodId;
        }

        run();
    }

    public Downstream opDispatch(string method, Args...)(Args args) {
        logTrace("[Cocaine]: handling %s(%s)", method, args);
        if (!isConnected) {
            connect();
        }

        if (method !in api) {
            throw new ServiceError("service '%s' has no method '%s'", name, method);
        }

        auto downstream = sendMessage(api[method], args);
        return downstream;
    }
}

struct CocaineService {
    string name;
};

@CocaineService("storage")
interface Storage {
    string read(string collection, string key);
    void write(string collection, string key, string blob);
}
