module cocaine.service;

import std.container;
import std.stdio;
import std.variant;
import std.socket;
import std.typecons;

import msgpack;

import vibe.vibe;
import vibe.core.log;

import cocaine.protocol;
import cocaine.logging;

//struct NullTypes {}

//struct Upstream(State) {}
//struct Downstream(State) {}

//struct Write { string message; }
//struct Error { string message; }
//struct Close {}

//alias Algebraic!(Write, Error, Close) WriteTypes;

//struct Control(State = FinishState) {
//    alias Types = State.Types;

//    Upstream!State upstream;
//    Downstream!State downstream;
//}

//interface FinishState {
//    alias Types = NullTypes;
//}

//interface StreamingState {
//    alias Types = WriteTypes;

//    Control!StreamingState write(string message);
//    Control!() error(string reason);
//    Control!() close();
//}

//@CocaineService("storage")
//interface Storage {
//    Control!StreamingState read(string collection, string key);
//}

auto log = cocaine.logging.getLogger("cocaine");

T popFront(T)(ref DList!T container) {
    T value = container.front();
    container.removeFront();
    return value;
}

class ServiceError : Exception {
    public this(Args...)(string reason, Args args) {
        super(format(reason, args));
    }
}

class Upstream {}
class Downstream {
    private DList!Value messages;
    private ManualEvent event;
    private bool closed;

    public this() {
        this.event = createManualEvent();
    }

    public void wait() {
        if (messages.empty) {
            if (closed) {
                throw new Exception("stream is closed");
            }

            event.wait();
        }
    }

    public T read(T)() {
        wait();
        auto message = messages.popFront();
        return message.as!T;
    }

    private void push(Value message) {
        messages.insertBack(message);
        event.emit();
    }

    private void close() {
        closed = true;
        event.emit();
    }
}

class Control {
    public Upstream upstream;
    public Downstream downstream;

    public this() {
        this.upstream = new Upstream();
        this.downstream = new Downstream();
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

    alias State[uint] StateMap;

    struct State {
        string name;
        StateMap up;
        StateMap down;
    }

    Endpoint endpoint;
    uint version_;
    StateMap api;
}

struct Message {
    ulong session;
    uint id;
    ulong trace;
    msgpack.Value data;

    public this(msgpack.Unpacked unpacked) {
        this.session = unpacked[0].as!ulong;
        this.id = unpacked[1].as!uint;
        this.trace = unpacked[2].as!ulong;
        this.data = unpacked[3];
    }
}

class BaseService {
    private const string name;
    private ResolveInfo info;
    private ResolveInfo.StateMap state;

    private ulong session;
    private Control[ulong] sessions;

    private TCPConnection connection;

    public this(in string name) {
        this.name = name.dup;
    }

    @property
    public bool isConnected() const {
        return connection !is null && connection.connected;
    }

    public abstract void connect();

    private void connect(in string host, ushort port) {
        if (isConnected) {
            return;
        }

        logDiagnostic("[Cocaine]: connecting to the '%s' service at %s:%d", name, host, port);
        connection = connectTCP(host, port);
        run();
    }

    private Control invoke(Args...)(uint id, Args data) {
        connect();

        auto packer = Packer();
        packer.beginArray(4);
        packer.pack(++session);
        packer.pack(id);
        packer.pack(0);
        packer.beginArray(data.length);
        packer.pack(data);

        logTrace("[Cocaine]: sending message '%s'", packer.stream.data);

        // Transition.
        state = info.api[id].down;

        auto control = new Control();
        sessions[session] = control;
        connection.write(packer.stream.data);
        return control;
    }

    private void run() {
        // TODO: Need guard to be sure that only one task per service is running.
        runTask({
            logDiagnostic("[Cocaine]: service '%s' is running ...", name);
            auto unpacker = StreamingUnpacker(cast(ubyte[])null);

            while (isConnected) {
                ubyte[] response = new ubyte[connection.leastSize];
                connection.read(response);
                unpacker.feed(response);

                foreach (unpacked; unpacker) {
                    auto message = Message(unpacked);
                    logDebug("[Cocaine]: received Message(%d, %d)", message.session, message.id);
                    logDebug("[Cocaine]: %s", message.data);

                    if (message.session !in sessions) {
                        throw new Exception("received message with unknown/closed session");
                    }

                    auto control = sessions[message.session];
                    control.downstream.push(message.data);

                    logDebug("%s", state);
                    if (state[message.id].down !is null && state[message.id].down.length == 0) {
                        control.downstream.close();
                        sessions.remove(session);
                    }
                }
            }

            if (unpacker.size) {
                throw new Exception("message is too large");
            }
        });
    }
}

class Locator : BaseService {
    public this() {
        super("locator");

        with (ResolveInfo) {
            info = ResolveInfo(
                Endpoint(Defaults.LOCATOR.host, Defaults.LOCATOR.port),
                1,
                [
                    0: State(
                        "resolve",
                        StateMap.init,
                        [
                            0: State("write", null, StateMap.init),
                            1: State("error", StateMap.init, StateMap.init),
                            2: State("close", StateMap.init, StateMap.init)
                        ]
                    ),
                    1: State(
                        "synchronize",
                        StateMap.init,
                        [
                            0: State("write", null, StateMap.init),
                            1: State("error", StateMap.init, StateMap.init),
                            2: State("close", StateMap.init, StateMap.init)
                        ]
                    ),
                    2: State(
                        "refresh",
                        StateMap.init,
                        [
                            0: State("write", null, StateMap.init),
                            1: State("error", StateMap.init, StateMap.init),
                            2: State("close", StateMap.init, StateMap.init)
                        ]
                    )
                ]
            );
        }
    }

    override public void connect() {
        super.connect(info.endpoint.host, info.endpoint.port);
    }

    public ResolveInfo resolve(string name) {
        logDebug("[Cocaine]: resolving '%s' service ...", name);
        auto control = invoke(0, name);
        return control.downstream.read!ResolveInfo();
    }
}

class Service : BaseService {
    public this(in string name) {
        super(name);
    }

    override public void connect() {
        if (isConnected) {
            return;
        }

        auto locator = new Locator();
        info = locator.resolve(name);
        logDebug("[Cocaine]: service '%s' has been resolved - %s", name, info);
        AddressInfo[] addressInfos = getAddressInfo(
            info.endpoint.host,
            to!string(info.endpoint.port),
            ProtocolType.TCP,
            SocketType.STREAM
        );
        logDebug("[Cocaine]: candidates: %s", addressInfos);
        foreach (addressInfo; addressInfos) {
            try {
                super.connect(addressInfo.address.toAddrString(), info.endpoint.port);
                break;
            } catch (Exception err) {
                logWarn("[Cocaine]: %s", err.msg);
            }
        }
    }
}

struct CocaineService {
    string name;
};

interface Application {
}
