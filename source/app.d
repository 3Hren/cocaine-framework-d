import std.stdio;
import std.traits : FunctionTypeOf, ParameterTypeTuple, ReturnType;
import std.typecons;
import std.typetuple;

import msgpack;

import vibe.vibe;
import vibe.core.log;

import cocaine.protocol;

enum MessageType : uint {
	CHUNK = 4,
	ERROR = 5,
	CHOKE = 6
}

struct Packet {
	uint id;
	ulong session;
	string[] data;
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

struct ResolveInfo {
	Tuple!(string, "host", ushort, "port") address;
	uint version_;
	string[uint] api;
}

struct Downstream {
	private TCPConnection connection;
	private StreamingUnpacker unpacker = StreamingUnpacker(cast(ubyte[])null);

	this(TCPConnection connection) {
		this.connection = connection;
	}	

	Tuple!T readAll(T...)() {
		Tuple!T result;
		foreach(ref item; result.tupleof)
			read!(typeof(item))(item);
		return result;
	}

	T readAll(T)() {
		T result;
		read!(T)(result);
		return result;
	}

	private void read(T)(ref T item) {
		Done: while (true) {
			ubyte[] response = new ubyte[connection.leastSize];			
			connection.read(response);
			unpacker.feed(response);

			while (unpacker.execute()) {
				auto unpacked = unpacker.purge();
				MessageType messageId = unpacked[0].as!(MessageType);
				logDiagnostic("message id=%d", messageId);

				final switch (messageId) {
					case MessageType.CHUNK: {
						Chunk chunk = unpacked.as!(Chunk);
						msgpack.unpack(cast(ubyte[])chunk.data[0], item);
						break;
					}
					case MessageType.ERROR: {
						Error error = unpacked.as!(Error);
						throw new Exception(error.reason.message);
					}
					case MessageType.CHOKE: {
						Choke choke = unpacked.as!(Choke);
						break Done;
					}
				}
			}
		}
	}
}

struct Locator {
	private TCPConnection conn;
	private ulong session;

	static public Locator get() {
		Locator locator;
		locator.connect();
		return locator;
	}	

	public void connect(string host = "localhost", ushort port = 10053) {
		if (conn is null || !conn.connected) {
			conn = connectTCP(host, port);				
		}
	}

	public ResolveInfo resolve(string name) {
		Downstream downstream = sendMessage(0, [name]);
		ResolveInfo info = downstream.readAll!(ResolveInfo);
		logDiagnostic("service '%s' resolved: %s", name, info);
		return info;
	}

	private Downstream sendMessage(uint id, string[] data) {		
		Packet packet = {id, session++, data};
		return sendMessage(packet);
	}

	private Downstream sendMessage(T)(T message) {	
		Downstream downstream = Downstream(conn);
		ubyte[] packed = msgpack.pack(message);
		conn.write(packed);
		return downstream;
	}
}

class Service {
	private TCPConnection conn;
	private ulong session;
	private string name;	

	this(string name) {
		this.name = name;
	}

	public void connect(string host = "localhost", ushort port = 10053) {
		if (conn is null || !conn.connected) {
			Locator locator = Locator.get();
			locator.connect(host, port);
			ResolveInfo info = locator.resolve(name);			
			conn = connectTCP(info.address.host, info.address.port);				
		}
	}

	public Downstream sendMessage(Args...)(uint id, Args data) {		
		auto packer = packer(appender!(ubyte[])());
		packer.beginArray(3);
		packer.pack(id);
		packer.pack(session++);
		packer.beginArray(data.length);	
		packer.pack(data);			

		Downstream downstream = Downstream(conn);
		conn.write(packer.stream.data);
		return downstream;
	}

	private Downstream sendMessage(uint id, string[] data) {		
		Packet packet = {id, session++, data};
		return sendMessage(packet);
	}

	private Downstream sendMessage(T)(T message) {	
		Downstream downstream = Downstream(conn);
		ubyte[] packed = msgpack.pack(message);
		conn.write(packed);
		return downstream;
	}	
}

struct CocaineService {
	string name;
};

@CocaineService("storage")
interface Storage {	
	string read(string collection, string key);
}

private template GetOverloadedMethods(T) {
    import std.typetuple : Filter;

    alias allMembers = TypeTuple!(__traits(allMembers, T));
    template follows(size_t i = 0) {
        static if (i >= allMembers.length) {
            alias follows = TypeTuple!();
        } else static if (!__traits(compiles, mixin("T."~allMembers[i]))) {
            alias follows = follows!(i + 1);
        } else {
            enum name = allMembers[i];

            template isMethod(alias f) {
                static if (is(typeof(&f) F == F*) && is(F == function))
                    enum isMethod = !__traits(isStaticFunction, f);
                else
                    enum isMethod = false;
            }
            alias follows = TypeTuple!(std.typetuple.Filter!(isMethod, __traits(getOverloads, T, name)), follows!(i + 1));
        }
    }
    alias GetOverloadedMethods = follows!();
}

private template staticIota(int beg, int end) {
    static if (beg + 1 >= end) {
        static if (beg >= end) {
            alias TypeTuple!() staticIota;
        } else {
            alias TypeTuple!(+beg) staticIota;
        }
    } else {
        enum mid = beg + (end - beg) / 2;
        alias staticIota = TypeTuple!(staticIota!(beg, mid), staticIota!(mid, end));
    }
}

private template mixinAll(mixins...) {
    static if (mixins.length == 1) {
        static if (is(typeof(mixins[0]) == string)) {
            mixin(mixins[0]);
        } else{
            alias mixins[0] it;
            mixin it;
        }
    } else static if (mixins.length >= 2) {
        mixin mixinAll!(mixins[ 0 .. $/2]);
        mixin mixinAll!(mixins[$/2 .. $ ]);
    }
}

template hasAttribute(T, Attribute) {
	enum hasAttribute = (staticIndexOf!(Attribute, typeof(__traits(getAttributes, T))) != -1);    
}

private template getServiceName(Service) {
	enum id = staticIndexOf!(CocaineService, typeof(__traits(getAttributes, Service)));
	enum attribute = __traits(getAttributes, Service)[id];
	enum getServiceName = __traits(getMember, attribute, "name");
}

struct Repository {
	static T create(T)() {	
		template FuncInfo(string s, F) {
            enum name = s;
            alias type = F;
        }

		alias Concat = GetOverloadedMethods!(T);

		template Uniq(members...) {
            static if (members.length == 0) {
                alias Uniq = TypeTuple!();
            } else {
                alias func = members[0];
                enum name = __traits(identifier, func);
                alias type = FunctionTypeOf!func;
                template check(size_t i, mem...) {
                    static if (i >= mem.length) {
                        enum ptrdiff_t check = -1;
                    } else {
                        enum ptrdiff_t check =
                            __traits(identifier, func) == __traits(identifier, mem[i]) &&
                            !is(DerivedFunctionType!(type, FunctionTypeOf!(mem[i])) == void)
                          ? i : check!(i + 1, mem);
                    }
                }

                enum ptrdiff_t x = 1 + check!(0, members[1 .. $]);
                static if (x >= 1) {
                    alias typex = DerivedFunctionType!(type, FunctionTypeOf!(members[x]));
                    alias remain = Uniq!(members[1 .. x], members[x + 1 .. $]);

                    static if (remain.length >= 1 && remain[0].name == name && !is(DerivedFunctionType!(typex, remain[0].type) == void)) {
                        alias F = DerivedFunctionType!(typex, remain[0].type);
                        alias Uniq = TypeTuple!(FuncInfo!(name, F), remain[1 .. $]);
                    } else {
                        alias Uniq = TypeTuple!(FuncInfo!(name, typex), remain);
                    }
                } else {
                    alias Uniq = TypeTuple!(FuncInfo!(name, type), Uniq!(members[1 .. $]));
                }
            }
        }

		alias TargetMembers = Uniq!(Concat);
                    
		final class Impl : T {
			private Service service;

			private template generateFunction(size_t i) {				
				enum name = TargetMembers[i].name;            				
				enum n = to!string(i);
				enum functionBody = "doIt!(ReturnType!(TargetMembers["~n~"].type))("~n~", args)";				
				enum generateFunction = "override ReturnType!(TargetMembers["~n~"].type) "~
					name~"(ParameterTypeTuple!(TargetMembers["~n~"].type) args) "~
                    "{ return "~functionBody~"; }";
			}		

			private T doIt(T, Args...)(uint id, Args args) {
				Downstream downstream = service.sendMessage(id, args);
				return downstream.readAll!(T);				
			}
		public:		
			this() {
				static assert(hasAttribute!(T, CocaineService), "cocaine services must be decorated with 'CocaineService' attribute");				 
				string name = getServiceName!T;
				service = new Service(name);
				service.connect();
			}

			mixin mixinAll!(staticMap!(generateFunction, staticIota!(0, TargetMembers.length)));						
		}

		return new Impl;
	}
}

int main() { 
	setLogLevel(LogLevel.trace);	

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
