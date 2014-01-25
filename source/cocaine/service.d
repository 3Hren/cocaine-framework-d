module cocaine.service;

import std.typecons;
import std.typetuple;

import msgpack;

import vibe.vibe;

import cocaine.stream;

struct CocaineService {
	string name;
};

struct ResolveInfo {
	Tuple!(string, "host", ushort, "port") endpoint;
	uint version_;
	string[uint] api;
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
		logDiagnostic("[Cocaine]: service '%s' resolved: %s", name, info);
		return info;
	}

	private Downstream sendMessage(uint id, string[] data) {		
		return sendMessage(Tuple!(uint, ulong, string[])(id, session++, data));
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
		import std.socket;

		if (conn !is null && conn.connected) {
			return;
		}

		Locator locator = Locator.get();
		locator.connect(host, port);					
		ResolveInfo info = locator.resolve(name);						

		Address[] addresses = getAddress(info.endpoint.host, info.endpoint.port);
		logDiagnostic("[Cocaine]: candidates: %s", addresses);
		foreach (Address address; addresses) {			
			try {
				logDiagnostic("[Cocaine]: trying: %s", address);
				conn = connectTCP(address.toAddrString(), info.endpoint.port);
				logDiagnostic("[Cocaine]: succeed");
				break;
			} catch (Exception err) {
				logWarn("[Cocaine]: %s", err.msg);
			}
		}

		if (conn is null) {
			throw new Exception("failed to connect to the service %s", name);
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

	private Downstream sendMessage(T)(T message) {	
		Downstream downstream = Downstream(conn);
		ubyte[] packed = msgpack.pack(message);
		conn.write(packed);
		return downstream;
	}	
}
