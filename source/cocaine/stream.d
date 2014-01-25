module cocaine.stream;

import msgpack;

import vibe.vibe;
import vibe.core.log;

import cocaine.protocol;

struct Downstream {
	private TCPConnection connection;
	private StreamingUnpacker unpacker = StreamingUnpacker(cast(ubyte[])null);
	private bool closed;

	this(TCPConnection connection) {
		this.connection = connection;
	}	

	public Tuple!T readAll(T...)() {
		Tuple!T result;
		foreach(ref item; result.tupleof) {
			read(item);
		}
		return result;
	}

	public T readAll(T)() {
		T result;
		read(result);
		return result;
	}

	private void read(T)(ref T item) {
		while (!closed) {
			ubyte[] response = new ubyte[connection.leastSize];			
			connection.read(response);
			unpacker.feed(response);

			foreach (unpacked; unpacker) {
				MessageType messageId = unpacked[0].as!(MessageType);
				logDiagnostic("[Cocaine]: message id=%d", messageId);

				final switch (messageId) {
					case MessageType.CHUNK: {
						cocaine.protocol.Chunk chunk = unpacked.as!(cocaine.protocol.Chunk);
						msgpack.unpack(cast(ubyte[])chunk.data[0], item);
						break;
					}
					case MessageType.ERROR: {
						cocaine.protocol.Error error = unpacked.as!(cocaine.protocol.Error);
						throw new Exception(error.reason.message);
					}
					case MessageType.CHOKE: {
						cocaine.protocol.Choke choke = unpacked.as!(cocaine.protocol.Choke);
						closed = true;
						break;
					}
				}
			}
		}
	}
}
