module cocaine.worker;

import std.array;
import std.c.stdlib;
import std.format;
import std.getopt;
import std.stdio;

import vibe.vibe;

import cocaine.logging;

struct WorkerConfiguration {
    string name;
    string uuid;
    string endpoint;
}

struct WorkerConfigurator {
    static WorkerConfiguration parse(string[] args) {
        auto config = WorkerConfiguration();
        std.getopt.getopt(
            args,
            "name", &config.name,
            "uuid", &config.uuid,
            "endpoint", &config.endpoint,
            "help", &helpHandler
        );

        if (config.name.empty || config.uuid.empty || config.endpoint.empty) {
            throw new Exception("invalid configuration - some of required fields are empty");
        }

        return config;
    }

    private static void helpHandler() {
        import core.runtime;
        writefln("Usage: %s --name NAME --uuid UUID --endpoint ENDPOINT", Runtime.args[0]);
        writefln("");
        writefln("Options:");
        writefln("    %-10s - %s", "--name", "application name");
        writefln("    %-10s - %s", "--uuid", "worker universally unique identifier (UUID)");
        writefln("    %-10s - %s", "--endpoint", "path to the unix socket, which is listening by cocaine-runtime");
        writefln("    %-10s - %s", "--help", "show this message");
        exit(0);
    }
}

struct Request {}
struct Response {}

auto log = cocaine.logging.getLogger("worker");

class Worker {
    alias Handler = void delegate(Request, Response);

    private const WorkerConfiguration config;
    private Handler[string] handlers;

    public this(in WorkerConfiguration config) {
        this.config = config;
    }

    public void on(string event)(Handler handler) {
        handlers[event] = handler;
    }

    public void run() {
        log.log!(Level.DEBUG)("starting event loop");
        runEventLoop();
    }
}
