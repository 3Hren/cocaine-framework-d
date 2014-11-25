import std.stdio;

import cocaine.worker;

void main(string[] args) {
    auto config = WorkerConfigurator.parse(args);
    auto worker = new Worker(config);
    worker.on!"ping"((request, response) {
        writeln("%s : %s", request, response);
    });
    worker.run();
}
