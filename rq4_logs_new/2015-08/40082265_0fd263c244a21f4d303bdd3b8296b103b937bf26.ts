/// <reference path="../typings/tsd.d.ts" />
import Rx = require("rx");
import rabbit = require("rabbit.js");
declare module rabbitRx {
    enum SocketType {
        PUB = 0,
        SUB = 1,
    }
    interface IOpts {
        uri: string;
        socketType: SocketType;
        queue: string;
    }
    class RabbitBase {
        private opts;
        constructor(opts: IOpts);
        private static connectSocket(queue, socket);
        connectContext(): Rx.Observable<rabbit.Socket>;
    }
    class RabbitSub extends RabbitBase {
        constructor(opts: IOpts);
        private connect();
        read(): Rx.Observable<any>;
    }
    class RabbitPub extends RabbitBase {
        private socketStream;
        constructor(opts: IOpts);
        private connectOnce();
        write(data: any): Rx.Observable<any>;
    }
}
export = rabbitRx;