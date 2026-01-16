import SubSocket from './subsocket';

export class SocketServer {
    public of(_nsp: string | RegExp): any {
        return new SubSocket();
    }

    public close(fn?: () => void): void {
        fn();
    }

    public on(_event: 'connection', _listener: (socket: any) => void): any {
        return;
    }

    public emit(_event: string, ..._args: any[]): any {
        return jest.fn();
    }

    public to() {
        return this;
    }
}

export default function() {
    return new SocketServer();
}