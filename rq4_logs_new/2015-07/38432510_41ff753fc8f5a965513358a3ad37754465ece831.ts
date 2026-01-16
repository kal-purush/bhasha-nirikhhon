import {log} from '../db/api';
import userManager from '../users/userManager';
import assign = require('object.assign');
import {ObjectID} from 'mongodb';

class Edison {
    private _id: ObjectID;
    private id: string;
    private isAlive: boolean;
    private socket: SocketIO.Socket;

    /**
    * @param data - Data from DB
    */
    constructor(dbData) {
        if (!(dbData._id instanceof ObjectID))
            throw Error('Edison instance can be created only by data form db');
        assign(this, dbData);
        this.id = dbData._id.toString();
        this.isAlive = false;
    }

    alive(): boolean {
        return this.isAlive;
    }

    getId(): string {
        return this.id;
    }

    getObjectId(): ObjectID {
        return this._id;
    }

    getData(): any {
        var obj: any = {};
        assign(obj, this);
        delete obj.socket;
        return obj
    }

    setSocket(socket: SocketIO.Socket) {
        this.socket = socket;
        this.isAlive = this.socket.connected;

        this.socket.on('log', (data) => {
            log(this, data);
        });

        this.socket.on('disconnect', () => {
            console.log('--- Edison "%s" disconnected', this.id);
            this.isAlive = this.socket.connected;
            userManager.notifyEdisonUpdated(this.id, this.getData());
        });

        this.socket.on('error', () => {
            this.socket.disconnect();
            console.log('--- Socket error on Edison "%s": connected =', this.id, this.socket.connected)
        });

        userManager.notifyEdisonUpdated(this.id, this.getData());
    }
}

export default Edison;