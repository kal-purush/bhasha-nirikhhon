import axios, { AxiosResponse } from 'axios';
import { Eventing } from './Eventing';
import { Attributes } from './Attributes';
import { Sync } from './Sync';

interface HasId {
    id?: string | number;
}

export class Model<T extends HasId> {
    protected attributes: Attributes<T> = new Attributes<T>(this.data);
    protected events: Eventing<T> = new Eventing<T>(this.data);
    protected sync: Sync<T> = new Sync<T>(this.url, this.data);

    constructor(protected data: T, protected url: string) {
        console.log('Model constructor');
    }

    get = this.attributes.get;
    // get = <K extends keyof T>(prop: K): T[K] => {
    //     return this.attributes.get(prop);
    // };

    set = (update: Partial<T>) => {
        return this.attributes.set(update);
    };

    on = (eventName, callback) => {
        this.events.on(eventName, callback);
    };

    trigger = eventName => {
        this.events.trigger(eventName);
    };

    fetch = async () => {
        return await this.sync.fetch().then((response: AxiosResponse<T>) => {
            this.data = response.data;
            return this.data;
        });
    };

    save = async () => {
        return await this.sync.save().then((response: AxiosResponse<T>) => {
            console.log(`save ${this.data.id} ${response.status}`);
        });
    };
}