import * as Bacon from 'baconjs'

interface BusDictionary {
    [index: string]: Bacon.Bus<any, any>;
}

export default class Dispatcher {
    busCache: BusDictionary;

    constructor() { 
        this.busCache = {} as BusDictionary;
     }

    private bus(name: string): Bacon.Bus<any, any> {
        return this.busCache[name] = this.busCache[name] || new Bacon.Bus();
    }

    stream(name: string) {
        return this.bus(name);
    }

    push(name: string, value: any) {
        this.bus(name).push(value);
    }

    plug(name: string, value: Bacon.EventStream<any, any>) {
        this.bus(name).plug(value);
    }
}