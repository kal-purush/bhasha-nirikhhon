/// <reference path="../typings/tsd.d.ts"/>

export class MyAppCtrl {

}

export class GreetingCtrl {
    public name: string;

    get upperName() {
        return this.name.toUpperCase();
    }
}

export class CounterCtrl {
    public count: number;

    constructor() {
        this.count = 0;
    }

    public inc() {
        console.log(this.count);
        this.count++;
    }

    public dec() {
        this.count--;
    }

}