/// <reference path="chance.d.ts" />

// Instantiation
var globalInstance: Chance.Chance = chance;
var createYourOwn = new Chance();

// Basic usage
var randBool: boolean = chance.bool();

var birthday: Date = chance.birthday();
var birthdayStr: Date|string = chance.birthday({ string: true });

var strArr: string[] = chance.n(chance.string, 42);

var uniqInts: number[] = chance.unique(chance.integer, 99);

// Mixins can be used with on-the-fly type declaration
declare module Chance {
    interface Chance {
        time(): string;
    }
}

chance.mixin({
    time: function () {
        var h = chance.hour({ twentyfour: true }),
            m = chance.minute();
        return `${h}:${m}`;
    }
});

var timeString: string = chance.time();