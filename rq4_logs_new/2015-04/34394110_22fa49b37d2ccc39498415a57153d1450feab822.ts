/**
 * Created by jaboko on 04.04.15.
 */

/// <reference path='../typings/tsd.d.ts' />
/// <reference path='../lib/processor.ts' />

import { ProcessorBuilder } from '../lib/processor';

export function onSet(...list) {

    function processNextDecorator(decorators, target, processor, value) {

        if (decorators.length == 0) return value;

        var d = decorators.shift();
        var args = processor.args.concat();
        args[2] = deepClone(args[2]);
        args[2].set = (v) => {
            return processNextDecorator(decorators, target, processor, v);
        };

        var p = d.apply(target, args);
        var newValue = p.set.apply(target, [value]);

        if (newValue === undefined)
            newValue = value;

        return newValue;
    }

    var p = ProcessorBuilder.create("onSet")
        .setter(function (value) {
            var v = processNextDecorator(list.concat(), this, p, value);
            return v;
        }).build();

    return p.create();
}

export function onGet(...list) {

    function processNextDecorator(decorators, target, processor, value) {

        if (decorators.length == 0) return value;

        var d = decorators.shift();
        var args = processor.args.concat();
        args[2] = deepClone(args[2]);
        args[2].get = (v) => {
            var v = processNextDecorator(decorators, target, processor, v);
            return v;
        };

        var p = d.apply(target, args);
        var newValue = p.get.apply(target, [value]);

        if (newValue === undefined)
            newValue = value;

        return newValue;
    }

    var p = ProcessorBuilder.create("onGet")
        .getter(function (value) {
            var v = processNextDecorator(list.concat(), this, p, value);
            return v;
        }).build();

    return p.create();
}

function deepClone(obj: any) {
    "use strict";

    if (obj == null) {
        return obj;
    } else if (Array.isArray(obj)) {
        return obj.map((obj: any)=> deepClone(obj));
    } else if (obj instanceof RegExp) {
        return obj;
    } else if (typeof obj === "object") {
        var cloned: any = {};
        Object.keys(obj).forEach(key=> cloned[key] = deepClone(obj[key]));
        return cloned;
    } else {
        return obj;
    }
}