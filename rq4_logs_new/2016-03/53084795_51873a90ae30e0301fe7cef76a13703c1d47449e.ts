'use strict';

// stack trace is working with ts files fine with source-map-support npm package used
import 'source-map-support/register';

// relative path thanks to symlink(for runtime) and moduleReferencing: classic (for tsc/IDE)
import foo from 'lib/foo/foo';

foo();

// no cycle reference even when using ambient declarations placed inside linked dir
class World implements ICanHello {
    constructor () {
        console.log('constructor');
    }
    public hello () {
        console.log('hello u');
    }
}

// debugging is done through ts code correctly
console.log('before new World');
let world = new World();
console.log('after new World');
throw new Error('error a - stack trace is working with ts files fine with source-map-support');
world.hello();
console.log('after hello');