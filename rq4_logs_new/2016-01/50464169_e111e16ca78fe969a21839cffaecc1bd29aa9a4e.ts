import {global} from 'angular2/src/facade/lang';
import {ListWrapper} from 'angular2/src/facade/collection';

import {
    FunctionWithParamTokens,
    inject,
    injectAsync,
    TestInjector,
    getTestInjector
} from 'angular2/testing';

export {inject, injectAsync} from 'angular2/testing';

var _global: any = <any>(typeof window === 'undefined' ? global : window);

export var afterEach: Function = _global.afterEach;
export var describe: Function = _global.describe;
export var xdescribe: Function = _global.xdescribe;

export type SyncTestFn = () => void;
export type AsyncTestFn = (done: () => void) => void;
export type AnyTestFn = SyncTestFn | AsyncTestFn;

var jsmBeforeEach = _global.beforeEach;
var jsmIt = _global.it;
var jsmXIt = _global.xit;

var testInjector: TestInjector = getTestInjector();
jsmBeforeEach((done) => { testInjector.reset(); done(); });

export function beforeEachProviders(fn): void {
    jsmBeforeEach((done) => {
        var providers = fn();
        if (!providers) return;
        try {
            testInjector.addProviders(providers);
            done();
        } catch (e) {
            throw new Error('beforeEachProviders was called after the injector had ' +
                'been used in a beforeEach or it block. This invalidates the ' +
                'test injector');
        }
    });
}

function _isPromiseLike(input): boolean {
    return input && !!(input.then);
}


function runInTestZone(fnToExecute, finishCallback, failCallback): any {
    var pendingMicrotasks = 0;
    var pendingTimeouts = [];

    var ngTestZone = (<Zone>global.zone)
        .fork({
            onError: function(e) { failCallback(e); },
            '$run': function(parentRun) {
                return function() {
                    try {
                        return parentRun.apply(this, arguments);
                    } finally {
                        if (pendingMicrotasks === 0 && pendingTimeouts.length === 0) {
                            finishCallback();
                        }
                    }
                };
            },
            '$scheduleMicrotask': function(parentScheduleMicrotask) {
                return function(fn) {
                    pendingMicrotasks++;
                    var microtask = function() {
                        try {
                            fn();
                        } finally {
                            pendingMicrotasks--;
                        }
                    };
                    parentScheduleMicrotask.call(this, microtask);
                };
            },
            '$setTimeout': function(parentSetTimeout) {
                return function(fn: Function, delay: number, ...args) {
                    var id;
                    var cb = function() {
                        fn();
                        ListWrapper.remove(pendingTimeouts, id);
                    };
                    id = parentSetTimeout(cb, delay, args);
                    pendingTimeouts.push(id);
                    return id;
                };
            },
            '$clearTimeout': function(parentClearTimeout) {
                return function(id: number) {
                    parentClearTimeout(id);
                    ListWrapper.remove(pendingTimeouts, id);
                };
            },
        });

    return ngTestZone.run(fnToExecute);
}

function _it(jsmFn: Function, name: string, testFn: FunctionWithParamTokens | AnyTestFn,
             testTimeOut: number): void {
    var timeOut = testTimeOut;

    if (testFn instanceof FunctionWithParamTokens) {
        jsmFn(name, (done) => {
            var finishCallback = () => {
                // Wait one more event loop to make sure we catch unreturned promises and
                // promise rejections.
                setTimeout(done, 0);
            };
            var returnedTestValue =
                runInTestZone(() => testInjector.execute(testFn), finishCallback, done.fail);

            if (testFn.isAsync) {
                if (_isPromiseLike(returnedTestValue)) {
                    (<Promise<any>>returnedTestValue).then(null, (err) => { done.fail(err); });
                } else {
                    done.fail('Error: injectAsync was expected to return a promise, but the ' +
                        ' returned value was: ' + returnedTestValue);
                }
            } else {
                if (!(returnedTestValue === undefined)) {
                    done.fail('Error: inject returned a value. Did you mean to use injectAsync? Returned ' +
                        'value was: ' + returnedTestValue);
                }
            }
        }, timeOut);
    } else {
        // The test case doesn't use inject(). ie `it('test', (done) => { ... }));`
        jsmFn(name, testFn, timeOut);
    }
}

export function beforeEach(fn: FunctionWithParamTokens | AnyTestFn): void {
    if (fn instanceof FunctionWithParamTokens) {
        // The test case uses inject(). ie `beforeEach(inject([ClassA], (a) => { ...
        // }));`
        jsmBeforeEach((done) => {
            var finishCallback = () => {
                // Wait one more event loop to make sure we catch unreturned promises and
                // promise rejections.
                setTimeout(done, 0);
            };

            var returnedTestValue =
                runInTestZone(() => testInjector.execute(fn), finishCallback, done.fail);
            if (fn.isAsync) {
                if (_isPromiseLike(returnedTestValue)) {
                    (<Promise<any>>returnedTestValue).then(null, (err) => { done.fail(err); });
                } else {
                    done.fail('Error: injectAsync was expected to return a promise, but the ' +
                        ' returned value was: ' + returnedTestValue);
                }
            } else {
                if (!(returnedTestValue === undefined)) {
                    done.fail('Error: inject returned a value. Did you mean to use injectAsync? Returned ' +
                        'value was: ' + returnedTestValue);
                }
            }
        });
    } else {
        // The test case doesn't use inject(). ie `beforeEach((done) => { ... }));`
        if ((<any>fn).length === 0) {
            jsmBeforeEach(() => { (<SyncTestFn>fn)(); });
        } else {
            jsmBeforeEach((done) => { (<AsyncTestFn>fn)(done); });
        }
    }
}

export function it(name: string, fn: FunctionWithParamTokens | AnyTestFn,
                   timeOut: number = null): void {
    return _it(jsmIt, name, fn, timeOut);
}

export function xit(name: string, fn: FunctionWithParamTokens | AnyTestFn,
                    timeOut: number = null): void {
    return _it(jsmXIt, name, fn, timeOut);
}