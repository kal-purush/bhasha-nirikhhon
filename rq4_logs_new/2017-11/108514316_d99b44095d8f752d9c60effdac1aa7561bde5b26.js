(function (root, factory) {
    if (typeof exports === 'object' && typeof exports.nodeName !== 'string') {
        // CommonJS
        var jasmineRequire = require('jasmine-core');
        module.exports = factory(root, function () {
            return jasmineRequire;
        });
    } else {
        // Browser globals
        window.AsyncCustomMatchers = factory(root, getJasmineRequireObj);
    }
}(typeof window !== 'undefined' ? window : global, function (global, getJasmineRequireObj) {
    getJasmineRequireObj().asyncTestMatcher = function (jRequire) {
        var asyncExcept = {};

        asyncExcept.ExpectationAdaptor = jRequire.AsyncExpectationAdaptor;
        asyncExcept.AsyncWrap = jRequire.wrapCompareAsync;
        asyncExcept.CustomMatchers = jRequire.AsyncMatchers;
        asyncExcept.AsyncCustomMatchers = jRequire.AsyncCustomMatchers(asyncExcept);
        return asyncExcept.AsyncCustomMatchers;
    };
    getJasmineRequireObj().jasmineMatchers = global.jasmine.matchers;
    getJasmineRequireObj().AsyncCustomMatchers = function(exAsAsync) {
        function AsyncCustomMatchers(global) {
            var testTarget,
                defaultExept,
                defaultWrap,
                customExceptions = exAsAsync.CustomMatchers(),
                asyncWrap = exAsAsync.AsyncWrap,
                excpAdapt = exAsAsync.ExpectationAdaptor;
            this.install = function(t_) {
                testTarget = t_;
                global.jasmine.addMatchers(customExceptions);
                defaultExept = global.jasmine.Expectation;
                defaultWrap = global.jasmine.Expectation.prototype.wrapCompare;

                global.jasmine.Expectation = excpAdapt();
                global.jasmine.Expectation.prototype.wrapCompare = asyncWrap;
            };
            this.uninstall = () => {
                this.testTarget = null;
                global.jasmine.Expectation = defaultExept;
                global.jasmine.Expectation.prototype.wrapCompare = defaultWrap;
            };
        }

        return AsyncCustomMatchers;
    };

    getJasmineRequireObj().AsyncExpectationAdaptor = function () {
        function Expectation(options) {
            this.util = options.util || { buildFailureMessage: function () { } };
            this.customEqualityTesters = options.customEqualityTesters || [];
            this.actual = options.actual;
            this.addExpectationResult = options.addExpectationResult || function () { };
            this.isNot = options.isNot;

            var customMatchers = Object.assign(options.customMatchers, getJasmineRequireObj().jasmineMatchers) || {};
            for (let matcherName in customMatchers) {
                this[matcherName] = Expectation.prototype.wrapCompare(matcherName, customMatchers[matcherName], options);
            }
        }
        Expectation.addCoreMatchers = function (matchers) {
            var prototype = Expectation.prototype;
            for (var matcherName in matchers) {
                var matcher = matchers[matcherName];
                prototype[matcherName] = prototype.wrapCompare(matcherName, matcher);
            }
        };

        Expectation.Factory = function (options) {
            options = options || {};
            var expect = new Expectation(options);
            options.isNot = true;
            expect.not = new Expectation(options);
            return expect;
        };

        return Expectation;
    };

    getJasmineRequireObj().wrapCompareAsync = function (name, matcherFactory, options) {
        return async function () {
            var args = Array.prototype.slice.call(arguments, 0),
                expected = args.slice(0),
                message = '',
                result = null;

            args.unshift(this.actual);

            var matcher = matcherFactory(this.util, this.customEqualityTesters),
                matcherCompare = matcher.compare;


            async function defaultNegativeCompare() {
                var result;
                try {
                    if (name.indexOf('Async') !== -1) {
                        result = await matcher.compare.apply(null, args);
                    } else {
                        result = matcher.compare.apply(null, args);
                    }
                } catch (err) {
                    throw new Error(`Test assertion threw unexpected exception: ${err.message}`);
                }
                result.pass = !result.pass;
                return result;
            }

            if (this.isNot) {
                matcherCompare = matcher.negativeCompare || defaultNegativeCompare;
            }

            try {
                if (name.indexOf('Async') !== -1) {
                    result = await matcherCompare.apply(null, args);
                } else {
                    result = matcherCompare.apply(null, args);
                }

            } catch (err) {
                throw new Error(`Test assertion threw unexpected exception: ${err.message}`);
            }

            if (!result.pass) {
                if (!result.message) {
                    args.unshift(this.isNot);
                    args.unshift(name);
                    message = options.util.buildFailureMessage.apply(null, args);
                } else {
                    if (Object.prototype.toString.apply(result.message) === '[object Function]') {
                        message = result.message();
                    } else {
                        message = result.message;
                    }
                }
            }

            if (expected.length == 1) {
                expected = expected[0];
            }
            options.addExpectationResult(
                result.pass,
                {
                    matcherName: name,
                    passed: result.pass,
                    message: message,
                    actual: options.actual,
                    expected: expected
                }
            );
        };
    };

    getJasmineRequireObj().AsyncMatchers = function () {
        return {
            toThrowAnExceptionAsync: (util) => {
                return {
                    compare: async (actual, expected) => {
                        var output = {},
                            threwException = false,
                            result = {};

                        if (!expected || !expected.__proto__) {
                            throw new Error("Expected method is null or undefined");
                        }
                        if (actual.constructor.name !== "Promise" && typeof actual.then != "function") {
                            throw new Error("Passed in method is not a promise");
                        }
                        if (!expected.name) {
                            throw new Error("Passed in object does not contain a name");
                        }

                        try {
                            if (typeof actual.then === "function") {
                                await actual.then(() => { }, (err) => { throw err;});
                            } else {
                                await actual;
                            }
                        } catch (err) {
                            threwException = true;
                            output = err;
                        }

                        result.pass = util.equals(output.constructor, expected);
                        if (result.pass) {
                            result.message = `Expected exception of type: ${expected.name}`;
                        } else if (!result.pass && threwException) {
                            result.message =
                                `Expected exception of type: ${expected.name} but got exception of type ${output.name}`;
                        } else {
                            result.message = "Expected function did not throw exception";
                        }
                        return result;
                    }

                }
            }, toEqualAsync: (util) => {
                return {
                    compare: async function (actual, expected) {
                        var result = {
                            pass: false
                        };

                        result.pass = util.equals(actual, expected);

                        return result;
                    }
                }
            }
        }
    };


    var jRequire = getJasmineRequireObj();
    var AsyncTestMatcher = jRequire.asyncTestMatcher(jRequire);
    window.jasmine.AsyncTestMatcher = new AsyncTestMatcher(global);

    return AsyncTestMatcher;
}));