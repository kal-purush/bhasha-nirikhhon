(window.webpackJsonp = window.webpackJsonp || []).push([[1], {
    0: function(t, e, n) {
        t.exports = n("zUnb")
    },
    zUnb: function(t, e, n) {
        "use strict";
        function r(t) {
            return (r = Object.setPrototypeOf ? Object.getPrototypeOf : function(t) {
                return t.__proto__ || Object.getPrototypeOf(t)
            }
            )(t)
        }
        function i(t, e, n) {
            return (i = "undefined" != typeof Reflect && Reflect.get ? Reflect.get : function(t, e, n) {
                var i = function(t, e) {
                    for (; !Object.prototype.hasOwnProperty.call(t, e) && null !== (t = r(t)); )
                        ;
                    return t
                }(t, e);
                if (i) {
                    var o = Object.getOwnPropertyDescriptor(i, e);
                    return o.get ? o.get.call(n) : o.value
                }
            }
            )(t, e, n || t)
        }
        function o(t) {
            if (void 0 === t)
                throw new ReferenceError("this hasn't been initialised - super() hasn't been called");
            return t
        }
        function a(t, e) {
            (null == e || e > t.length) && (e = t.length);
            for (var n = 0, r = new Array(e); n < e; n++)
                r[n] = t[n];
            return r
        }
        function u(t, e) {
            if (t) {
                if ("string" == typeof t)
                    return a(t, e);
                var n = Object.prototype.toString.call(t).slice(8, -1);
                return "Object" === n && t.constructor && (n = t.constructor.name),
                "Map" === n || "Set" === n ? Array.from(t) : "Arguments" === n || /^(?:Ui|I)nt(?:8|16|32)(?:Clamped)?Array$/.test(n) ? a(t, e) : void 0
            }
        }
        function s(t, e) {
            return function(t) {
                if (Array.isArray(t))
                    return t
            }(t) || function(t, e) {
                if ("undefined" != typeof Symbol && Symbol.iterator in Object(t)) {
                    var n = []
                      , r = !0
                      , i = !1
                      , o = void 0;
                    try {
                        for (var a, u = t[Symbol.iterator](); !(r = (a = u.next()).done) && (n.push(a.value),
                        !e || n.length !== e); r = !0)
                            ;
                    } catch (s) {
                        i = !0,
                        o = s
                    } finally {
                        try {
                            r || null == u.return || u.return()
                        } finally {
                            if (i)
                                throw o
                        }
                    }
                    return n
                }
            }(t, e) || u(t, e) || function() {
                throw new TypeError("Invalid attempt to destructure non-iterable instance.\nIn order to be iterable, non-array objects must have a [Symbol.iterator]() method.")
            }()
        }
        function l(t) {
            return function(t) {
                if (Array.isArray(t))
                    return a(t)
            }(t) || function(t) {
                if ("undefined" != typeof Symbol && Symbol.iterator in Object(t))
                    return Array.from(t)
            }(t) || u(t) || function() {
                throw new TypeError("Invalid attempt to spread non-iterable instance.\nIn order to be iterable, non-array objects must have a [Symbol.iterator]() method.")
            }()
        }
        function c(t, e, n) {
            return e in t ? Object.defineProperty(t, e, {
                value: n,
                enumerable: !0,
                configurable: !0,
                writable: !0
            }) : t[e] = n,
            t
        }
        function h(t, e) {
            var n;
            if ("undefined" == typeof Symbol || null == t[Symbol.iterator]) {
                if (Array.isArray(t) || (n = u(t)) || e && t && "number" == typeof t.length) {
                    n && (t = n);
                    var r = 0
                      , i = function() {};
                    return {
                        s: i,
                        n: function() {
                            return r >= t.length ? {
                                done: !0
                            } : {
                                done: !1,
                                value: t[r++]
                            }
                        },
                        e: function(t) {
                            throw t
                        },
                        f: i
                    }
                }
                throw new TypeError("Invalid attempt to iterate non-iterable instance.\nIn order to be iterable, non-array objects must have a [Symbol.iterator]() method.")
            }
            var o, a = !0, s = !1;
            return {
                s: function() {
                    n = t[Symbol.iterator]()
                },
                n: function() {
                    var t = n.next();
                    return a = t.done,
                    t
                },
                e: function(t) {
                    s = !0,
                    o = t
                },
                f: function() {
                    try {
                        a || null == n.return || n.return()
                    } finally {
                        if (s)
                            throw o
                    }
                }
            }
        }
        function f(t, e) {
            return (f = Object.setPrototypeOf || function(t, e) {
                return t.__proto__ = e,
                t
            }
            )(t, e)
        }
        function d(t, e) {
            if ("function" != typeof e && null !== e)
                throw new TypeError("Super expression must either be null or a function");
            t.prototype = Object.create(e && e.prototype, {
                constructor: {
                    value: t,
                    writable: !0,
                    configurable: !0
                }
            }),
            e && f(t, e)
        }
        function v() {
            if ("undefined" == typeof Reflect || !Reflect.construct)
                return !1;
            if (Reflect.construct.sham)
                return !1;
            if ("function" == typeof Proxy)
                return !0;
            try {
                return Date.prototype.toString.call(Reflect.construct(Date, [], (function() {}
                ))),
                !0
            } catch (t) {
                return !1
            }
        }
        function p(t) {
            return (p = "function" == typeof Symbol && "symbol" == typeof Symbol.iterator ? function(t) {
                return typeof t
            }
            : function(t) {
                return t && "function" == typeof Symbol && t.constructor === Symbol && t !== Symbol.prototype ? "symbol" : typeof t
            }
            )(t)
        }
        function g(t, e) {
            return !e || "object" !== p(e) && "function" != typeof e ? o(t) : e
        }
        function y(t) {
            var e = v();
            return function() {
                var n, i = r(t);
                if (e) {
                    var o = r(this).constructor;
                    n = Reflect.construct(i, arguments, o)
                } else
                    n = i.apply(this, arguments);
                return g(this, n)
            }
        }
        function m(t, e) {
            if (!(t instanceof e))
                throw new TypeError("Cannot call a class as a function")
        }
        function w(t, e) {
            for (var n = 0; n < e.length; n++) {
                var r = e[n];
                r.enumerable = r.enumerable || !1,
                r.configurable = !0,
                "value"in r && (r.writable = !0),
                Object.defineProperty(t, r.key, r)
            }
        }
        function _(t, e, n) {
            return e && w(t.prototype, e),
            n && w(t, n),
            t
        }
        function k(t, e, n) {
            return (k = v() ? Reflect.construct : function(t, e, n) {
                var r = [null];
                r.push.apply(r, e);
                var i = new (Function.bind.apply(t, r));
                return n && f(i, n.prototype),
                i
            }
            ).apply(null, arguments)
        }
        n.r(e);
        var b = function() {
            return Array.isArray || function(t) {
                return t && "number" == typeof t.length
            }
        }();
        function C(t) {
            return null !== t && "object" == typeof t
        }
        function S(t) {
            return "function" == typeof t
        }
        var x = function() {
            function t(t) {
                return Error.call(this),
                this.message = t ? "".concat(t.length, " errors occurred during unsubscription:\n").concat(t.map((function(t, e) {
                    return "".concat(e + 1, ") ").concat(t.toString())
                }
                )).join("\n  ")) : "",
                this.name = "UnsubscriptionError",
                this.errors = t,
                this
            }
            return t.prototype = Object.create(Error.prototype),
            t
        }()
          , E = function() {
            var t = function() {
                function t(e) {
                    m(this, t),
                    this.closed = !1,
                    this._parentOrParents = null,
                    this._subscriptions = null,
                    e && (this._ctorUnsubscribe = !0,
                    this._unsubscribe = e)
                }
                return _(t, [{
                    key: "unsubscribe",
                    value: function() {
                        var e;
                        if (!this.closed) {
                            var n = this._parentOrParents
                              , r = this._ctorUnsubscribe
                              , i = this._unsubscribe
                              , o = this._subscriptions;
                            if (this.closed = !0,
                            this._parentOrParents = null,
                            this._subscriptions = null,
                            n instanceof t)
                                n.remove(this);
                            else if (null !== n)
                                for (var a = 0; a < n.length; ++a)
                                    n[a].remove(this);
                            if (S(i)) {
                                r && (this._unsubscribe = void 0);
                                try {
                                    i.call(this)
                                } catch (c) {
                                    e = c instanceof x ? T(c.errors) : [c]
                                }
                            }
                            if (b(o))
                                for (var u = -1, s = o.length; ++u < s; ) {
                                    var l = o[u];
                                    if (C(l))
                                        try {
                                            l.unsubscribe()
                                        } catch (c) {
                                            e = e || [],
                                            c instanceof x ? e = e.concat(T(c.errors)) : e.push(c)
                                        }
                                }
                            if (e)
                                throw new x(e)
                        }
                    }
                }, {
                    key: "add",
                    value: function(e) {
                        var n = e;
                        if (!e)
                            return t.EMPTY;
                        switch (typeof e) {
                        case "function":
                            n = new t(e);
                        case "object":
                            if (n === this || n.closed || "function" != typeof n.unsubscribe)
                                return n;
                            if (this.closed)
                                return n.unsubscribe(),
                                n;
                            if (!(n instanceof t)) {
                                var r = n;
                                (n = new t)._subscriptions = [r]
                            }
                            break;
                        default:
                            throw new Error("unrecognized teardown " + e + " added to Subscription.")
                        }
                        var i = n._parentOrParents;
                        if (null === i)
                            n._parentOrParents = this;
                        else if (i instanceof t) {
                            if (i === this)
                                return n;
                            n._parentOrParents = [i, this]
                        } else {
                            if (-1 !== i.indexOf(this))
                                return n;
                            i.push(this)
                        }
                        var o = this._subscriptions;
                        return null === o ? this._subscriptions = [n] : o.push(n),
                        n
                    }
                }, {
                    key: "remove",
                    value: function(t) {
                        var e = this._subscriptions;
                        if (e) {
                            var n = e.indexOf(t);
                            -1 !== n && e.splice(n, 1)
                        }
                    }
                }]),
                t
            }();
            return t.EMPTY = function(t) {
                return t.closed = !0,
                t
            }(new t),
            t
        }();
        function T(t) {
            return t.reduce((function(t, e) {
                return t.concat(e instanceof x ? e.errors : e)
            }
            ), [])
        }
        var O = !1
          , A = {
            Promise: void 0,
            set useDeprecatedSynchronousErrorHandling(t) {
                if (t) {
                    var e = new Error;
                    console.warn("DEPRECATED! RxJS was set to use deprecated synchronous error handling behavior by code at: \n" + e.stack)
                } else
                    O && console.log("RxJS: Back to a better error behavior. Thank you. <3");
                O = t
            },
            get useDeprecatedSynchronousErrorHandling() {
                return O
            }
        };
        function R(t) {
            setTimeout((function() {
                throw t
            }
            ), 0)
        }
        var P = {
            closed: !0,
            next: function(t) {},
            error: function(t) {
                if (A.useDeprecatedSynchronousErrorHandling)
                    throw t;
                R(t)
            },
            complete: function() {}
        }
          , I = function() {
            return "function" == typeof Symbol ? Symbol("rxSubscriber") : "@@rxSubscriber_" + Math.random()
        }()
          , j = function(t) {
            d(n, t);
            var e = y(n);
            function n(t, r, i) {
                var a;
                switch (m(this, n),
                (a = e.call(this)).syncErrorValue = null,
                a.syncErrorThrown = !1,
                a.syncErrorThrowable = !1,
                a.isStopped = !1,
                arguments.length) {
                case 0:
                    a.destination = P;
                    break;
                case 1:
                    if (!t) {
                        a.destination = P;
                        break
                    }
                    if ("object" == typeof t) {
                        t instanceof n ? (a.syncErrorThrowable = t.syncErrorThrowable,
                        a.destination = t,
                        t.add(o(a))) : (a.syncErrorThrowable = !0,
                        a.destination = new N(o(a),t));
                        break
                    }
                default:
                    a.syncErrorThrowable = !0,
                    a.destination = new N(o(a),t,r,i)
                }
                return a
            }
            return _(n, [{
                key: I,
                value: function() {
                    return this
                }
            }, {
                key: "next",
                value: function(t) {
                    this.isStopped || this._next(t)
                }
            }, {
                key: "error",
                value: function(t) {
                    this.isStopped || (this.isStopped = !0,
                    this._error(t))
                }
            }, {
                key: "complete",
                value: function() {
                    this.isStopped || (this.isStopped = !0,
                    this._complete())
                }
            }, {
                key: "unsubscribe",
                value: function() {
                    this.closed || (this.isStopped = !0,
                    i(r(n.prototype), "unsubscribe", this).call(this))
                }
            }, {
                key: "_next",
                value: function(t) {
                    this.destination.next(t)
                }
            }, {
                key: "_error",
                value: function(t) {
                    this.destination.error(t),
                    this.unsubscribe()
                }
            }, {
                key: "_complete",
                value: function() {
                    this.destination.complete(),
                    this.unsubscribe()
                }
            }, {
                key: "_unsubscribeAndRecycle",
                value: function() {
                    var t = this._parentOrParents;
                    return this._parentOrParents = null,
                    this.unsubscribe(),
                    this.closed = !1,
                    this.isStopped = !1,
                    this._parentOrParents = t,
                    this
                }
            }], [{
                key: "create",
                value: function(t, e, r) {
                    var i = new n(t,e,r);
                    return i.syncErrorThrowable = !1,
                    i
                }
            }]),
            n
        }(E)
          , N = function(t) {
            d(n, t);
            var e = y(n);
            function n(t, r, i, a) {
                var u, s;
                m(this, n),
                (u = e.call(this))._parentSubscriber = t;
                var l = o(u);
                return S(r) ? s = r : r && (s = r.next,
                i = r.error,
                a = r.complete,
                r !== P && (S((l = Object.create(r)).unsubscribe) && u.add(l.unsubscribe.bind(l)),
                l.unsubscribe = u.unsubscribe.bind(o(u)))),
                u._context = l,
                u._next = s,
                u._error = i,
                u._complete = a,
                u
            }
            return _(n, [{
                key: "next",
                value: function(t) {
                    if (!this.isStopped && this._next) {
                        var e = this._parentSubscriber;
                        A.useDeprecatedSynchronousErrorHandling && e.syncErrorThrowable ? this.__tryOrSetError(e, this._next, t) && this.unsubscribe() : this.__tryOrUnsub(this._next, t)
                    }
                }
            }, {
                key: "error",
                value: function(t) {
                    if (!this.isStopped) {
                        var e = this._parentSubscriber
                          , n = A.useDeprecatedSynchronousErrorHandling;
                        if (this._error)
                            n && e.syncErrorThrowable ? (this.__tryOrSetError(e, this._error, t),
                            this.unsubscribe()) : (this.__tryOrUnsub(this._error, t),
                            this.unsubscribe());
                        else if (e.syncErrorThrowable)
                            n ? (e.syncErrorValue = t,
                            e.syncErrorThrown = !0) : R(t),
                            this.unsubscribe();
                        else {
                            if (this.unsubscribe(),
                            n)
                                throw t;
                            R(t)
                        }
                    }
                }
            }, {
                key: "complete",
                value: function() {
                    var t = this;
                    if (!this.isStopped) {
                        var e = this._parentSubscriber;
                        if (this._complete) {
                            var n = function() {
                                return t._complete.call(t._context)
                            };
                            A.useDeprecatedSynchronousErrorHandling && e.syncErrorThrowable ? (this.__tryOrSetError(e, n),
                            this.unsubscribe()) : (this.__tryOrUnsub(n),
                            this.unsubscribe())
                        } else
                            this.unsubscribe()
                    }
                }
            }, {
                key: "__tryOrUnsub",
                value: function(t, e) {
                    try {
                        t.call(this._context, e)
                    } catch (n) {
                        if (this.unsubscribe(),
                        A.useDeprecatedSynchronousErrorHandling)
                            throw n;
                        R(n)
                    }
                }
            }, {
                key: "__tryOrSetError",
                value: function(t, e, n) {
                    if (!A.useDeprecatedSynchronousErrorHandling)
                        throw new Error("bad call");
                    try {
                        e.call(this._context, n)
                    } catch (r) {
                        return A.useDeprecatedSynchronousErrorHandling ? (t.syncErrorValue = r,
                        t.syncErrorThrown = !0,
                        !0) : (R(r),
                        !0)
                    }
                    return !1
                }
            }, {
                key: "_unsubscribe",
                value: function() {
                    var t = this._parentSubscriber;
                    this._context = null,
                    this._parentSubscriber = null,
                    t.unsubscribe()
                }
            }]),
            n
        }(j)
          , M = function() {
            return "function" == typeof Symbol && Symbol.observable || "@@observable"
        }();
        function U(t) {
            return t
        }
        function D(t) {
            return 0 === t.length ? U : 1 === t.length ? t[0] : function(e) {
                return t.reduce((function(t, e) {
                    return e(t)
                }
                ), e)
            }
        }
        var H = function() {
            var t = function() {
                function t(e) {
                    m(this, t),
                    this._isScalar = !1,
                    e && (this._subscribe = e)
                }
                return _(t, [{
                    key: "lift",
                    value: function(e) {
                        var n = new t;
                        return n.source = this,
                        n.operator = e,
                        n
                    }
                }, {
                    key: "subscribe",
                    value: function(t, e, n) {
                        var r = this.operator
                          , i = function(t, e, n) {
                            if (t) {
                                if (t instanceof j)
                                    return t;
                                if (t[I])
                                    return t[I]()
                            }
                            return t || e || n ? new j(t,e,n) : new j(P)
                        }(t, e, n);
                        if (i.add(r ? r.call(i, this.source) : this.source || A.useDeprecatedSynchronousErrorHandling && !i.syncErrorThrowable ? this._subscribe(i) : this._trySubscribe(i)),
                        A.useDeprecatedSynchronousErrorHandling && i.syncErrorThrowable && (i.syncErrorThrowable = !1,
                        i.syncErrorThrown))
                            throw i.syncErrorValue;
                        return i
                    }
                }, {
                    key: "_trySubscribe",
                    value: function(t) {
                        try {
                            return this._subscribe(t)
                        } catch (e) {
                            A.useDeprecatedSynchronousErrorHandling && (t.syncErrorThrown = !0,
                            t.syncErrorValue = e),
                            function(t) {
                                for (; t; ) {
                                    var e = t.destination;
                                    if (t.closed || t.isStopped)
                                        return !1;
                                    t = e && e instanceof j ? e : null
                                }
                                return !0
                            }(t) ? t.error(e) : console.warn(e)
                        }
                    }
                }, {
                    key: "forEach",
                    value: function(t, e) {
                        var n = this;
                        return new (e = L(e))((function(e, r) {
                            var i;
                            i = n.subscribe((function(e) {
                                try {
                                    t(e)
                                } catch (n) {
                                    r(n),
                                    i && i.unsubscribe()
                                }
                            }
                            ), r, e)
                        }
                        ))
                    }
                }, {
                    key: "_subscribe",
                    value: function(t) {
                        var e = this.source;
                        return e && e.subscribe(t)
                    }
                }, {
                    key: M,
                    value: function() {
                        return this
                    }
                }, {
                    key: "pipe",
                    value: function() {
                        for (var t = arguments.length, e = new Array(t), n = 0; n < t; n++)
                            e[n] = arguments[n];
                        return 0 === e.length ? this : D(e)(this)
                    }
                }, {
                    key: "toPromise",
                    value: function(t) {
                        var e = this;
                        return new (t = L(t))((function(t, n) {
                            var r;
                            e.subscribe((function(t) {
                                return r = t
                            }
                            ), (function(t) {
                                return n(t)
                            }
                            ), (function() {
                                return t(r)
                            }
                            ))
                        }
                        ))
                    }
                }]),
                t
            }();
            return t.create = function(e) {
                return new t(e)
            }
            ,
            t
        }();
        function L(t) {
            if (t || (t = A.Promise || Promise),
            !t)
                throw new Error("no Promise impl found");
            return t
        }
        var F = function() {
            function t() {
                return Error.call(this),
                this.message = "object unsubscribed",
                this.name = "ObjectUnsubscribedError",
                this
            }
            return t.prototype = Object.create(Error.prototype),
            t
        }()
          , V = function(t) {
            d(n, t);
            var e = y(n);
            function n(t, r) {
                var i;
                return m(this, n),
                (i = e.call(this)).subject = t,
                i.subscriber = r,
                i.closed = !1,
                i
            }
            return _(n, [{
                key: "unsubscribe",
                value: function() {
                    if (!this.closed) {
                        this.closed = !0;
                        var t = this.subject
                          , e = t.observers;
                        if (this.subject = null,
                        e && 0 !== e.length && !t.isStopped && !t.closed) {
                            var n = e.indexOf(this.subscriber);
                            -1 !== n && e.splice(n, 1)
                        }
                    }
                }
            }]),
            n
        }(E)
          , z = function(t) {
            d(n, t);
            var e = y(n);
            function n(t) {
                var r;
                return m(this, n),
                (r = e.call(this, t)).destination = t,
                r
            }
            return n
        }(j)
          , q = function() {
            var t = function(t) {
                d(n, t);
                var e = y(n);
                function n() {
                    var t;
                    return m(this, n),
                    (t = e.call(this)).observers = [],
                    t.closed = !1,
                    t.isStopped = !1,
                    t.hasError = !1,
                    t.thrownError = null,
                    t
                }
                return _(n, [{
                    key: I,
                    value: function() {
                        return new z(this)
                    }
                }, {
                    key: "lift",
                    value: function(t) {
                        var e = new B(this,this);
                        return e.operator = t,
                        e
                    }
                }, {
                    key: "next",
                    value: function(t) {
                        if (this.closed)
                            throw new F;
                        if (!this.isStopped)
                            for (var e = this.observers, n = e.length, r = e.slice(), i = 0; i < n; i++)
                                r[i].next(t)
                    }
                }, {
                    key: "error",
                    value: function(t) {
                        if (this.closed)
                            throw new F;
                        this.hasError = !0,
                        this.thrownError = t,
                        this.isStopped = !0;
                        for (var e = this.observers, n = e.length, r = e.slice(), i = 0; i < n; i++)
                            r[i].error(t);
                        this.observers.length = 0
                    }
                }, {
                    key: "complete",
                    value: function() {
                        if (this.closed)
                            throw new F;
                        this.isStopped = !0;
                        for (var t = this.observers, e = t.length, n = t.slice(), r = 0; r < e; r++)
                            n[r].complete();
                        this.observers.length = 0
                    }
                }, {
                    key: "unsubscribe",
                    value: function() {
                        this.isStopped = !0,
                        this.closed = !0,
                        this.observers = null
                    }
                }, {
                    key: "_trySubscribe",
                    value: function(t) {
                        if (this.closed)
                            throw new F;
                        return i(r(n.prototype), "_trySubscribe", this).call(this, t)
                    }
                }, {
                    key: "_subscribe",
                    value: function(t) {
                        if (this.closed)
                            throw new F;
                        return this.hasError ? (t.error(this.thrownError),
                        E.EMPTY) : this.isStopped ? (t.complete(),
                        E.EMPTY) : (this.observers.push(t),
                        new V(this,t))
                    }
                }, {
                    key: "asObservable",
                    value: function() {
                        var t = new H;
                        return t.source = this,
                        t
                    }
                }]),
                n
            }(H);
            return t.create = function(t, e) {
                return new B(t,e)
            }
            ,
            t
        }()
          , B = function(t) {
            d(n, t);
            var e = y(n);
            function n(t, r) {
                var i;
                return m(this, n),
                (i = e.call(this)).destination = t,
                i.source = r,
                i
            }
            return _(n, [{
                key: "next",
                value: function(t) {
                    var e = this.destination;
                    e && e.next && e.next(t)
                }
            }, {
                key: "error",
                value: function(t) {
                    var e = this.destination;
                    e && e.error && this.destination.error(t)
                }
            }, {
                key: "complete",
                value: function() {
                    var t = this.destination;
                    t && t.complete && this.destination.complete()
                }
            }, {
                key: "_subscribe",
                value: function(t) {
                    return this.source ? this.source.subscribe(t) : E.EMPTY
                }
            }]),
            n
        }(q);
        function Z(t) {
            return t && "function" == typeof t.schedule
        }
        function W(t, e) {
            return function(n) {
                if ("function" != typeof t)
                    throw new TypeError("argument is not a function. Are you looking for `mapTo()`?");
                return n.lift(new G(t,e))
            }
        }
        var G = function() {
            function t(e, n) {
                m(this, t),
                this.project = e,
                this.thisArg = n
            }
            return _(t, [{
                key: "call",
                value: function(t, e) {
                    return e.subscribe(new Q(t,this.project,this.thisArg))
                }
            }]),
            t
        }()
          , Q = function(t) {
            d(n, t);
            var e = y(n);
            function n(t, r, i) {
                var a;
                return m(this, n),
                (a = e.call(this, t)).project = r,
                a.count = 0,
                a.thisArg = i || o(a),
                a
            }
            return _(n, [{
                key: "_next",
                value: function(t) {
                    var e;
                    try {
                        e = this.project.call(this.thisArg, t, this.count++)
                    } catch (n) {
                        return void this.destination.error(n)
                    }
                    this.destination.next(e)
                }
            }]),
            n
        }(j)
          , J = function(t) {
            return function(e) {
                for (var n = 0, r = t.length; n < r && !e.closed; n++)
                    e.next(t[n]);
                e.complete()
            }
        };
        function K() {
            return "function" == typeof Symbol && Symbol.iterator ? Symbol.iterator : "@@iterator"
        }
        var Y = K()
          , X = function(t) {
            return t && "number" == typeof t.length && "function" != typeof t
        };
        function $(t) {
            return !!t && "function" != typeof t.subscribe && "function" == typeof t.then
        }
        var tt = function(t) {
            if (t && "function" == typeof t[M])
                return r = t,
                function(t) {
                    var e = r[M]();
                    if ("function" != typeof e.subscribe)
                        throw new TypeError("Provided object does not correctly implement Symbol.observable");
                    return e.subscribe(t)
                }
                ;
            if (X(t))
                return J(t);
            if ($(t))
                return n = t,
                function(t) {
                    return n.then((function(e) {
                        t.closed || (t.next(e),
                        t.complete())
                    }
                    ), (function(e) {
                        return t.error(e)
                    }
                    )).then(null, R),
                    t
                }
                ;
            if (t && "function" == typeof t[Y])
                return e = t,
                function(t) {
                    for (var n = e[Y](); ; ) {
                        var r = void 0;
                        try {
                            r = n.next()
                        } catch (i) {
                            return t.error(i),
                            t
                        }
                        if (r.done) {
                            t.complete();
                            break
                        }
                        if (t.next(r.value),
                        t.closed)
                            break
                    }
                    return "function" == typeof n.return && t.add((function() {
                        n.return && n.return()
                    }
                    )),
                    t
                }
                ;
            var e, n, r, i = C(t) ? "an invalid object" : "'".concat(t, "'"), o = "You provided ".concat(i, " where a stream was expected.") + " You can provide an Observable, Promise, Array, or Iterable.";
            throw new TypeError(o)
        };
        function et(t, e) {
            return new H((function(n) {
                var r = new E
                  , i = 0;
                return r.add(e.schedule((function() {
                    i !== t.length ? (n.next(t[i++]),
                    n.closed || r.add(this.schedule())) : n.complete()
                }
                ))),
                r
            }
            ))
        }
        function nt(t, e) {
            return e ? function(t, e) {
                if (null != t) {
                    if (function(t) {
                        return t && "function" == typeof t[M]
                    }(t))
                        return function(t, e) {
                            return new H((function(n) {
                                var r = new E;
                                return r.add(e.schedule((function() {
                                    var i = t[M]();
                                    r.add(i.subscribe({
                                        next: function(t) {
                                            r.add(e.schedule((function() {
                                                return n.next(t)
                                            }
                                            )))
                                        },
                                        error: function(t) {
                                            r.add(e.schedule((function() {
                                                return n.error(t)
                                            }
                                            )))
                                        },
                                        complete: function() {
                                            r.add(e.schedule((function() {
                                                return n.complete()
                                            }
                                            )))
                                        }
                                    }))
                                }
                                ))),
                                r
                            }
                            ))
                        }(t, e);
                    if ($(t))
                        return function(t, e) {
                            return new H((function(n) {
                                var r = new E;
                                return r.add(e.schedule((function() {
                                    return t.then((function(t) {
                                        r.add(e.schedule((function() {
                                            n.next(t),
                                            r.add(e.schedule((function() {
                                                return n.complete()
                                            }
                                            )))
                                        }
                                        )))
                                    }
                                    ), (function(t) {
                                        r.add(e.schedule((function() {
                                            return n.error(t)
                                        }
                                        )))
                                    }
                                    ))
                                }
                                ))),
                                r
                            }
                            ))
                        }(t, e);
                    if (X(t))
                        return et(t, e);
                    if (function(t) {
                        return t && "function" == typeof t[Y]
                    }(t) || "string" == typeof t)
                        return function(t, e) {
                            if (!t)
                                throw new Error("Iterable cannot be null");
                            return new H((function(n) {
                                var r, i = new E;
                                return i.add((function() {
                                    r && "function" == typeof r.return && r.return()
                                }
                                )),
                                i.add(e.schedule((function() {
                                    r = t[Y](),
                                    i.add(e.schedule((function() {
                                        if (!n.closed) {
                                            var t, e;
                                            try {
                                                var i = r.next();
                                                t = i.value,
                                                e = i.done
                                            } catch (o) {
                                                return void n.error(o)
                                            }
                                            e ? n.complete() : (n.next(t),
                                            this.schedule())
                                        }
                                    }
                                    )))
                                }
                                ))),
                                i
                            }
                            ))
                        }(t, e)
                }
                throw new TypeError((null !== t && typeof t || t) + " is not observable")
            }(t, e) : t instanceof H ? t : new H(tt(t))
        }
        var rt = function(t) {
            d(n, t);
            var e = y(n);
            function n(t) {
                var r;
                return m(this, n),
                (r = e.call(this)).parent = t,
                r
            }
            return _(n, [{
                key: "_next",
                value: function(t) {
                    this.parent.notifyNext(t)
                }
            }, {
                key: "_error",
                value: function(t) {
                    this.parent.notifyError(t),
                    this.unsubscribe()
                }
            }, {
                key: "_complete",
                value: function() {
                    this.parent.notifyComplete(),
                    this.unsubscribe()
                }
            }]),
            n
        }(j)
          , it = function(t) {
            d(n, t);
            var e = y(n);
            function n() {
                return m(this, n),
                e.apply(this, arguments)
            }
            return _(n, [{
                key: "notifyNext",
                value: function(t) {
                    this.destination.next(t)
                }
            }, {
                key: "notifyError",
                value: function(t) {
                    this.destination.error(t)
                }
            }, {
                key: "notifyComplete",
                value: function() {
                    this.destination.complete()
                }
            }]),
            n
        }(j);
        function ot(t, e) {
            if (!e.closed)
                return t instanceof H ? t.subscribe(e) : tt(t)(e)
        }
        function at(t, e) {
            var n = arguments.length > 2 && void 0 !== arguments[2] ? arguments[2] : Number.POSITIVE_INFINITY;
            return "function" == typeof e ? function(r) {
                return r.pipe(at((function(n, r) {
                    return nt(t(n, r)).pipe(W((function(t, i) {
                        return e(n, t, r, i)
                    }
                    )))
                }
                ), n))
            }
            : ("number" == typeof e && (n = e),
            function(e) {
                return e.lift(new ut(t,n))
            }
            )
        }
        var ut = function() {
            function t(e) {
                var n = arguments.length > 1 && void 0 !== arguments[1] ? arguments[1] : Number.POSITIVE_INFINITY;
                m(this, t),
                this.project = e,
                this.concurrent = n
            }
            return _(t, [{
                key: "call",
                value: function(t, e) {
                    return e.subscribe(new st(t,this.project,this.concurrent))
                }
            }]),
            t
        }()
          , st = function(t) {
            d(n, t);
            var e = y(n);
            function n(t, r) {
                var i, o = arguments.length > 2 && void 0 !== arguments[2] ? arguments[2] : Number.POSITIVE_INFINITY;
                return m(this, n),
                (i = e.call(this, t)).project = r,
                i.concurrent = o,
                i.hasCompleted = !1,
                i.buffer = [],
                i.active = 0,
                i.index = 0,
                i
            }
            return _(n, [{
                key: "_next",
                value: function(t) {
                    this.active < this.concurrent ? this._tryNext(t) : this.buffer.push(t)
                }
            }, {
                key: "_tryNext",
                value: function(t) {
                    var e, n = this.index++;
                    try {
                        e = this.project(t, n)
                    } catch (r) {
                        return void this.destination.error(r)
                    }
                    this.active++,
                    this._innerSub(e)
                }
            }, {
                key: "_innerSub",
                value: function(t) {
                    var e = new rt(this)
                      , n = this.destination;
                    n.add(e);
                    var r = ot(t, e);
                    r !== e && n.add(r)
                }
            }, {
                key: "_complete",
                value: function() {
                    this.hasCompleted = !0,
                    0 === this.active && 0 === this.buffer.length && this.destination.complete(),
                    this.unsubscribe()
                }
            }, {
                key: "notifyNext",
                value: function(t) {
                    this.destination.next(t)
                }
            }, {
                key: "notifyComplete",
                value: function() {
                    var t = this.buffer;
                    this.active--,
                    t.length > 0 ? this._next(t.shift()) : 0 === this.active && this.hasCompleted && this.destination.complete()
                }
            }]),
            n
        }(it);
        function lt() {
            var t = arguments.length > 0 && void 0 !== arguments[0] ? arguments[0] : Number.POSITIVE_INFINITY;
            return at(U, t)
        }
        function ct(t, e) {
            return e ? et(t, e) : new H(J(t))
        }
        function ht() {
            return function(t) {
                return t.lift(new ft(t))
            }
        }
        var ft = function() {
            function t(e) {
                m(this, t),
                this.connectable = e
            }
            return _(t, [{
                key: "call",
                value: function(t, e) {
                    var n = this.connectable;
                    n._refCount++;
                    var r = new dt(t,n)
                      , i = e.subscribe(r);
                    return r.closed || (r.connection = n.connect()),
                    i
                }
            }]),
            t
        }()
          , dt = function(t) {
            d(n, t);
            var e = y(n);
            function n(t, r) {
                var i;
                return m(this, n),
                (i = e.call(this, t)).connectable = r,
                i
            }
            return _(n, [{
                key: "_unsubscribe",
                value: function() {
                    var t = this.connectable;
                    if (t) {
                        this.connectable = null;
                        var e = t._refCount;
                        if (e <= 0)
                            this.connection = null;
                        else if (t._refCount = e - 1,
                        e > 1)
                            this.connection = null;
                        else {
                            var n = this.connection
                              , r = t._connection;
                            this.connection = null,
                            !r || n && r !== n || r.unsubscribe()
                        }
                    } else
                        this.connection = null
                }
            }]),
            n
        }(j)
          , vt = function(t) {
            d(n, t);
            var e = y(n);
            function n(t, r) {
                var i;
                return m(this, n),
                (i = e.call(this)).source = t,
                i.subjectFactory = r,
                i._refCount = 0,
                i._isComplete = !1,
                i
            }
            return _(n, [{
                key: "_subscribe",
                value: function(t) {
                    return this.getSubject().subscribe(t)
                }
            }, {
                key: "getSubject",
                value: function() {
                    var t = this._subject;
                    return t && !t.isStopped || (this._subject = this.subjectFactory()),
                    this._subject
                }
            }, {
                key: "connect",
                value: function() {
                    var t = this._connection;
                    return t || (this._isComplete = !1,
                    (t = this._connection = new E).add(this.source.subscribe(new gt(this.getSubject(),this))),
                    t.closed && (this._connection = null,
                    t = E.EMPTY)),
                    t
                }
            }, {
                key: "refCount",
                value: function() {
                    return ht()(this)
                }
            }]),
            n
        }(H)
          , pt = function() {
            var t = vt.prototype;
            return {
                operator: {
                    value: null
                },
                _refCount: {
                    value: 0,
                    writable: !0
                },
                _subject: {
                    value: null,
                    writable: !0
                },
                _connection: {
                    value: null,
                    writable: !0
                },
                _subscribe: {
                    value: t._subscribe
                },
                _isComplete: {
                    value: t._isComplete,
                    writable: !0
                },
                getSubject: {
                    value: t.getSubject
                },
                connect: {
                    value: t.connect
                },
                refCount: {
                    value: t.refCount
                }
            }
        }()
          , gt = function(t) {
            d(n, t);
            var e = y(n);
            function n(t, r) {
                var i;
                return m(this, n),
                (i = e.call(this, t)).connectable = r,
                i
            }
            return _(n, [{
                key: "_error",
                value: function(t) {
                    this._unsubscribe(),
                    i(r(n.prototype), "_error", this).call(this, t)
                }
            }, {
                key: "_complete",
                value: function() {
                    this.connectable._isComplete = !0,
                    this._unsubscribe(),
                    i(r(n.prototype), "_complete", this).call(this)
                }
            }, {
                key: "_unsubscribe",
                value: function() {
                    var t = this.connectable;
                    if (t) {
                        this.connectable = null;
                        var e = t._connection;
                        t._refCount = 0,
                        t._subject = null,
                        t._connection = null,
                        e && e.unsubscribe()
                    }
                }
            }]),
            n
        }(z);
        function yt() {
            return new q
        }
        function mt(t) {
            return {
                toString: t
            }.toString()
        }
        var wt = "__parameters__";
        function _t(t, e, n) {
            return mt((function() {
                var r = function(t) {
                    return function() {
                        if (t) {
                            var e = t.apply(void 0, arguments);
                            for (var n in e)
                                this[n] = e[n]
                        }
                    }
                }(e);
                function i() {
                    for (var t = arguments.length, e = new Array(t), n = 0; n < t; n++)
                        e[n] = arguments[n];
                    if (this instanceof i)
                        return r.apply(this, e),
                        this;
                    var o = k(i, e);
                    return a.annotation = o,
                    a;
                    function a(t, e, n) {
                        for (var r = t.hasOwnProperty(wt) ? t[wt] : Object.defineProperty(t, wt, {
                            value: []
                        })[wt]; r.length <= n; )
                            r.push(null);
                        return (r[n] = r[n] || []).push(o),
                        t
                    }
                }
                return n && (i.prototype = Object.create(n.prototype)),
                i.prototype.ngMetadataName = t,
                i.annotationCls = i,
                i
            }
            ))
        }
        var kt = _t("Inject", (function(t) {
            return {
                token: t
            }
        }
        ))
          , bt = _t("Optional")
          , Ct = _t("Self")
          , St = _t("SkipSelf")
          , xt = function(t) {
            return t[t.Default = 0] = "Default",
            t[t.Host = 1] = "Host",
            t[t.Self = 2] = "Self",
            t[t.SkipSelf = 4] = "SkipSelf",
            t[t.Optional = 8] = "Optional",
            t
        }({});
        function Et(t) {
            for (var e in t)
                if (t[e] === Et)
                    return e;
            throw Error("Could not find renamed property on target object.")
        }
        function Tt(t) {
            return {
                token: t.token,
                providedIn: t.providedIn || null,
                factory: t.factory,
                value: void 0
            }
        }
        function Ot(t) {
            return {
                factory: t.factory,
                providers: t.providers || [],
                imports: t.imports || []
            }
        }
        function At(t) {
            return Rt(t, t[It]) || Rt(t, t[Mt])
        }
        function Rt(t, e) {
            return e && e.token === t ? e : null
        }
        function Pt(t) {
            return t && (t.hasOwnProperty(jt) || t.hasOwnProperty(Ut)) ? t[jt] : null
        }
        var It = Et({
            "\u0275prov": Et
        })
          , jt = Et({
            "\u0275inj": Et
        })
          , Nt = Et({
            "\u0275provFallback": Et
        })
          , Mt = Et({
            ngInjectableDef: Et
        })
          , Ut = Et({
            ngInjectorDef: Et
        });
        function Dt(t) {
            if ("string" == typeof t)
                return t;
            if (Array.isArray(t))
                return "[" + t.map(Dt).join(", ") + "]";
            if (null == t)
                return "" + t;
            if (t.overriddenName)
                return "".concat(t.overriddenName);
            if (t.name)
                return "".concat(t.name);
            var e = t.toString();
            if (null == e)
                return "" + e;
            var n = e.indexOf("\n");
            return -1 === n ? e : e.substring(0, n)
        }
        function Ht(t, e) {
            return null == t || "" === t ? null === e ? "" : e : null == e || "" === e ? t : t + " " + e
        }
        var Lt = Et({
            __forward_ref__: Et
        });
        function Ft(t) {
            return t.__forward_ref__ = Ft,
            t.toString = function() {
                return Dt(this())
            }
            ,
            t
        }
        function Vt(t) {
            return "function" == typeof (e = t) && e.hasOwnProperty(Lt) && e.__forward_ref__ === Ft ? t() : t;
            var e
        }
        var zt = "undefined" != typeof globalThis && globalThis
          , qt = "undefined" != typeof window && window
          , Bt = "undefined" != typeof self && "undefined" != typeof WorkerGlobalScope && self instanceof WorkerGlobalScope && self
          , Zt = "undefined" != typeof global && global
          , Wt = zt || Zt || qt || Bt
          , Gt = Et({
            "\u0275cmp": Et
        })
          , Qt = Et({
            "\u0275dir": Et
        })
          , Jt = Et({
            "\u0275pipe": Et
        })
          , Kt = Et({
            "\u0275mod": Et
        })
          , Yt = Et({
            "\u0275loc": Et
        })
          , Xt = Et({
            "\u0275fac": Et
        })
          , $t = Et({
            __NG_ELEMENT_ID__: Et
        });
        var te, ee = function() {
            function t(e, n) {
                m(this, t),
                this._desc = e,
                this.ngMetadataName = "InjectionToken",
                this.\u0275prov = void 0,
                "number" == typeof n ? this.__NG_ELEMENT_ID__ = n : void 0 !== n && (this.\u0275prov = Tt({
                    token: this,
                    providedIn: n.providedIn || "root",
                    factory: n.factory
                }))
            }
            return _(t, [{
                key: "toString",
                value: function() {
                    return "InjectionToken ".concat(this._desc)
                }
            }]),
            t
        }(), ne = new ee("INJECTOR",-1), re = {}, ie = /\n/gm, oe = "__source", ae = Et({
            provide: String,
            useValue: Et
        }), ue = void 0;
        function se(t) {
            var e = ue;
            return ue = t,
            e
        }
        function le(t) {
            var e = te;
            return te = t,
            e
        }
        function ce(t) {
            var e = arguments.length > 1 && void 0 !== arguments[1] ? arguments[1] : xt.Default;
            if (void 0 === ue)
                throw new Error("inject() must be called from an injection context");
            return null === ue ? fe(t, void 0, e) : ue.get(t, e & xt.Optional ? null : void 0, e)
        }
        function he(t) {
            var e = arguments.length > 1 && void 0 !== arguments[1] ? arguments[1] : xt.Default;
            return (te || ce)(Vt(t), e)
        }
        function fe(t, e, n) {
            var r = At(t);
            if (r && "root" == r.providedIn)
                return void 0 === r.value ? r.value = r.factory() : r.value;
            if (n & xt.Optional)
                return null;
            if (void 0 !== e)
                return e;
            throw new Error("Injector: NOT_FOUND [".concat(Dt(t), "]"))
        }
        function de(t) {
            for (var e = [], n = 0; n < t.length; n++) {
                var r = Vt(t[n]);
                if (Array.isArray(r)) {
                    if (0 === r.length)
                        throw new Error("Arguments array must have arguments.");
                    for (var i = void 0, o = xt.Default, a = 0; a < r.length; a++) {
                        var u = r[a];
                        u instanceof bt || "Optional" === u.ngMetadataName || u === bt ? o |= xt.Optional : u instanceof St || "SkipSelf" === u.ngMetadataName || u === St ? o |= xt.SkipSelf : u instanceof Ct || "Self" === u.ngMetadataName || u === Ct ? o |= xt.Self : i = u instanceof kt || u === kt ? u.token : u
                    }
                    e.push(he(i, o))
                } else
                    e.push(he(r))
            }
            return e
        }
        var ve = function() {
            function t() {
                m(this, t)
            }
            return _(t, [{
                key: "get",
                value: function(t) {
                    var e = arguments.length > 1 && void 0 !== arguments[1] ? arguments[1] : re;
                    if (e === re) {
                        var n = new Error("NullInjectorError: No provider for ".concat(Dt(t), "!"));
                        throw n.name = "NullInjectorError",
                        n
                    }
                    return e
                }
            }]),
            t
        }();
        function pe(t, e, n, r) {
            var i = t.ngTempTokenPath;
            throw e[oe] && i.unshift(e[oe]),
            t.message = function(t, e, n) {
                var r = arguments.length > 3 && void 0 !== arguments[3] ? arguments[3] : null;
                t = t && "\n" === t.charAt(0) && "\u0275" == t.charAt(1) ? t.substr(2) : t;
                var i = Dt(e);
                if (Array.isArray(e))
                    i = e.map(Dt).join(" -> ");
                else if ("object" == typeof e) {
                    var o = [];
                    for (var a in e)
                        if (e.hasOwnProperty(a)) {
                            var u = e[a];
                            o.push(a + ":" + ("string" == typeof u ? JSON.stringify(u) : Dt(u)))
                        }
                    i = "{".concat(o.join(", "), "}")
                }
                return "".concat(n).concat(r ? "(" + r + ")" : "", "[").concat(i, "]: ").concat(t.replace(ie, "\n  "))
            }("\n" + t.message, i, n, r),
            t.ngTokenPath = i,
            t.ngTempTokenPath = null,
            t
        }
        var ge = function t() {
            m(this, t)
        }
          , ye = function t() {
            m(this, t)
        };
        function me(t, e) {
            t.forEach((function(t) {
                return Array.isArray(t) ? me(t, e) : e(t)
            }
            ))
        }
        function we(t, e, n) {
            e >= t.length ? t.push(n) : t.splice(e, 0, n)
        }
        function _e(t, e) {
            return e >= t.length - 1 ? t.pop() : t.splice(e, 1)[0]
        }
        var ke = function(t) {
            return t[t.OnPush = 0] = "OnPush",
            t[t.Default = 1] = "Default",
            t
        }({})
          , be = function(t) {
            return t[t.Emulated = 0] = "Emulated",
            t[t.Native = 1] = "Native",
            t[t.None = 2] = "None",
            t[t.ShadowDom = 3] = "ShadowDom",
            t
        }({})
          , Ce = {}
          , Se = []
          , xe = 0;
        function Ee(t) {
            return mt((function() {
                var e = {}
                  , n = {
                    type: t.type,
                    providersResolver: null,
                    decls: t.decls,
                    vars: t.vars,
                    factory: null,
                    template: t.template || null,
                    consts: t.consts || null,
                    ngContentSelectors: t.ngContentSelectors,
                    hostBindings: t.hostBindings || null,
                    hostVars: t.hostVars || 0,
                    hostAttrs: t.hostAttrs || null,
                    contentQueries: t.contentQueries || null,
                    declaredInputs: e,
                    inputs: null,
                    outputs: null,
                    exportAs: t.exportAs || null,
                    onPush: t.changeDetection === ke.OnPush,
                    directiveDefs: null,
                    pipeDefs: null,
                    selectors: t.selectors || Se,
                    viewQuery: t.viewQuery || null,
                    features: t.features || null,
                    data: t.data || {},
                    encapsulation: t.encapsulation || be.Emulated,
                    id: "c",
                    styles: t.styles || Se,
                    _: null,
                    setInput: null,
                    schemas: t.schemas || null,
                    tView: null
                }
                  , r = t.directives
                  , i = t.features
                  , o = t.pipes;
                return n.id += xe++,
                n.inputs = Pe(t.inputs, e),
                n.outputs = Pe(t.outputs),
                i && i.forEach((function(t) {
                    return t(n)
                }
                )),
                n.directiveDefs = r ? function() {
                    return ("function" == typeof r ? r() : r).map(Te)
                }
                : null,
                n.pipeDefs = o ? function() {
                    return ("function" == typeof o ? o() : o).map(Oe)
                }
                : null,
                n
            }
            ))
        }
        function Te(t) {
            return je(t) || function(t) {
                return t[Qt] || null
            }(t)
        }
        function Oe(t) {
            return function(t) {
                return t[Jt] || null
            }(t)
        }
        var Ae = {};
        function Re(t) {
            var e = {
                type: t.type,
                bootstrap: t.bootstrap || Se,
                declarations: t.declarations || Se,
                imports: t.imports || Se,
                exports: t.exports || Se,
                transitiveCompileScopes: null,
                schemas: t.schemas || null,
                id: t.id || null
            };
            return null != t.id && mt((function() {
                Ae[t.id] = t.type
            }
            )),
            e
        }
        function Pe(t, e) {
            if (null == t)
                return Ce;
            var n = {};
            for (var r in t)
                if (t.hasOwnProperty(r)) {
                    var i = t[r]
                      , o = i;
                    Array.isArray(i) && (o = i[1],
                    i = i[0]),
                    n[i] = r,
                    e && (e[i] = o)
                }
            return n
        }
        var Ie = Ee;
        function je(t) {
            return t[Gt] || null
        }
        function Ne(t, e) {
            return t.hasOwnProperty(Xt) ? t[Xt] : null
        }
        function Me(t, e) {
            var n = t[Kt] || null;
            if (!n && !0 === e)
                throw new Error("Type ".concat(Dt(t), " does not have '\u0275mod' property."));
            return n
        }
        var Ue = 20
          , De = 10;
        function He(t) {
            return Array.isArray(t) && "object" == typeof t[1]
        }
        function Le(t) {
            return Array.isArray(t) && !0 === t[1]
        }
        function Fe(t) {
            return 0 != (8 & t.flags)
        }
        function Ve(t) {
            return 2 == (2 & t.flags)
        }
        function ze(t) {
            return 1 == (1 & t.flags)
        }
        function qe(t) {
            return null !== t.template
        }
        function Be(t) {
            return 0 != (512 & t[2])
        }
        var Ze = function() {
            function t(e, n, r) {
                m(this, t),
                this.previousValue = e,
                this.currentValue = n,
                this.firstChange = r
            }
            return _(t, [{
                key: "isFirstChange",
                value: function() {
                    return this.firstChange
                }
            }]),
            t
        }();
        function We() {
            var t = Qe(this)
              , e = null == t ? void 0 : t.current;
            if (e) {
                var n = t.previous;
                if (n === Ce)
                    t.previous = e;
                else
                    for (var r in e)
                        n[r] = e[r];
                t.current = null,
                this.ngOnChanges(e)
            }
        }
        function Ge(t, e, n, r) {
            var i = Qe(t) || function(t, e) {
                return t.__ngSimpleChanges__ = e
            }(t, {
                previous: Ce,
                current: null
            })
              , o = i.current || (i.current = {})
              , a = i.previous
              , u = this.declaredInputs[n]
              , s = a[u];
            o[u] = new Ze(s && s.currentValue,e,a === Ce),
            t[r] = e
        }
        function Qe(t) {
            return t.__ngSimpleChanges__ || null
        }
        var Je = "http://www.w3.org/2000/svg"
          , Ke = void 0;
        function Ye(t) {
            return !!t.listen
        }
        var Xe = {
            createRenderer: function(t, e) {
                return void 0 !== Ke ? Ke : "undefined" != typeof document ? document : void 0
            }
        };
        function $e(t) {
            for (; Array.isArray(t); )
                t = t[0];
            return t
        }
        function tn(t, e) {
            return $e(e[t.index])
        }
        function en(t, e) {
            return t.data[e + Ue]
        }
        function nn(t, e) {
            var n = e[t];
            return He(n) ? n : n[0]
        }
        function rn(t) {
            var e = function(t) {
                return t.__ngContext__ || null
            }(t);
            return e ? Array.isArray(e) ? e : e.lView : null
        }
        function on(t) {
            return 128 == (128 & t[2])
        }
        function an(t, e) {
            return null === t || null == e ? null : t[e]
        }
        function un(t) {
            t[18] = 0
        }
        function sn(t, e) {
            t[5] += e;
            for (var n = t, r = t[3]; null !== r && (1 === e && 1 === n[5] || -1 === e && 0 === n[5]); )
                r[5] += e,
                n = r,
                r = r[3]
        }
        var ln = {
            lFrame: xn(null),
            bindingsEnabled: !0,
            checkNoChangesMode: !1
        };
        function cn() {
            return ln.bindingsEnabled
        }
        function hn() {
            return ln.lFrame.lView
        }
        function fn() {
            return ln.lFrame.tView
        }
        function dn() {
            return ln.lFrame.currentTNode
        }
        function vn(t, e) {
            ln.lFrame.currentTNode = t,
            ln.lFrame.isParent = e
        }
        function pn() {
            return ln.lFrame.isParent
        }
        function gn() {
            return ln.checkNoChangesMode
        }
        function yn(t) {
            ln.checkNoChangesMode = t
        }
        function mn() {
            return ln.lFrame.bindingIndex++
        }
        function wn(t, e) {
            var n = ln.lFrame;
            n.bindingIndex = n.bindingRootIndex = t,
            _n(e)
        }
        function _n(t) {
            ln.lFrame.currentDirectiveIndex = t
        }
        function kn(t) {
            ln.lFrame.currentQueryIndex = t
        }
        function bn(t, e) {
            var n = Sn();
            ln.lFrame = n,
            n.currentTNode = e,
            n.lView = t
        }
        function Cn(t) {
            var e = Sn()
              , n = t[1];
            ln.lFrame = e,
            e.currentTNode = n.firstChild,
            e.lView = t,
            e.tView = n,
            e.contextLView = t,
            e.bindingIndex = n.bindingStartIndex
        }
        function Sn() {
            var t = ln.lFrame
              , e = null === t ? null : t.child;
            return null === e ? xn(t) : e
        }
        function xn(t) {
            var e = {
                currentTNode: null,
                isParent: !0,
                lView: null,
                tView: null,
                selectedIndex: 0,
                contextLView: null,
                elementDepthCount: 0,
                currentNamespace: null,
                currentDirectiveIndex: -1,
                bindingRootIndex: -1,
                bindingIndex: -1,
                currentQueryIndex: 0,
                parent: t,
                child: null
            };
            return null !== t && (t.child = e),
            e
        }
        function En() {
            var t = ln.lFrame;
            return ln.lFrame = t.parent,
            t.currentTNode = null,
            t.lView = null,
            t
        }
        var Tn = En;
        function On() {
            var t = En();
            t.isParent = !0,
            t.tView = null,
            t.selectedIndex = 0,
            t.contextLView = null,
            t.elementDepthCount = 0,
            t.currentDirectiveIndex = -1,
            t.currentNamespace = null,
            t.bindingRootIndex = -1,
            t.bindingIndex = -1,
            t.currentQueryIndex = 0
        }
        function An(t) {
            return (ln.lFrame.contextLView = function(t, e) {
                for (; t > 0; )
                    e = e[15],
                    t--;
                return e
            }(t, ln.lFrame.contextLView))[8]
        }
        function Rn() {
            return ln.lFrame.selectedIndex
        }
        function Pn(t) {
            ln.lFrame.selectedIndex = t
        }
        function In() {
            ln.lFrame.currentNamespace = Je
        }
        function jn() {
            ln.lFrame.currentNamespace = null
        }
        function Nn(t, e) {
            for (var n = e.directiveStart, r = e.directiveEnd; n < r; n++) {
                var i = t.data[n].type.prototype
                  , o = i.ngAfterContentInit
                  , a = i.ngAfterContentChecked
                  , u = i.ngAfterViewInit
                  , s = i.ngAfterViewChecked
                  , l = i.ngOnDestroy;
                o && (t.contentHooks || (t.contentHooks = [])).push(-n, o),
                a && ((t.contentHooks || (t.contentHooks = [])).push(n, a),
                (t.contentCheckHooks || (t.contentCheckHooks = [])).push(n, a)),
                u && (t.viewHooks || (t.viewHooks = [])).push(-n, u),
                s && ((t.viewHooks || (t.viewHooks = [])).push(n, s),
                (t.viewCheckHooks || (t.viewCheckHooks = [])).push(n, s)),
                null != l && (t.destroyHooks || (t.destroyHooks = [])).push(n, l)
            }
        }
        function Mn(t, e, n) {
            Hn(t, e, 3, n)
        }
        function Un(t, e, n, r) {
            (3 & t[2]) === n && Hn(t, e, n, r)
        }
        function Dn(t, e) {
            var n = t[2];
            (3 & n) === e && (n &= 2047,
            t[2] = n += 1)
        }
        function Hn(t, e, n, r) {
            for (var i = null != r ? r : -1, o = 0, a = void 0 !== r ? 65535 & t[18] : 0; a < e.length; a++)
                if ("number" == typeof e[a + 1]) {
                    if (o = e[a],
                    null != r && o >= r)
                        break
                } else
                    e[a] < 0 && (t[18] += 65536),
                    (o < i || -1 == i) && (Ln(t, n, e, a),
                    t[18] = (4294901760 & t[18]) + a + 2),
                    a++
        }
        function Ln(t, e, n, r) {
            var i = n[r] < 0
              , o = n[r + 1]
              , a = t[i ? -n[r] : n[r]];
            i ? t[2] >> 11 < t[18] >> 16 && (3 & t[2]) === e && (t[2] += 2048,
            o.call(a)) : o.call(a)
        }
        var Fn = -1
          , Vn = function t(e, n, r) {
            m(this, t),
            this.factory = e,
            this.resolving = !1,
            this.canSeeViewProviders = n,
            this.injectImpl = r
        };
        function zn(t, e, n) {
            for (var r = Ye(t), i = 0; i < n.length; ) {
                var o = n[i];
                if ("number" == typeof o) {
                    if (0 !== o)
                        break;
                    i++;
                    var a = n[i++]
                      , u = n[i++]
                      , s = n[i++];
                    r ? t.setAttribute(e, u, s, a) : e.setAttributeNS(a, u, s)
                } else {
                    var l = o
                      , c = n[++i];
                    Bn(l) ? r && t.setProperty(e, l, c) : r ? t.setAttribute(e, l, c) : e.setAttribute(l, c),
                    i++
                }
            }
            return i
        }
        function qn(t) {
            return 3 === t || 4 === t || 6 === t
        }
        function Bn(t) {
            return 64 === t.charCodeAt(0)
        }
        function Zn(t, e) {
            if (null === e || 0 === e.length)
                ;
            else if (null === t || 0 === t.length)
                t = e.slice();
            else
                for (var n = -1, r = 0; r < e.length; r++) {
                    var i = e[r];
                    "number" == typeof i ? n = i : 0 === n || Wn(t, n, i, null, -1 === n || 2 === n ? e[++r] : null)
                }
            return t
        }
        function Wn(t, e, n, r, i) {
            var o = 0
              , a = t.length;
            if (-1 === e)
                a = -1;
            else
                for (; o < t.length; ) {
                    var u = t[o++];
                    if ("number" == typeof u) {
                        if (u === e) {
                            a = -1;
                            break
                        }
                        if (u > e) {
                            a = o - 1;
                            break
                        }
                    }
                }
            for (; o < t.length; ) {
                var s = t[o];
                if ("number" == typeof s)
                    break;
                if (s === n) {
                    if (null === r)
                        return void (null !== i && (t[o + 1] = i));
                    if (r === t[o + 1])
                        return void (t[o + 2] = i)
                }
                o++,
                null !== r && o++,
                null !== i && o++
            }
            -1 !== a && (t.splice(a, 0, e),
            o = a + 1),
            t.splice(o++, 0, n),
            null !== r && t.splice(o++, 0, r),
            null !== i && t.splice(o++, 0, i)
        }
        function Gn(t) {
            return t !== Fn
        }
        function Qn(t) {
            return 32767 & t
        }
        function Jn(t, e) {
            for (var n = t >> 16, r = e; n > 0; )
                r = r[15],
                n--;
            return r
        }
        function Kn(t) {
            return "string" == typeof t ? t : null == t ? "" : "" + t
        }
        function Yn(t) {
            return "function" == typeof t ? t.name || t.toString() : "object" == typeof t && null != t && "function" == typeof t.type ? t.type.name || t.type.toString() : Kn(t)
        }
        var Xn = function() {
            return ("undefined" != typeof requestAnimationFrame && requestAnimationFrame || setTimeout).bind(Wt)
        }();
        function $n(t) {
            return t instanceof Function ? t() : t
        }
        var tr = !0;
        function er(t) {
            var e = tr;
            return tr = t,
            e
        }
        var nr = 0;
        function rr(t, e) {
            var n = or(t, e);
            if (-1 !== n)
                return n;
            var r = e[1];
            r.firstCreatePass && (t.injectorIndex = e.length,
            ir(r.data, t),
            ir(e, null),
            ir(r.blueprint, null));
            var i = ar(t, e)
              , o = t.injectorIndex;
            if (Gn(i))
                for (var a = Qn(i), u = Jn(i, e), s = u[1].data, l = 0; l < 8; l++)
                    e[o + l] = u[a + l] | s[a + l];
            return e[o + 8] = i,
            o
        }
        function ir(t, e) {
            t.push(0, 0, 0, 0, 0, 0, 0, 0, e)
        }
        function or(t, e) {
            return -1 === t.injectorIndex || t.parent && t.parent.injectorIndex === t.injectorIndex || null === e[t.injectorIndex + 8] ? -1 : t.injectorIndex
        }
        function ar(t, e) {
            if (t.parent && -1 !== t.parent.injectorIndex)
                return t.parent.injectorIndex;
            for (var n = 0, r = null, i = e; null !== i; ) {
                var o = i[1]
                  , a = o.type;
                if (null === (r = 2 === a ? o.declTNode : 1 === a ? i[6] : null))
                    return Fn;
                if (n++,
                i = i[15],
                -1 !== r.injectorIndex)
                    return r.injectorIndex | n << 16
            }
            return Fn
        }
        function ur(t, e, n) {
            !function(t, e, n) {
                var r;
                "string" == typeof n ? r = n.charCodeAt(0) || 0 : n.hasOwnProperty($t) && (r = n[$t]),
                null == r && (r = n[$t] = nr++);
                var i = 255 & r
                  , o = 1 << i
                  , a = 64 & i
                  , u = 32 & i
                  , s = e.data;
                128 & i ? a ? u ? s[t + 7] |= o : s[t + 6] |= o : u ? s[t + 5] |= o : s[t + 4] |= o : a ? u ? s[t + 3] |= o : s[t + 2] |= o : u ? s[t + 1] |= o : s[t] |= o
            }(t, e, n)
        }
        function sr(t, e, n) {
            var r = arguments.length > 3 && void 0 !== arguments[3] ? arguments[3] : xt.Default
              , i = arguments.length > 4 ? arguments[4] : void 0;
            if (null !== t) {
                var o = fr(n);
                if ("function" == typeof o) {
                    bn(e, t);
                    try {
                        var a = o();
                        if (null != a || r & xt.Optional)
                            return a;
                        throw new Error("No provider for ".concat(Yn(n), "!"))
                    } finally {
                        Tn()
                    }
                } else if ("number" == typeof o) {
                    if (-1 === o)
                        return new pr(t,e);
                    var u = null
                      , s = or(t, e)
                      , l = Fn
                      , c = r & xt.Host ? e[16][6] : null;
                    for ((-1 === s || r & xt.SkipSelf) && ((l = -1 === s ? ar(t, e) : e[s + 8]) !== Fn && vr(r, !1) ? (u = e[1],
                    s = Qn(l),
                    e = Jn(l, e)) : s = -1); -1 !== s; ) {
                        var h = e[1];
                        if (dr(o, s, h.data)) {
                            var f = cr(s, e, n, u, r, c);
                            if (f !== lr)
                                return f
                        }
                        (l = e[s + 8]) !== Fn && vr(r, e[1].data[s + 8] === c) && dr(o, s, e) ? (u = h,
                        s = Qn(l),
                        e = Jn(l, e)) : s = -1
                    }
                }
            }
            if (r & xt.Optional && void 0 === i && (i = null),
            0 == (r & (xt.Self | xt.Host))) {
                var d = e[9]
                  , v = le(void 0);
                try {
                    return d ? d.get(n, i, r & xt.Optional) : fe(n, i, r & xt.Optional)
                } finally {
                    le(v)
                }
            }
            if (r & xt.Optional)
                return i;
            throw new Error("NodeInjector: NOT_FOUND [".concat(Yn(n), "]"))
        }
        var lr = {};
        function cr(t, e, n, r, i, o) {
            var a = e[1]
              , u = a.data[t + 8]
              , s = function(t, e, n, r, i) {
                for (var o = t.providerIndexes, a = e.data, u = 1048575 & o, s = t.directiveStart, l = o >> 20, c = i ? u + l : t.directiveEnd, h = r ? u : u + l; h < c; h++) {
                    var f = a[h];
                    if (h < s && n === f || h >= s && f.type === n)
                        return h
                }
                if (i) {
                    var d = a[s];
                    if (d && qe(d) && d.type === n)
                        return s
                }
                return null
            }(u, a, n, null == r ? Ve(u) && tr : r != a && 2 === u.type, i & xt.Host && o === u);
            return null !== s ? hr(e, a, s, u) : lr
        }
        function hr(t, e, n, r) {
            var i = t[n]
              , o = e.data;
            if (i instanceof Vn) {
                var a = i;
                if (a.resolving)
                    throw new Error("Circular dep for ".concat(Yn(o[n])));
                var u = er(a.canSeeViewProviders);
                a.resolving = !0;
                var s = a.injectImpl ? le(a.injectImpl) : null;
                bn(t, r);
                try {
                    i = t[n] = a.factory(void 0, o, t, r),
                    e.firstCreatePass && n >= r.directiveStart && function(t, e, n) {
                        var r, i = e.type.prototype, o = i.ngOnInit, a = i.ngDoCheck;
                        if (i.ngOnChanges) {
                            var u = ((r = e).type.prototype.ngOnChanges && (r.setInput = Ge),
                            We);
                            (n.preOrderHooks || (n.preOrderHooks = [])).push(t, u),
                            (n.preOrderCheckHooks || (n.preOrderCheckHooks = [])).push(t, u)
                        }
                        o && (n.preOrderHooks || (n.preOrderHooks = [])).push(0 - t, o),
                        a && ((n.preOrderHooks || (n.preOrderHooks = [])).push(t, a),
                        (n.preOrderCheckHooks || (n.preOrderCheckHooks = [])).push(t, a))
                    }(n, o[n], e)
                } finally {
                    null !== s && le(s),
                    er(u),
                    a.resolving = !1,
                    Tn()
                }
            }
            return i
        }
        function fr(t) {
            if ("string" == typeof t)
                return t.charCodeAt(0) || 0;
            var e = t.hasOwnProperty($t) ? t[$t] : void 0;
            return "number" == typeof e && e > 0 ? 255 & e : e
        }
        function dr(t, e, n) {
            var r = 64 & t
              , i = 32 & t;
            return !!((128 & t ? r ? i ? n[e + 7] : n[e + 6] : i ? n[e + 5] : n[e + 4] : r ? i ? n[e + 3] : n[e + 2] : i ? n[e + 1] : n[e]) & 1 << t)
        }
        function vr(t, e) {
            return !(t & xt.Self || t & xt.Host && e)
        }
        var pr = function() {
            function t(e, n) {
                m(this, t),
                this._tNode = e,
                this._lView = n
            }
            return _(t, [{
                key: "get",
                value: function(t, e) {
                    return sr(this._tNode, this._lView, t, void 0, e)
                }
            }]),
            t
        }();
        function gr(t) {
            return t.ngDebugContext
        }
        function yr(t) {
            return t.ngOriginalError
        }
        function mr(t) {
            for (var e = arguments.length, n = new Array(e > 1 ? e - 1 : 0), r = 1; r < e; r++)
                n[r - 1] = arguments[r];
            t.error.apply(t, n)
        }
        var wr = function() {
            function t() {
                m(this, t),
                this._console = console
            }
            return _(t, [{
                key: "handleError",
                value: function(t) {
                    var e = this._findOriginalError(t)
                      , n = this._findContext(t)
                      , r = function(t) {
                        return t.ngErrorLogger || mr
                    }(t);
                    r(this._console, "ERROR", t),
                    e && r(this._console, "ORIGINAL ERROR", e),
                    n && r(this._console, "ERROR CONTEXT", n)
                }
            }, {
                key: "_findContext",
                value: function(t) {
                    return t ? gr(t) ? gr(t) : this._findContext(yr(t)) : null
                }
            }, {
                key: "_findOriginalError",
                value: function(t) {
                    for (var e = yr(t); e && yr(e); )
                        e = yr(e);
                    return e
                }
            }]),
            t
        }()
          , _r = !0
          , kr = !1;
        function br() {
            return kr = !0,
            _r
        }
        function Cr(t, e) {
            t.__ngContext__ = e
        }
        function Sr(t, e, n) {
            for (var r = t.length; ; ) {
                var i = t.indexOf(e, n);
                if (-1 === i)
                    return i;
                if (0 === i || t.charCodeAt(i - 1) <= 32) {
                    var o = e.length;
                    if (i + o === r || t.charCodeAt(i + o) <= 32)
                        return i
                }
                n = i + 1
            }
        }
        var xr = "ng-template";
        function Er(t, e, n) {
            for (var r = 0; r < t.length; ) {
                var i = t[r++];
                if (n && "class" === i) {
                    if (-1 !== Sr((i = t[r]).toLowerCase(), e, 0))
                        return !0
                } else if (1 === i) {
                    for (; r < t.length && "string" == typeof (i = t[r++]); )
                        if (i.toLowerCase() === e)
                            return !0;
                    return !1
                }
            }
            return !1
        }
        function Tr(t) {
            return 0 === t.type && t.tagName !== xr
        }
        function Or(t, e, n) {
            return e === (0 !== t.type || n ? t.tagName : xr)
        }
        function Ar(t, e, n) {
            for (var r = 4, i = t.attrs || [], o = function(t) {
                for (var e = 0; e < t.length; e++)
                    if (qn(t[e]))
                        return e;
                return t.length
            }(i), a = !1, u = 0; u < e.length; u++) {
                var s = e[u];
                if ("number" != typeof s) {
                    if (!a)
                        if (4 & r) {
                            if (r = 2 | 1 & r,
                            "" !== s && !Or(t, s, n) || "" === s && 1 === e.length) {
                                if (Rr(r))
                                    return !1;
                                a = !0
                            }
                        } else {
                            var l = 8 & r ? s : e[++u];
                            if (8 & r && null !== t.attrs) {
                                if (!Er(t.attrs, l, n)) {
                                    if (Rr(r))
                                        return !1;
                                    a = !0
                                }
                                continue
                            }
                            var c = Pr(8 & r ? "class" : s, i, Tr(t), n);
                            if (-1 === c) {
                                if (Rr(r))
                                    return !1;
                                a = !0;
                                continue
                            }
                            if ("" !== l) {
                                var h;
                                h = c > o ? "" : i[c + 1].toLowerCase();
                                var f = 8 & r ? h : null;
                                if (f && -1 !== Sr(f, l, 0) || 2 & r && l !== h) {
                                    if (Rr(r))
                                        return !1;
                                    a = !0
                                }
                            }
                        }
                } else {
                    if (!a && !Rr(r) && !Rr(s))
                        return !1;
                    if (a && Rr(s))
                        continue;
                    a = !1,
                    r = s | 1 & r
                }
            }
            return Rr(r) || a
        }
        function Rr(t) {
            return 0 == (1 & t)
        }
        function Pr(t, e, n, r) {
            if (null === e)
                return -1;
            var i = 0;
            if (r || !n) {
                for (var o = !1; i < e.length; ) {
                    var a = e[i];
                    if (a === t)
                        return i;
                    if (3 === a || 6 === a)
                        o = !0;
                    else {
                        if (1 === a || 2 === a) {
                            for (var u = e[++i]; "string" == typeof u; )
                                u = e[++i];
                            continue
                        }
                        if (4 === a)
                            break;
                        if (0 === a) {
                            i += 4;
                            continue
                        }
                    }
                    i += o ? 1 : 2
                }
                return -1
            }
            return function(t, e) {
                var n = t.indexOf(4);
                if (n > -1)
                    for (n++; n < t.length; ) {
                        var r = t[n];
                        if ("number" == typeof r)
                            return -1;
                        if (r === e)
                            return n;
                        n++
                    }
                return -1
            }(e, t)
        }
        function Ir(t, e) {
            for (var n = arguments.length > 2 && void 0 !== arguments[2] && arguments[2], r = 0; r < e.length; r++)
                if (Ar(t, e[r], n))
                    return !0;
            return !1
        }
        function jr(t, e) {
            return t ? ":not(" + e.trim() + ")" : e
        }
        function Nr(t) {
            for (var e = t[0], n = 1, r = 2, i = "", o = !1; n < t.length; ) {
                var a = t[n];
                if ("string" == typeof a)
                    if (2 & r) {
                        var u = t[++n];
                        i += "[" + a + (u.length > 0 ? '="' + u + '"' : "") + "]"
                    } else
                        8 & r ? i += "." + a : 4 & r && (i += " " + a);
                else
                    "" === i || Rr(a) || (e += jr(o, i),
                    i = ""),
                    r = a,
                    o = o || !Rr(r);
                n++
            }
            return "" !== i && (e += jr(o, i)),
            e
        }
        var Mr = {};
        function Ur(t) {
            var e = t[3];
            return Le(e) ? e[3] : e
        }
        function Dr(t) {
            return Lr(t[13])
        }
        function Hr(t) {
            return Lr(t[4])
        }
        function Lr(t) {
            for (; null !== t && !Le(t); )
                t = t[4];
            return t
        }
        function Fr(t) {
            Vr(fn(), hn(), Rn() + t, gn())
        }
        function Vr(t, e, n, r) {
            if (!r)
                if (3 == (3 & e[2])) {
                    var i = t.preOrderCheckHooks;
                    null !== i && Mn(e, i, n)
                } else {
                    var o = t.preOrderHooks;
                    null !== o && Un(e, o, 0, n)
                }
            Pn(n)
        }
        function zr(t, e) {
            var n = t.contentQueries;
            if (null !== n)
                for (var r = 0; r < n.length; r += 2) {
                    var i = n[r + 1];
                    if (-1 !== i) {
                        var o = t.data[i];
                        kn(n[r]),
                        o.contentQueries(2, e[i], i)
                    }
                }
        }
        function qr(t, e, n) {
            return Ye(e) ? e.createElement(t, n) : null === n ? e.createElement(t) : e.createElementNS(n, t)
        }
        function Br(t, e, n, r, i, o, a, u, s, l) {
            var c = e.blueprint.slice();
            return c[0] = i,
            c[2] = 140 | r,
            un(c),
            c[3] = c[15] = t,
            c[8] = n,
            c[10] = a || t && t[10],
            c[11] = u || t && t[11],
            c[12] = s || t && t[12] || null,
            c[9] = l || t && t[9] || null,
            c[6] = o,
            c[16] = 2 == e.type ? t[16] : c,
            c
        }
        function Zr(t, e, n, r, i) {
            var o = e + Ue
              , a = t.data[o] || function(t, e, n, r, i) {
                var o = dn()
                  , a = pn()
                  , u = t.data[e] = function(t, e, n, r, i, o) {
                    return {
                        type: n,
                        index: r,
                        injectorIndex: e ? e.injectorIndex : -1,
                        directiveStart: -1,
                        directiveEnd: -1,
                        directiveStylingLast: -1,
                        propertyBindings: null,
                        flags: 0,
                        providerIndexes: 0,
                        tagName: i,
                        attrs: o,
                        mergedAttrs: null,
                        localNames: null,
                        initialInputs: void 0,
                        inputs: null,
                        outputs: null,
                        tViews: null,
                        next: null,
                        projectionNext: null,
                        child: null,
                        parent: e,
                        projection: null,
                        styles: null,
                        stylesWithoutHost: null,
                        residualStyles: void 0,
                        classes: null,
                        classesWithoutHost: null,
                        residualClasses: void 0,
                        classBindings: 0,
                        styleBindings: 0
                    }
                }(0, a ? o : o && o.parent, n, e, r, i);
                return null === t.firstChild && (t.firstChild = u),
                null !== o && (a && null == o.child && null !== u.parent ? o.child = u : a || (o.next = u)),
                u
            }(t, o, n, r, i);
            return vn(a, !0),
            a
        }
        function Wr(t, e, n) {
            Cn(e);
            try {
                var r = t.viewQuery;
                null !== r && _i(1, r, n);
                var i = t.template;
                null !== i && Jr(t, e, i, 1, n),
                t.firstCreatePass && (t.firstCreatePass = !1),
                t.staticContentQueries && zr(t, e),
                t.staticViewQueries && _i(2, t.viewQuery, n);
                var o = t.components;
                null !== o && function(t, e) {
                    for (var n = 0; n < e.length; n++)
                        pi(t, e[n])
                }(e, o)
            } catch (a) {
                throw t.firstCreatePass && (t.incompleteFirstPass = !0),
                a
            } finally {
                e[2] &= -5,
                On()
            }
        }
        function Gr(t, e, n, r) {
            var i = e[2];
            if (256 != (256 & i)) {
                Cn(e);
                var o = gn();
                try {
                    un(e),
                    ln.lFrame.bindingIndex = t.bindingStartIndex,
                    null !== n && Jr(t, e, n, 2, r);
                    var a = 3 == (3 & i);
                    if (!o)
                        if (a) {
                            var u = t.preOrderCheckHooks;
                            null !== u && Mn(e, u, null)
                        } else {
                            var s = t.preOrderHooks;
                            null !== s && Un(e, s, 0, null),
                            Dn(e, 0)
                        }
                    if (function(t) {
                        for (var e = Dr(t); null !== e; e = Hr(e))
                            if (e[2])
                                for (var n = e[9], r = 0; r < n.length; r++) {
                                    var i = n[r];
                                    0 == (1024 & i[2]) && sn(i[3], 1),
                                    i[2] |= 1024
                                }
                    }(e),
                    function(t) {
                        for (var e = Dr(t); null !== e; e = Hr(e))
                            for (var n = De; n < e.length; n++) {
                                var r = e[n]
                                  , i = r[1];
                                on(r) && Gr(i, r, i.template, r[8])
                            }
                    }(e),
                    null !== t.contentQueries && zr(t, e),
                    !o)
                        if (a) {
                            var l = t.contentCheckHooks;
                            null !== l && Mn(e, l)
                        } else {
                            var c = t.contentHooks;
                            null !== c && Un(e, c, 1),
                            Dn(e, 1)
                        }
                    !function(t, e) {
                        try {
                            var n = t.expandoInstructions;
                            if (null !== n)
                                for (var r = t.expandoStartIndex, i = -1, o = 0; o < n.length; o++) {
                                    var a = n[o];
                                    "number" == typeof a ? a <= 0 ? (Pn(0 - a),
                                    i = r += 9 + n[++o]) : r += a : (null !== a && (wn(r, i),
                                    a(2, e[i])),
                                    i++)
                                }
                        } finally {
                            Pn(-1)
                        }
                    }(t, e);
                    var h = t.components;
                    null !== h && function(t, e) {
                        for (var n = 0; n < e.length; n++)
                            vi(t, e[n])
                    }(e, h);
                    var f = t.viewQuery;
                    if (null !== f && _i(2, f, r),
                    !o)
                        if (a) {
                            var d = t.viewCheckHooks;
                            null !== d && Mn(e, d)
                        } else {
                            var v = t.viewHooks;
                            null !== v && Un(e, v, 2),
                            Dn(e, 2)
                        }
                    !0 === t.firstUpdatePass && (t.firstUpdatePass = !1),
                    o || (e[2] &= -73),
                    1024 & e[2] && (e[2] &= -1025,
                    sn(e[3], -1))
                } finally {
                    On()
                }
            }
        }
        function Qr(t, e, n, r) {
            var i = e[10]
              , o = !gn()
              , a = 4 == (4 & e[2]);
            try {
                o && !a && i.begin && i.begin(),
                a && Wr(t, e, r),
                Gr(t, e, n, r)
            } finally {
                o && !a && i.end && i.end()
            }
        }
        function Jr(t, e, n, r, i) {
            var o = Rn();
            try {
                Pn(-1),
                2 & r && e.length > Ue && Vr(t, e, 0, gn()),
                n(r, i)
            } finally {
                Pn(o)
            }
        }
        function Kr(t, e, n) {
            cn() && (function(t, e, n, r) {
                var i = n.directiveStart
                  , o = n.directiveEnd;
                t.firstCreatePass || rr(n, e),
                Cr(r, e);
                for (var a = n.initialInputs, u = i; u < o; u++) {
                    var s = t.data[u]
                      , l = qe(s);
                    l && ci(e, n, s);
                    var c = hr(e, t, u, n);
                    Cr(c, e),
                    null !== a && hi(0, u - i, c, s, 0, a),
                    l && (nn(n.index, e)[8] = c)
                }
            }(t, e, n, tn(n, e)),
            128 == (128 & n.flags) && function(t, e, n) {
                var r = n.directiveStart
                  , i = n.directiveEnd
                  , o = t.expandoInstructions
                  , a = t.firstCreatePass
                  , u = n.index - Ue
                  , s = ln.lFrame.currentDirectiveIndex;
                try {
                    Pn(u);
                    for (var l = r; l < i; l++) {
                        var c = t.data[l]
                          , h = e[l];
                        _n(l),
                        null !== c.hostBindings || 0 !== c.hostVars || null !== c.hostAttrs ? ii(c, h) : a && o.push(null)
                    }
                } finally {
                    Pn(-1),
                    _n(s)
                }
            }(t, e, n))
        }
        function Yr(t, e) {
            var n = arguments.length > 2 && void 0 !== arguments[2] ? arguments[2] : tn
              , r = e.localNames;
            if (null !== r)
                for (var i = e.index + 1, o = 0; o < r.length; o += 2) {
                    var a = r[o + 1]
                      , u = -1 === a ? n(e, t) : t[a];
                    t[i++] = u
                }
        }
        function Xr(t) {
            var e = t.tView;
            return null === e || e.incompleteFirstPass ? t.tView = $r(1, null, t.template, t.decls, t.vars, t.directiveDefs, t.pipeDefs, t.viewQuery, t.schemas, t.consts) : e
        }
        function $r(t, e, n, r, i, o, a, u, s, l) {
            var c = Ue + r
              , h = c + i
              , f = function(t, e) {
                for (var n = [], r = 0; r < e; r++)
                    n.push(r < t ? null : Mr);
                return n
            }(c, h)
              , d = "function" == typeof l ? l() : l;
            return f[1] = {
                type: t,
                blueprint: f,
                template: n,
                queries: null,
                viewQuery: u,
                declTNode: e,
                data: f.slice().fill(null, c),
                bindingStartIndex: c,
                expandoStartIndex: h,
                expandoInstructions: null,
                firstCreatePass: !0,
                firstUpdatePass: !0,
                staticViewQueries: !1,
                staticContentQueries: !1,
                preOrderHooks: null,
                preOrderCheckHooks: null,
                contentHooks: null,
                contentCheckHooks: null,
                viewHooks: null,
                viewCheckHooks: null,
                destroyHooks: null,
                cleanup: null,
                contentQueries: null,
                components: null,
                directiveRegistry: "function" == typeof o ? o() : o,
                pipeRegistry: "function" == typeof a ? a() : a,
                firstChild: null,
                schemas: s,
                consts: d,
                incompleteFirstPass: !1
            }
        }
        function ti(t, e, n) {
            for (var r in t)
                if (t.hasOwnProperty(r)) {
                    var i = t[r];
                    (n = null === n ? {} : n).hasOwnProperty(r) ? n[r].push(e, i) : n[r] = [e, i]
                }
            return n
        }
        function ei(t, e, n, r) {
            var i = !1;
            if (cn()) {
                var o = function(t, e, n) {
                    var r = t.directiveRegistry
                      , i = null;
                    if (r)
                        for (var o = 0; o < r.length; o++) {
                            var a = r[o];
                            Ir(n, a.selectors, !1) && (i || (i = []),
                            ur(rr(n, e), t, a.type),
                            qe(a) ? (ai(t, n),
                            i.unshift(a)) : i.push(a))
                        }
                    return i
                }(t, e, n)
                  , a = null === r ? null : {
                    "": -1
                };
                if (null !== o) {
                    var u = 0;
                    i = !0,
                    si(n, t.data.length, o.length);
                    for (var s = 0; s < o.length; s++) {
                        var l = o[s];
                        l.providersResolver && l.providersResolver(l)
                    }
                    oi(t, n, o.length);
                    for (var c = !1, h = !1, f = 0; f < o.length; f++) {
                        var d = o[f];
                        n.mergedAttrs = Zn(n.mergedAttrs, d.hostAttrs),
                        li(t, e, d),
                        ui(t.data.length - 1, d, a),
                        null !== d.contentQueries && (n.flags |= 8),
                        null === d.hostBindings && null === d.hostAttrs && 0 === d.hostVars || (n.flags |= 128);
                        var v = d.type.prototype;
                        !c && (v.ngOnChanges || v.ngOnInit || v.ngDoCheck) && ((t.preOrderHooks || (t.preOrderHooks = [])).push(n.index - Ue),
                        c = !0),
                        h || !v.ngOnChanges && !v.ngDoCheck || ((t.preOrderCheckHooks || (t.preOrderCheckHooks = [])).push(n.index - Ue),
                        h = !0),
                        ni(t, d),
                        u += d.hostVars
                    }
                    !function(t, e) {
                        for (var n = e.directiveEnd, r = t.data, i = e.attrs, o = [], a = null, u = null, s = e.directiveStart; s < n; s++) {
                            var l = r[s]
                              , c = l.inputs
                              , h = null === i || Tr(e) ? null : fi(c, i);
                            o.push(h),
                            a = ti(c, s, a),
                            u = ti(l.outputs, s, u)
                        }
                        null !== a && (a.hasOwnProperty("class") && (e.flags |= 16),
                        a.hasOwnProperty("style") && (e.flags |= 32)),
                        e.initialInputs = o,
                        e.inputs = a,
                        e.outputs = u
                    }(t, n),
                    ri(t, e, u)
                }
                a && function(t, e, n) {
                    if (e)
                        for (var r = t.localNames = [], i = 0; i < e.length; i += 2) {
                            var o = n[e[i + 1]];
                            if (null == o)
                                throw new Error("Export of name '".concat(e[i + 1], "' not found!"));
                            r.push(e[i], o)
                        }
                }(n, r, a)
            }
            return n.mergedAttrs = Zn(n.mergedAttrs, n.attrs),
            i
        }
        function ni(t, e) {
            var n = t.expandoInstructions;
            n.push(e.hostBindings),
            0 !== e.hostVars && n.push(e.hostVars)
        }
        function ri(t, e, n) {
            for (var r = 0; r < n; r++)
                e.push(Mr),
                t.blueprint.push(Mr),
                t.data.push(null)
        }
        function ii(t, e) {
            null !== t.hostBindings && t.hostBindings(1, e)
        }
        function oi(t, e, n) {
            var r = Ue - e.index
              , i = t.data.length - (1048575 & e.providerIndexes);
            (t.expandoInstructions || (t.expandoInstructions = [])).push(r, i, n)
        }
        function ai(t, e) {
            e.flags |= 2,
            (t.components || (t.components = [])).push(e.index)
        }
        function ui(t, e, n) {
            if (n) {
                if (e.exportAs)
                    for (var r = 0; r < e.exportAs.length; r++)
                        n[e.exportAs[r]] = t;
                qe(e) && (n[""] = t)
            }
        }
        function si(t, e, n) {
            t.flags |= 1,
            t.directiveStart = e,
            t.directiveEnd = e + n,
            t.providerIndexes = e
        }
        function li(t, e, n) {
            t.data.push(n);
            var r = n.factory || (n.factory = Ne(n.type))
              , i = new Vn(r,qe(n),null);
            t.blueprint.push(i),
            e.push(i)
        }
        function ci(t, e, n) {
            var r = tn(e, t)
              , i = Xr(n)
              , o = t[10]
              , a = gi(t, Br(t, i, null, n.onPush ? 64 : 16, r, e, o, o.createRenderer(r, n), null, null));
            t[e.index] = a
        }
        function hi(t, e, n, r, i, o) {
            var a = o[e];
            if (null !== a)
                for (var u = r.setInput, s = 0; s < a.length; ) {
                    var l = a[s++]
                      , c = a[s++]
                      , h = a[s++];
                    null !== u ? r.setInput(n, h, l, c) : n[c] = h
                }
        }
        function fi(t, e) {
            for (var n = null, r = 0; r < e.length; ) {
                var i = e[r];
                if (0 !== i)
                    if (5 !== i) {
                        if ("number" == typeof i)
                            break;
                        t.hasOwnProperty(i) && (null === n && (n = []),
                        n.push(i, t[i], e[r + 1])),
                        r += 2
                    } else
                        r += 2;
                else
                    r += 4
            }
            return n
        }
        function di(t, e, n, r) {
            return new Array(t,!0,!1,e,null,0,r,n,null,null)
        }
        function vi(t, e) {
            var n = nn(e, t);
            if (on(n)) {
                var r = n[1];
                80 & n[2] ? Gr(r, n, r.template, n[8]) : n[5] > 0 && function t(e) {
                    for (var n = Dr(e); null !== n; n = Hr(n))
                        for (var r = De; r < n.length; r++) {
                            var i = n[r];
                            if (1024 & i[2]) {
                                var o = i[1];
                                Gr(o, i, o.template, i[8])
                            } else
                                i[5] > 0 && t(i)
                        }
                    var a = e[1].components;
                    if (null !== a)
                        for (var u = 0; u < a.length; u++) {
                            var s = nn(a[u], e);
                            on(s) && s[5] > 0 && t(s)
                        }
                }(n)
            }
        }
        function pi(t, e) {
            var n = nn(e, t)
              , r = n[1];
            !function(t, e) {
                for (var n = e.length; n < t.blueprint.length; n++)
                    e.push(t.blueprint[n])
            }(r, n),
            Wr(r, n, n[8])
        }
        function gi(t, e) {
            return t[13] ? t[14][4] = e : t[13] = e,
            t[14] = e,
            e
        }
        function yi(t) {
            for (; t; ) {
                t[2] |= 64;
                var e = Ur(t);
                if (Be(t) && !e)
                    return t;
                t = e
            }
            return null
        }
        function mi(t, e, n) {
            var r = e[10];
            r.begin && r.begin();
            try {
                Gr(t, e, t.template, n)
            } catch (i) {
                throw Ci(e, i),
                i
            } finally {
                r.end && r.end()
            }
        }
        function wi(t) {
            !function(t) {
                for (var e = 0; e < t.components.length; e++) {
                    var n = t.components[e]
                      , r = rn(n)
                      , i = r[1];
                    Qr(i, r, i.template, n)
                }
            }(t[8])
        }
        function _i(t, e, n) {
            kn(0),
            e(t, n)
        }
        var ki = function() {
            return Promise.resolve(null)
        }();
        function bi(t) {
            return t[7] || (t[7] = [])
        }
        function Ci(t, e) {
            var n = t[9]
              , r = n ? n.get(wr, null) : null;
            r && r.handleError(e)
        }
        function Si(t, e, n, r, i) {
            for (var o = 0; o < n.length; ) {
                var a = n[o++]
                  , u = n[o++]
                  , s = e[a]
                  , l = t.data[a];
                null !== l.setInput ? l.setInput(s, i, r, u) : s[u] = i
            }
        }
        function xi(t, e, n, r, i) {
            if (null != r) {
                var o, a = !1;
                Le(r) ? o = r : He(r) && (a = !0,
                r = r[0]);
                var u = $e(r);
                0 === t && null !== n ? null == i ? Ii(e, n, u) : Pi(e, n, u, i || null) : 1 === t && null !== n ? Pi(e, n, u, i || null) : 2 === t ? function(t, e, n) {
                    var r = Ni(t, e);
                    r && function(t, e, n, r) {
                        Ye(t) ? t.removeChild(e, n, r) : e.removeChild(n)
                    }(t, r, e, n)
                }(e, u, a) : 3 === t && e.destroyNode(u),
                null != o && function(t, e, n, r, i) {
                    var o = n[7];
                    o !== $e(n) && xi(e, t, r, o, i);
                    for (var a = De; a < n.length; a++) {
                        var u = n[a];
                        Di(u[1], u, t, e, r, o)
                    }
                }(e, t, o, n, i)
            }
        }
        function Ei(t, e) {
            return Ye(e) ? e.createText(t) : e.createTextNode(t)
        }
        function Ti(t, e) {
            var n = t[9]
              , r = n.indexOf(e)
              , i = e[3];
            1024 & e[2] && (e[2] &= -1025,
            sn(i, -1)),
            n.splice(r, 1)
        }
        function Oi(t, e) {
            if (!(t.length <= De)) {
                var n, r = De + e, i = t[r];
                if (i) {
                    var o = i[17];
                    null !== o && o !== t && Ti(o, i),
                    e > 0 && (t[r - 1][4] = i[4]);
                    var a = _e(t, De + e);
                    Di(i[1], n = i, n[11], 2, null, null),
                    n[0] = null,
                    n[6] = null;
                    var u = a[19];
                    null !== u && u.detachView(a[1]),
                    i[3] = null,
                    i[4] = null,
                    i[2] &= -129
                }
                return i
            }
        }
        function Ai(t, e) {
            if (!(256 & e[2])) {
                var n = e[11];
                Ye(n) && n.destroyNode && Di(t, e, n, 3, null, null),
                function(t) {
                    var e = t[13];
                    if (!e)
                        return Ri(t[1], t);
                    for (; e; ) {
                        var n = null;
                        if (He(e))
                            n = e[13];
                        else {
                            var r = e[10];
                            r && (n = r)
                        }
                        if (!n) {
                            for (; e && !e[4] && e !== t; )
                                He(e) && Ri(e[1], e),
                                e = e[3];
                            null === e && (e = t),
                            He(e) && Ri(e[1], e),
                            n = e && e[4]
                        }
                        e = n
                    }
                }(e)
            }
        }
        function Ri(t, e) {
            if (!(256 & e[2])) {
                e[2] &= -129,
                e[2] |= 256,
                function(t, e) {
                    var n;
                    if (null != t && null != (n = t.destroyHooks))
                        for (var r = 0; r < n.length; r += 2) {
                            var i = e[n[r]];
                            if (!(i instanceof Vn)) {
                                var o = n[r + 1];
                                if (Array.isArray(o))
                                    for (var a = 0; a < o.length; a += 2)
                                        o[a + 1].call(i[o[a]]);
                                else
                                    o.call(i)
                            }
                        }
                }(t, e),
                function(t, e) {
                    var n = t.cleanup;
                    if (null !== n) {
                        for (var r = e[7], i = 0; i < n.length - 1; i += 2)
                            if ("string" == typeof n[i]) {
                                var o = n[i + 1]
                                  , a = "function" == typeof o ? o(e) : $e(e[o])
                                  , u = n[i + 3];
                                "boolean" == typeof u ? a.removeEventListener(n[i], r[n[i + 2]], u) : u >= 0 ? r[u]() : r[-u].unsubscribe(),
                                i += 2
                            } else
                                n[i].call(r[n[i + 1]]);
                        e[7] = null
                    }
                }(t, e),
                1 === e[1].type && Ye(e[11]) && e[11].destroy();
                var n = e[17];
                if (null !== n && Le(e[3])) {
                    n !== e[3] && Ti(n, e);
                    var r = e[19];
                    null !== r && r.detachView(t)
                }
            }
        }
        function Pi(t, e, n, r) {
            Ye(t) ? t.insertBefore(e, n, r) : e.insertBefore(n, r, !0)
        }
        function Ii(t, e, n) {
            Ye(t) ? t.appendChild(e, n) : e.appendChild(n)
        }
        function ji(t, e, n, r) {
            null !== r ? Pi(t, e, n, r) : Ii(t, e, n)
        }
        function Ni(t, e) {
            return Ye(t) ? t.parentNode(e) : e.parentNode
        }
        function Mi(t, e, n, r) {
            var i = function(t, e, n) {
                for (var r = e.parent; null != r && (3 === r.type || 4 === r.type); )
                    r = (e = r).parent;
                if (null === r)
                    return n[0];
                if (e && 4 === e.type && 4 & e.flags)
                    return tn(e, n).parentNode;
                if (2 & r.flags) {
                    var i = t.data
                      , o = i[i[r.index].directiveStart].encapsulation;
                    if (o !== be.ShadowDom && o !== be.Native)
                        return null
                }
                return tn(r, n)
            }(t, r, e);
            if (null != i) {
                var o = e[11]
                  , a = function(t, e) {
                    return 3 === t.type || 4 === t.type ? tn(t, e) : null
                }(r.parent || e[6], e);
                if (Array.isArray(n))
                    for (var u = 0; u < n.length; u++)
                        ji(o, i, n[u], a);
                else
                    ji(o, i, n, a)
            }
        }
        function Ui(t, e, n, r, i, o, a) {
            for (; null != n; ) {
                var u = r[n.index]
                  , s = n.type;
                a && 0 === e && (u && Cr($e(u), r),
                n.flags |= 4),
                64 != (64 & n.flags) && (3 === s || 4 === s ? (Ui(t, e, n.child, r, i, o, !1),
                xi(e, t, i, u, o)) : 1 === s ? Hi(t, e, r, n, i, o) : xi(e, t, i, u, o)),
                n = a ? n.projectionNext : n.next
            }
        }
        function Di(t, e, n, r, i, o) {
            Ui(n, r, t.firstChild, e, i, o, !1)
        }
        function Hi(t, e, n, r, i, o) {
            var a = n[16]
              , u = a[6].projection[r.projection];
            if (Array.isArray(u))
                for (var s = 0; s < u.length; s++)
                    xi(e, t, i, u[s], o);
            else
                Ui(t, e, u, a[3], i, o, !0)
        }
        function Li(t, e, n) {
            Ye(t) ? t.setAttribute(e, "style", n) : e.style.cssText = n
        }
        function Fi(t, e, n) {
            Ye(t) ? "" === n ? t.removeAttribute(e, "class") : t.setAttribute(e, "class", n) : e.className = n
        }
        var Vi, zi, qi, Bi = function() {
            function t(e, n) {
                m(this, t),
                this._lView = e,
                this._cdRefInjectingView = n,
                this._appRef = null,
                this._viewContainerRef = null
            }
            return _(t, [{
                key: "destroy",
                value: function() {
                    if (this._appRef)
                        this._appRef.detachView(this);
                    else if (this._viewContainerRef) {
                        var t = this._viewContainerRef.indexOf(this);
                        t > -1 && this._viewContainerRef.detach(t),
                        this._viewContainerRef = null
                    }
                    Ai(this._lView[1], this._lView)
                }
            }, {
                key: "onDestroy",
                value: function(t) {
                    var e, n, r;
                    e = this._lView[1],
                    n = t,
                    (r = bi(this._lView)).push(null),
                    e.firstCreatePass && function(t) {
                        return t.cleanup || (t.cleanup = [])
                    }(e).push(n, r.length - 1)
                }
            }, {
                key: "markForCheck",
                value: function() {
                    yi(this._cdRefInjectingView || this._lView)
                }
            }, {
                key: "detach",
                value: function() {
                    this._lView[2] &= -129
                }
            }, {
                key: "reattach",
                value: function() {
                    this._lView[2] |= 128
                }
            }, {
                key: "detectChanges",
                value: function() {
                    mi(this._lView[1], this._lView, this.context)
                }
            }, {
                key: "checkNoChanges",
                value: function() {
                    !function(t, e, n) {
                        yn(!0);
                        try {
                            mi(t, e, n)
                        } finally {
                            yn(!1)
                        }
                    }(this._lView[1], this._lView, this.context)
                }
            }, {
                key: "attachToViewContainerRef",
                value: function(t) {
                    if (this._appRef)
                        throw new Error("This view is already attached directly to the ApplicationRef!");
                    this._viewContainerRef = t
                }
            }, {
                key: "detachFromAppRef",
                value: function() {
                    var t;
                    this._appRef = null,
                    Di(this._lView[1], t = this._lView, t[11], 2, null, null)
                }
            }, {
                key: "attachToAppRef",
                value: function(t) {
                    if (this._viewContainerRef)
                        throw new Error("This view is already attached to a ViewContainer!");
                    this._appRef = t
                }
            }, {
                key: "rootNodes",
                get: function() {
                    var t = this._lView
                      , e = t[1];
                    return function t(e, n, r, i) {
                        for (var o = arguments.length > 4 && void 0 !== arguments[4] && arguments[4]; null !== r; ) {
                            var a = n[r.index];
                            if (null !== a && i.push($e(a)),
                            Le(a))
                                for (var u = De; u < a.length; u++) {
                                    var s = a[u]
                                      , c = s[1].firstChild;
                                    null !== c && t(s[1], s, c, i)
                                }
                            var h = r.type;
                            if (3 === h || 4 === h)
                                t(e, n, r.child, i);
                            else if (1 === h) {
                                var f = n[16]
                                  , d = f[6]
                                  , v = d.projection[r.projection];
                                if (Array.isArray(v))
                                    i.push.apply(i, l(v));
                                else {
                                    var p = Ur(f);
                                    t(p[1], p, v, i, !0)
                                }
                            }
                            r = o ? r.projectionNext : r.next
                        }
                        return i
                    }(e, t, e.firstChild, [])
                }
            }, {
                key: "context",
                get: function() {
                    return this._lView[8]
                }
            }, {
                key: "destroyed",
                get: function() {
                    return 256 == (256 & this._lView[2])
                }
            }]),
            t
        }(), Zi = function(t) {
            d(n, t);
            var e = y(n);
            function n(t) {
                var r;
                return m(this, n),
                (r = e.call(this, t))._view = t,
                r
            }
            return _(n, [{
                key: "detectChanges",
                value: function() {
                    wi(this._view)
                }
            }, {
                key: "checkNoChanges",
                value: function() {
                    !function(t) {
                        yn(!0);
                        try {
                            wi(t)
                        } finally {
                            yn(!1)
                        }
                    }(this._view)
                }
            }, {
                key: "context",
                get: function() {
                    return null
                }
            }]),
            n
        }(Bi);
        function Wi(t, e, n) {
            return Vi || (Vi = function(t) {
                d(n, t);
                var e = y(n);
                function n() {
                    return m(this, n),
                    e.apply(this, arguments)
                }
                return n
            }(t)),
            new Vi(tn(e, n))
        }
        function Gi(t, e, n) {
            if (!n && Ve(t)) {
                var r = nn(t.index, e);
                return new Bi(r,r)
            }
            return 2 === t.type || 0 === t.type || 3 === t.type || 4 === t.type ? new Bi(e[16],e) : null
        }
        var Qi = function() {
            var t = function t() {
                m(this, t)
            };
            return t.__NG_ELEMENT_ID__ = function() {
                return Ji()
            }
            ,
            t
        }()
          , Ji = function() {
            var t = arguments.length > 0 && void 0 !== arguments[0] && arguments[0];
            return Gi(dn(), hn(), t)
        }
          , Ki = Function
          , Yi = new ee("Set Injector scope.")
          , Xi = {}
          , $i = {}
          , to = []
          , eo = void 0;
        function no() {
            return void 0 === eo && (eo = new ve),
            eo
        }
        function ro(t) {
            var e = arguments.length > 1 && void 0 !== arguments[1] ? arguments[1] : null
              , n = arguments.length > 2 && void 0 !== arguments[2] ? arguments[2] : null
              , r = arguments.length > 3 ? arguments[3] : void 0;
            return new io(t,n,e || no(),r)
        }
        var io = function() {
            function t(e, n, r) {
                var i = this
                  , o = arguments.length > 3 && void 0 !== arguments[3] ? arguments[3] : null;
                m(this, t),
                this.parent = r,
                this.records = new Map,
                this.injectorDefTypes = new Set,
                this.onDestroy = new Set,
                this._destroyed = !1;
                var a = [];
                n && me(n, (function(t) {
                    return i.processProvider(t, e, n)
                }
                )),
                me([e], (function(t) {
                    return i.processInjectorType(t, [], a)
                }
                )),
                this.records.set(ne, ao(void 0, this));
                var u = this.records.get(Yi);
                this.scope = null != u ? u.value : null,
                this.source = o || ("object" == typeof e ? null : Dt(e))
            }
            return _(t, [{
                key: "destroy",
                value: function() {
                    this.assertNotDestroyed(),
                    this._destroyed = !0;
                    try {
                        this.onDestroy.forEach((function(t) {
                            return t.ngOnDestroy()
                        }
                        ))
                    } finally {
                        this.records.clear(),
                        this.onDestroy.clear(),
                        this.injectorDefTypes.clear()
                    }
                }
            }, {
                key: "get",
                value: function(t) {
                    var e = arguments.length > 1 && void 0 !== arguments[1] ? arguments[1] : re
                      , n = arguments.length > 2 && void 0 !== arguments[2] ? arguments[2] : xt.Default;
                    this.assertNotDestroyed();
                    var r = se(this);
                    try {
                        if (!(n & xt.SkipSelf)) {
                            var i = this.records.get(t);
                            if (void 0 === i) {
                                var o = lo(t) && At(t);
                                i = o && this.injectableDefInScope(o) ? ao(oo(t), Xi) : null,
                                this.records.set(t, i)
                            }
                            if (null != i)
                                return this.hydrate(t, i)
                        }
                        var a = n & xt.Self ? no() : this.parent;
                        return a.get(t, e = n & xt.Optional && e === re ? null : e)
                    } catch (s) {
                        if ("NullInjectorError" === s.name) {
                            var u = s.ngTempTokenPath = s.ngTempTokenPath || [];
                            if (u.unshift(Dt(t)),
                            r)
                                throw s;
                            return pe(s, t, "R3InjectorError", this.source)
                        }
                        throw s
                    } finally {
                        se(r)
                    }
                }
            }, {
                key: "_resolveInjectorDefTypes",
                value: function() {
                    var t = this;
                    this.injectorDefTypes.forEach((function(e) {
                        return t.get(e)
                    }
                    ))
                }
            }, {
                key: "toString",
                value: function() {
                    var t = [];
                    return this.records.forEach((function(e, n) {
                        return t.push(Dt(n))
                    }
                    )),
                    "R3Injector[".concat(t.join(", "), "]")
                }
            }, {
                key: "assertNotDestroyed",
                value: function() {
                    if (this._destroyed)
                        throw new Error("Injector has already been destroyed.")
                }
            }, {
                key: "processInjectorType",
                value: function(t, e, n) {
                    var r = this;
                    if (!(t = Vt(t)))
                        return !1;
                    var i = Pt(t)
                      , o = null == i && t.ngModule || void 0
                      , a = void 0 === o ? t : o
                      , u = -1 !== n.indexOf(a);
                    if (void 0 !== o && (i = Pt(o)),
                    null == i)
                        return !1;
                    if (null != i.imports && !u) {
                        var s;
                        n.push(a);
                        try {
                            me(i.imports, (function(t) {
                                r.processInjectorType(t, e, n) && (void 0 === s && (s = []),
                                s.push(t))
                            }
                            ))
                        } finally {}
                        if (void 0 !== s)
                            for (var l = function(t) {
                                var e = s[t]
                                  , n = e.ngModule
                                  , i = e.providers;
                                me(i, (function(t) {
                                    return r.processProvider(t, n, i || to)
                                }
                                ))
                            }, c = 0; c < s.length; c++)
                                l(c)
                    }
                    this.injectorDefTypes.add(a),
                    this.records.set(a, ao(i.factory, Xi));
                    var h = i.providers;
                    if (null != h && !u) {
                        var f = t;
                        me(h, (function(t) {
                            return r.processProvider(t, f, h)
                        }
                        ))
                    }
                    return void 0 !== o && void 0 !== t.providers
                }
            }, {
                key: "processProvider",
                value: function(t, e, n) {
                    var r = so(t = Vt(t)) ? t : Vt(t && t.provide)
                      , i = function(t, e, n) {
                        return uo(t) ? ao(void 0, t.useValue) : ao(function(t, e, n) {
                            var r, i = void 0;
                            if (so(t)) {
                                var o = Vt(t);
                                return Ne(o) || oo(o)
                            }
                            if (uo(t))
                                i = function() {
                                    return Vt(t.useValue)
                                }
                                ;
                            else if ((r = t) && r.useFactory)
                                i = function() {
                                    return t.useFactory.apply(t, l(de(t.deps || [])))
                                }
                                ;
                            else if (function(t) {
                                return !(!t || !t.useExisting)
                            }(t))
                                i = function() {
                                    return he(Vt(t.useExisting))
                                }
                                ;
                            else {
                                var a = Vt(t && (t.useClass || t.provide));
                                if (!function(t) {
                                    return !!t.deps
                                }(t))
                                    return Ne(a) || oo(a);
                                i = function() {
                                    return k(a, l(de(t.deps)))
                                }
                            }
                            return i
                        }(t), Xi)
                    }(t);
                    if (so(t) || !0 !== t.multi)
                        this.records.get(r);
                    else {
                        var o = this.records.get(r);
                        o || ((o = ao(void 0, Xi, !0)).factory = function() {
                            return de(o.multi)
                        }
                        ,
                        this.records.set(r, o)),
                        r = t,
                        o.multi.push(t)
                    }
                    this.records.set(r, i)
                }
            }, {
                key: "hydrate",
                value: function(t, e) {
                    var n;
                    return e.value === Xi && (e.value = $i,
                    e.value = e.factory()),
                    "object" == typeof e.value && e.value && null !== (n = e.value) && "object" == typeof n && "function" == typeof n.ngOnDestroy && this.onDestroy.add(e.value),
                    e.value
                }
            }, {
                key: "injectableDefInScope",
                value: function(t) {
                    return !!t.providedIn && ("string" == typeof t.providedIn ? "any" === t.providedIn || t.providedIn === this.scope : this.injectorDefTypes.has(t.providedIn))
                }
            }, {
                key: "destroyed",
                get: function() {
                    return this._destroyed
                }
            }]),
            t
        }();
        function oo(t) {
            var e = At(t)
              , n = null !== e ? e.factory : Ne(t);
            if (null !== n)
                return n;
            var r = Pt(t);
            if (null !== r)
                return r.factory;
            if (t instanceof ee)
                throw new Error("Token ".concat(Dt(t), " is missing a \u0275prov definition."));
            if (t instanceof Function)
                return function(t) {
                    var e = t.length;
                    if (e > 0) {
                        var n = function(t, e) {
                            for (var n = [], r = 0; r < t; r++)
                                n.push("?");
                            return n
                        }(e);
                        throw new Error("Can't resolve all parameters for ".concat(Dt(t), ": (").concat(n.join(", "), ")."))
                    }
                    var r = function(t) {
                        var e = t && (t[It] || t[Mt] || t[Nt] && t[Nt]());
                        if (e) {
                            var n = function(t) {
                                if (t.hasOwnProperty("name"))
                                    return t.name;
                                var e = ("" + t).match(/^function\s*([^\s(]+)/);
                                return null === e ? "" : e[1]
                            }(t);
                            return console.warn('DEPRECATED: DI is instantiating a token "'.concat(n, '" that inherits its @Injectable decorator but does not provide one itself.\n') + 'This will become an error in a future version of Angular. Please add @Injectable() to the "'.concat(n, '" class.')),
                            e
                        }
                        return null
                    }(t);
                    return null !== r ? function() {
                        return r.factory(t)
                    }
                    : function() {
                        return new t
                    }
                }(t);
            throw new Error("unreachable")
        }
        function ao(t, e) {
            var n = arguments.length > 2 && void 0 !== arguments[2] && arguments[2];
            return {
                factory: t,
                value: e,
                multi: n ? [] : void 0
            }
        }
        function uo(t) {
            return null !== t && "object" == typeof t && ae in t
        }
        function so(t) {
            return "function" == typeof t
        }
        function lo(t) {
            return "function" == typeof t || "object" == typeof t && t instanceof ee
        }
        var co = function(t, e, n) {
            return function(t) {
                var e = ro(t, arguments.length > 1 && void 0 !== arguments[1] ? arguments[1] : null, arguments.length > 2 && void 0 !== arguments[2] ? arguments[2] : null, arguments.length > 3 ? arguments[3] : void 0);
                return e._resolveInjectorDefTypes(),
                e
            }({
                name: n
            }, e, t, n)
        }
          , ho = function() {
            var t = function() {
                function t() {
                    m(this, t)
                }
                return _(t, null, [{
                    key: "create",
                    value: function(t, e) {
                        return Array.isArray(t) ? co(t, e, "") : co(t.providers, t.parent, t.name || "")
                    }
                }]),
                t
            }();
            return t.THROW_IF_NOT_FOUND = re,
            t.NULL = new ve,
            t.\u0275prov = Tt({
                token: t,
                providedIn: "any",
                factory: function() {
                    return he(ne)
                }
            }),
            t.__NG_ELEMENT_ID__ = -1,
            t
        }()
          , fo = new ee("AnalyzeForEntryComponents");
        function vo(t, e, n) {
            var r = n ? t.styles : null
              , i = n ? t.classes : null
              , o = 0;
            if (null !== e)
                for (var a = 0; a < e.length; a++) {
                    var u = e[a];
                    "number" == typeof u ? o = u : 1 == o ? i = Ht(i, u) : 2 == o && (r = Ht(r, u + ": " + e[++a] + ";"))
                }
            n ? t.styles = r : t.stylesWithoutHost = r,
            n ? t.classes = i : t.classesWithoutHost = i
        }
        function po(t, e) {
            var n = rn(t)[1]
              , r = n.data.length - 1;
            Nn(n, {
                directiveStart: r,
                directiveEnd: r + 1
            })
        }
        var go = null;
        function yo() {
            if (!go) {
                var t = Wt.Symbol;
                if (t && t.iterator)
                    go = t.iterator;
                else
                    for (var e = Object.getOwnPropertyNames(Map.prototype), n = 0; n < e.length; ++n) {
                        var r = e[n];
                        "entries" !== r && "size" !== r && Map.prototype[r] === Map.prototype.entries && (go = r)
                    }
            }
            return go
        }
        function mo(t) {
            return !!wo(t) && (Array.isArray(t) || !(t instanceof Map) && yo()in t)
        }
        function wo(t) {
            return null !== t && ("function" == typeof t || "object" == typeof t)
        }
        function _o(t, e, n) {
            return !Object.is(t[e], n) && (t[e] = n,
            !0)
        }
        function ko(t) {
            var e = arguments.length > 1 && void 0 !== arguments[1] ? arguments[1] : xt.Default
              , n = hn();
            if (null === n)
                return he(t, e);
            var r = dn();
            return sr(r, n, Vt(t), e)
        }
        function bo(t, e, n) {
            var r, i = hn();
            return _o(i, mn(), e) && function(t, e, n, r, i, o, a, u) {
                var s, l, c = tn(e, n), h = e.inputs;
                null != h && (s = h[r]) ? (Si(t, n, s, r, i),
                Ve(e) && function(t, e) {
                    var n = nn(e, t);
                    16 & n[2] || (n[2] |= 64)
                }(n, e.index)) : 2 === e.type && (r = "class" === (l = r) ? "className" : "for" === l ? "htmlFor" : "formaction" === l ? "formAction" : "innerHtml" === l ? "innerHTML" : "readonly" === l ? "readOnly" : "tabindex" === l ? "tabIndex" : l,
                i = null != a ? a(i, e.tagName || "", r) : i,
                Ye(o) ? o.setProperty(c, r, i) : Bn(r) || (c.setProperty ? c.setProperty(r, i) : c[r] = i))
            }(fn(), en((r = ln.lFrame).tView, r.selectedIndex), i, t, e, i[11], n),
            bo
        }
        function Co(t, e, n, r, i) {
            var o = i ? "class" : "style";
            Si(t, n, e.inputs[o], o, r)
        }
        function So(t, e, n, r) {
            var i = hn()
              , o = fn()
              , a = Ue + t
              , u = i[11]
              , s = i[a] = qr(e, u, ln.lFrame.currentNamespace)
              , l = o.firstCreatePass ? function(t, e, n, r, i, o, a) {
                var u = e.consts
                  , s = Zr(e, t, 2, i, an(u, o));
                return ei(e, n, s, an(u, a)),
                null !== s.attrs && vo(s, s.attrs, !1),
                null !== s.mergedAttrs && vo(s, s.mergedAttrs, !0),
                null !== e.queries && e.queries.elementStart(e, s),
                s
            }(t, o, i, 0, e, n, r) : o.data[a];
            vn(l, !0);
            var c = l.mergedAttrs;
            null !== c && zn(u, s, c);
            var h = l.classes;
            null !== h && Fi(u, s, h);
            var f = l.styles;
            null !== f && Li(u, s, f),
            Mi(o, i, s, l),
            0 === ln.lFrame.elementDepthCount && Cr(s, i),
            ln.lFrame.elementDepthCount++,
            ze(l) && (Kr(o, i, l),
            function(t, e, n) {
                if (Fe(e))
                    for (var r = e.directiveEnd, i = e.directiveStart; i < r; i++) {
                        var o = t.data[i];
                        o.contentQueries && o.contentQueries(1, n[i], i)
                    }
            }(o, l, i)),
            null !== r && Yr(i, l)
        }
        function xo() {
            var t = dn();
            pn() ? ln.lFrame.isParent = !1 : vn(t = t.parent, !1);
            var e = t;
            ln.lFrame.elementDepthCount--;
            var n = fn();
            n.firstCreatePass && (Nn(n, t),
            Fe(t) && n.queries.elementEnd(t)),
            null != e.classesWithoutHost && function(t) {
                return 0 != (16 & t.flags)
            }(e) && Co(n, e, hn(), e.classesWithoutHost, !0),
            null != e.stylesWithoutHost && function(t) {
                return 0 != (32 & t.flags)
            }(e) && Co(n, e, hn(), e.stylesWithoutHost, !1)
        }
        function Eo(t, e, n, r) {
            So(t, e, n, r),
            xo()
        }
        function To(t) {
            return !!t && "function" == typeof t.then
        }
        function Oo(t, e) {
            var n = arguments.length > 2 && void 0 !== arguments[2] && arguments[2]
              , r = arguments.length > 3 ? arguments[3] : void 0
              , i = hn()
              , o = fn()
              , a = dn();
            return Ro(o, i, i[11], a, t, e, n, r),
            Oo
        }
        function Ao(t, e, n, r) {
            var i = t.cleanup;
            if (null != i)
                for (var o = 0; o < i.length - 1; o += 2) {
                    var a = i[o];
                    if (a === n && i[o + 1] === r) {
                        var u = e[7]
                          , s = i[o + 2];
                        return u.length > s ? u[s] : null
                    }
                    "string" == typeof a && (o += 2)
                }
            return null
        }
        function Ro(t, e, n, r, i, o) {
            var a = arguments.length > 6 && void 0 !== arguments[6] && arguments[6]
              , u = arguments.length > 7 ? arguments[7] : void 0
              , s = ze(r)
              , l = t.firstCreatePass
              , c = l && (t.cleanup || (t.cleanup = []))
              , h = bi(e)
              , f = !0;
            if (2 === r.type) {
                var d = tn(r, e)
                  , v = u ? u(d) : Ce
                  , p = v.target || d
                  , g = h.length
                  , y = u ? function(t) {
                    return u($e(t[r.index])).target
                }
                : r.index;
                if (Ye(n)) {
                    var m = null;
                    if (!u && s && (m = Ao(t, e, i, r.index)),
                    null !== m) {
                        var w = m.__ngLastListenerFn__ || m;
                        w.__ngNextListenerFn__ = o,
                        m.__ngLastListenerFn__ = o,
                        f = !1
                    } else {
                        o = Io(r, e, o, !1);
                        var _ = n.listen(v.name || p, i, o);
                        h.push(o, _),
                        c && c.push(i, y, g, g + 1)
                    }
                } else
                    o = Io(r, e, o, !0),
                    p.addEventListener(i, o, a),
                    h.push(o),
                    c && c.push(i, y, g, a)
            }
            var k, b = r.outputs;
            if (f && null !== b && (k = b[i])) {
                var C = k.length;
                if (C)
                    for (var S = 0; S < C; S += 2) {
                        var x = k[S]
                          , E = k[S + 1]
                          , T = e[x]
                          , O = T[E]
                          , A = O.subscribe(o)
                          , R = h.length;
                        h.push(o, A),
                        c && c.push(i, r.index, R, -(R + 1))
                    }
            }
        }
        function Po(t, e, n) {
            try {
                return !1 !== e(n)
            } catch (r) {
                return Ci(t, r),
                !1
            }
        }
        function Io(t, e, n, r) {
            return function i(o) {
                if (o === Function)
                    return n;
                var a = 2 & t.flags ? nn(t.index, e) : e;
                0 == (32 & e[2]) && yi(a);
                for (var u = Po(e, n, o), s = i.__ngNextListenerFn__; s; )
                    u = Po(e, s, o) && u,
                    s = s.__ngNextListenerFn__;
                return r && !1 === u && (o.preventDefault(),
                o.returnValue = !1),
                u
            }
        }
        function jo(t) {
            var e = arguments.length > 1 && void 0 !== arguments[1] ? arguments[1] : ""
              , n = hn()
              , r = fn()
              , i = t + Ue
              , o = r.firstCreatePass ? Zr(r, t, 2, null, null) : r.data[i]
              , a = n[i] = Ei(e, n[11]);
            Mi(r, n, a, o),
            vn(o, !1)
        }
        function No(t, e, n) {
            var r = hn()
              , i = function(t, e, n, r) {
                return _o(t, mn(), n) ? e + Kn(n) + r : Mr
            }(r, t, e, n);
            return i !== Mr && function(t, e, n) {
                var r = function(t, e) {
                    return $e(e[t + Ue])
                }(e, t)
                  , i = t[11];
                Ye(i) ? i.setValue(r, n) : r.textContent = n
            }(r, Rn(), i),
            No
        }
        var Mo = function t() {
            m(this, t)
        }
          , Uo = function t() {
            m(this, t)
        }
          , Do = function() {
            function t() {
                m(this, t)
            }
            return _(t, [{
                key: "resolveComponentFactory",
                value: function(t) {
                    throw function(t) {
                        var e = Error("No component factory found for ".concat(Dt(t), ". Did you add it to @NgModule.entryComponents?"));
                        return e.ngComponent = t,
                        e
                    }(t)
                }
            }]),
            t
        }()
          , Ho = function() {
            var t = function t() {
                m(this, t)
            };
            return t.NULL = new Do,
            t
        }()
          , Lo = function() {
            var t = function t(e) {
                m(this, t),
                this.nativeElement = e
            };
            return t.__NG_ELEMENT_ID__ = function() {
                return Fo(t)
            }
            ,
            t
        }()
          , Fo = function(t) {
            return Wi(t, dn(), hn())
        }
          , Vo = function t() {
            m(this, t)
        }
          , zo = function(t) {
            return t[t.Important = 1] = "Important",
            t[t.DashCase = 2] = "DashCase",
            t
        }({})
          , qo = function() {
            var t = function t() {
                m(this, t)
            };
            return t.\u0275prov = Tt({
                token: t,
                providedIn: "root",
                factory: function() {
                    return null
                }
            }),
            t
        }()
          , Bo = new function t(e) {
            m(this, t),
            this.full = e,
            this.major = e.split(".")[0],
            this.minor = e.split(".")[1],
            this.patch = e.split(".").slice(2).join(".")
        }
        ("10.1.6")
          , Zo = function() {
            function t() {
                m(this, t)
            }
            return _(t, [{
                key: "supports",
                value: function(t) {
                    return mo(t)
                }
            }, {
                key: "create",
                value: function(t) {
                    return new Go(t)
                }
            }]),
            t
        }()
          , Wo = function(t, e) {
            return e
        }
          , Go = function() {
            function t(e) {
                m(this, t),
                this.length = 0,
                this._linkedRecords = null,
                this._unlinkedRecords = null,
                this._previousItHead = null,
                this._itHead = null,
                this._itTail = null,
                this._additionsHead = null,
                this._additionsTail = null,
                this._movesHead = null,
                this._movesTail = null,
                this._removalsHead = null,
                this._removalsTail = null,
                this._identityChangesHead = null,
                this._identityChangesTail = null,
                this._trackByFn = e || Wo
            }
            return _(t, [{
                key: "forEachItem",
                value: function(t) {
                    var e;
                    for (e = this._itHead; null !== e; e = e._next)
                        t(e)
                }
            }, {
                key: "forEachOperation",
                value: function(t) {
                    for (var e = this._itHead, n = this._removalsHead, r = 0, i = null; e || n; ) {
                        var o = !n || e && e.currentIndex < Yo(n, r, i) ? e : n
                          , a = Yo(o, r, i)
                          , u = o.currentIndex;
                        if (o === n)
                            r--,
                            n = n._nextRemoved;
                        else if (e = e._next,
                        null == o.previousIndex)
                            r++;
                        else {
                            i || (i = []);
                            var s = a - r
                              , l = u - r;
                            if (s != l) {
                                for (var c = 0; c < s; c++) {
                                    var h = c < i.length ? i[c] : i[c] = 0
                                      , f = h + c;
                                    l <= f && f < s && (i[c] = h + 1)
                                }
                                i[o.previousIndex] = l - s
                            }
                        }
                        a !== u && t(o, a, u)
                    }
                }
            }, {
                key: "forEachPreviousItem",
                value: function(t) {
                    var e;
                    for (e = this._previousItHead; null !== e; e = e._nextPrevious)
                        t(e)
                }
            }, {
                key: "forEachAddedItem",
                value: function(t) {
                    var e;
                    for (e = this._additionsHead; null !== e; e = e._nextAdded)
                        t(e)
                }
            }, {
                key: "forEachMovedItem",
                value: function(t) {
                    var e;
                    for (e = this._movesHead; null !== e; e = e._nextMoved)
                        t(e)
                }
            }, {
                key: "forEachRemovedItem",
                value: function(t) {
                    var e;
                    for (e = this._removalsHead; null !== e; e = e._nextRemoved)
                        t(e)
                }
            }, {
                key: "forEachIdentityChange",
                value: function(t) {
                    var e;
                    for (e = this._identityChangesHead; null !== e; e = e._nextIdentityChange)
                        t(e)
                }
            }, {
                key: "diff",
                value: function(t) {
                    if (null == t && (t = []),
                    !mo(t))
                        throw new Error("Error trying to diff '".concat(Dt(t), "'. Only arrays and iterables are allowed"));
                    return this.check(t) ? this : null
                }
            }, {
                key: "onDestroy",
                value: function() {}
            }, {
                key: "check",
                value: function(t) {
                    var e = this;
                    this._reset();
                    var n, r, i, o = this._itHead, a = !1;
                    if (Array.isArray(t)) {
                        this.length = t.length;
                        for (var u = 0; u < this.length; u++)
                            i = this._trackByFn(u, r = t[u]),
                            null !== o && Object.is(o.trackById, i) ? (a && (o = this._verifyReinsertion(o, r, i, u)),
                            Object.is(o.item, r) || this._addIdentityChange(o, r)) : (o = this._mismatch(o, r, i, u),
                            a = !0),
                            o = o._next
                    } else
                        n = 0,
                        function(t, e) {
                            if (Array.isArray(t))
                                for (var n = 0; n < t.length; n++)
                                    e(t[n]);
                            else
                                for (var r, i = t[yo()](); !(r = i.next()).done; )
                                    e(r.value)
                        }(t, (function(t) {
                            i = e._trackByFn(n, t),
                            null !== o && Object.is(o.trackById, i) ? (a && (o = e._verifyReinsertion(o, t, i, n)),
                            Object.is(o.item, t) || e._addIdentityChange(o, t)) : (o = e._mismatch(o, t, i, n),
                            a = !0),
                            o = o._next,
                            n++
                        }
                        )),
                        this.length = n;
                    return this._truncate(o),
                    this.collection = t,
                    this.isDirty
                }
            }, {
                key: "_reset",
                value: function() {
                    if (this.isDirty) {
                        var t;
                        for (t = this._previousItHead = this._itHead; null !== t; t = t._next)
                            t._nextPrevious = t._next;
                        for (t = this._additionsHead; null !== t; t = t._nextAdded)
                            t.previousIndex = t.currentIndex;
                        for (this._additionsHead = this._additionsTail = null,
                        t = this._movesHead; null !== t; t = t._nextMoved)
                            t.previousIndex = t.currentIndex;
                        this._movesHead = this._movesTail = null,
                        this._removalsHead = this._removalsTail = null,
                        this._identityChangesHead = this._identityChangesTail = null
                    }
                }
            }, {
                key: "_mismatch",
                value: function(t, e, n, r) {
                    var i;
                    return null === t ? i = this._itTail : (i = t._prev,
                    this._remove(t)),
                    null !== (t = null === this._linkedRecords ? null : this._linkedRecords.get(n, r)) ? (Object.is(t.item, e) || this._addIdentityChange(t, e),
                    this._moveAfter(t, i, r)) : null !== (t = null === this._unlinkedRecords ? null : this._unlinkedRecords.get(n, null)) ? (Object.is(t.item, e) || this._addIdentityChange(t, e),
                    this._reinsertAfter(t, i, r)) : t = this._addAfter(new Qo(e,n), i, r),
                    t
                }
            }, {
                key: "_verifyReinsertion",
                value: function(t, e, n, r) {
                    var i = null === this._unlinkedRecords ? null : this._unlinkedRecords.get(n, null);
                    return null !== i ? t = this._reinsertAfter(i, t._prev, r) : t.currentIndex != r && (t.currentIndex = r,
                    this._addToMoves(t, r)),
                    t
                }
            }, {
                key: "_truncate",
                value: function(t) {
                    for (; null !== t; ) {
                        var e = t._next;
                        this._addToRemovals(this._unlink(t)),
                        t = e
                    }
                    null !== this._unlinkedRecords && this._unlinkedRecords.clear(),
                    null !== this._additionsTail && (this._additionsTail._nextAdded = null),
                    null !== this._movesTail && (this._movesTail._nextMoved = null),
                    null !== this._itTail && (this._itTail._next = null),
                    null !== this._removalsTail && (this._removalsTail._nextRemoved = null),
                    null !== this._identityChangesTail && (this._identityChangesTail._nextIdentityChange = null)
                }
            }, {
                key: "_reinsertAfter",
                value: function(t, e, n) {
                    null !== this._unlinkedRecords && this._unlinkedRecords.remove(t);
                    var r = t._prevRemoved
                      , i = t._nextRemoved;
                    return null === r ? this._removalsHead = i : r._nextRemoved = i,
                    null === i ? this._removalsTail = r : i._prevRemoved = r,
                    this._insertAfter(t, e, n),
                    this._addToMoves(t, n),
                    t
                }
            }, {
                key: "_moveAfter",
                value: function(t, e, n) {
                    return this._unlink(t),
                    this._insertAfter(t, e, n),
                    this._addToMoves(t, n),
                    t
                }
            }, {
                key: "_addAfter",
                value: function(t, e, n) {
                    return this._insertAfter(t, e, n),
                    this._additionsTail = null === this._additionsTail ? this._additionsHead = t : this._additionsTail._nextAdded = t,
                    t
                }
            }, {
                key: "_insertAfter",
                value: function(t, e, n) {
                    var r = null === e ? this._itHead : e._next;
                    return t._next = r,
                    t._prev = e,
                    null === r ? this._itTail = t : r._prev = t,
                    null === e ? this._itHead = t : e._next = t,
                    null === this._linkedRecords && (this._linkedRecords = new Ko),
                    this._linkedRecords.put(t),
                    t.currentIndex = n,
                    t
                }
            }, {
                key: "_remove",
                value: function(t) {
                    return this._addToRemovals(this._unlink(t))
                }
            }, {
                key: "_unlink",
                value: function(t) {
                    null !== this._linkedRecords && this._linkedRecords.remove(t);
                    var e = t._prev
                      , n = t._next;
                    return null === e ? this._itHead = n : e._next = n,
                    null === n ? this._itTail = e : n._prev = e,
                    t
                }
            }, {
                key: "_addToMoves",
                value: function(t, e) {
                    return t.previousIndex === e || (this._movesTail = null === this._movesTail ? this._movesHead = t : this._movesTail._nextMoved = t),
                    t
                }
            }, {
                key: "_addToRemovals",
                value: function(t) {
                    return null === this._unlinkedRecords && (this._unlinkedRecords = new Ko),
                    this._unlinkedRecords.put(t),
                    t.currentIndex = null,
                    t._nextRemoved = null,
                    null === this._removalsTail ? (this._removalsTail = this._removalsHead = t,
                    t._prevRemoved = null) : (t._prevRemoved = this._removalsTail,
                    this._removalsTail = this._removalsTail._nextRemoved = t),
                    t
                }
            }, {
                key: "_addIdentityChange",
                value: function(t, e) {
                    return t.item = e,
                    this._identityChangesTail = null === this._identityChangesTail ? this._identityChangesHead = t : this._identityChangesTail._nextIdentityChange = t,
                    t
                }
            }, {
                key: "isDirty",
                get: function() {
                    return null !== this._additionsHead || null !== this._movesHead || null !== this._removalsHead || null !== this._identityChangesHead
                }
            }]),
            t
        }()
          , Qo = function t(e, n) {
            m(this, t),
            this.item = e,
            this.trackById = n,
            this.currentIndex = null,
            this.previousIndex = null,
            this._nextPrevious = null,
            this._prev = null,
            this._next = null,
            this._prevDup = null,
            this._nextDup = null,
            this._prevRemoved = null,
            this._nextRemoved = null,
            this._nextAdded = null,
            this._nextMoved = null,
            this._nextIdentityChange = null
        }
          , Jo = function() {
            function t() {
                m(this, t),
                this._head = null,
                this._tail = null
            }
            return _(t, [{
                key: "add",
                value: function(t) {
                    null === this._head ? (this._head = this._tail = t,
                    t._nextDup = null,
                    t._prevDup = null) : (this._tail._nextDup = t,
                    t._prevDup = this._tail,
                    t._nextDup = null,
                    this._tail = t)
                }
            }, {
                key: "get",
                value: function(t, e) {
                    var n;
                    for (n = this._head; null !== n; n = n._nextDup)
                        if ((null === e || e <= n.currentIndex) && Object.is(n.trackById, t))
                            return n;
                    return null
                }
            }, {
                key: "remove",
                value: function(t) {
                    var e = t._prevDup
                      , n = t._nextDup;
                    return null === e ? this._head = n : e._nextDup = n,
                    null === n ? this._tail = e : n._prevDup = e,
                    null === this._head
                }
            }]),
            t
        }()
          , Ko = function() {
            function t() {
                m(this, t),
                this.map = new Map
            }
            return _(t, [{
                key: "put",
                value: function(t) {
                    var e = t.trackById
                      , n = this.map.get(e);
                    n || (n = new Jo,
                    this.map.set(e, n)),
                    n.add(t)
                }
            }, {
                key: "get",
                value: function(t, e) {
                    var n = this.map.get(t);
                    return n ? n.get(t, e) : null
                }
            }, {
                key: "remove",
                value: function(t) {
                    var e = t.trackById;
                    return this.map.get(e).remove(t) && this.map.delete(e),
                    t
                }
            }, {
                key: "clear",
                value: function() {
                    this.map.clear()
                }
            }, {
                key: "isEmpty",
                get: function() {
                    return 0 === this.map.size
                }
            }]),
            t
        }();
        function Yo(t, e, n) {
            var r = t.previousIndex;
            if (null === r)
                return r;
            var i = 0;
            return n && r < n.length && (i = n[r]),
            r + e + i
        }
        var Xo = function() {
            function t() {
                m(this, t)
            }
            return _(t, [{
                key: "supports",
                value: function(t) {
                    return t instanceof Map || wo(t)
                }
            }, {
                key: "create",
                value: function() {
                    return new $o
                }
            }]),
            t
        }()
          , $o = function() {
            function t() {
                m(this, t),
                this._records = new Map,
                this._mapHead = null,
                this._appendAfter = null,
                this._previousMapHead = null,
                this._changesHead = null,
                this._changesTail = null,
                this._additionsHead = null,
                this._additionsTail = null,
                this._removalsHead = null,
                this._removalsTail = null
            }
            return _(t, [{
                key: "forEachItem",
                value: function(t) {
                    var e;
                    for (e = this._mapHead; null !== e; e = e._next)
                        t(e)
                }
            }, {
                key: "forEachPreviousItem",
                value: function(t) {
                    var e;
                    for (e = this._previousMapHead; null !== e; e = e._nextPrevious)
                        t(e)
                }
            }, {
                key: "forEachChangedItem",
                value: function(t) {
                    var e;
                    for (e = this._changesHead; null !== e; e = e._nextChanged)
                        t(e)
                }
            }, {
                key: "forEachAddedItem",
                value: function(t) {
                    var e;
                    for (e = this._additionsHead; null !== e; e = e._nextAdded)
                        t(e)
                }
            }, {
                key: "forEachRemovedItem",
                value: function(t) {
                    var e;
                    for (e = this._removalsHead; null !== e; e = e._nextRemoved)
                        t(e)
                }
            }, {
                key: "diff",
                value: function(t) {
                    if (t) {
                        if (!(t instanceof Map || wo(t)))
                            throw new Error("Error trying to diff '".concat(Dt(t), "'. Only maps and objects are allowed"))
                    } else
                        t = new Map;
                    return this.check(t) ? this : null
                }
            }, {
                key: "onDestroy",
                value: function() {}
            }, {
                key: "check",
                value: function(t) {
                    var e = this;
                    this._reset();
                    var n = this._mapHead;
                    if (this._appendAfter = null,
                    this._forEach(t, (function(t, r) {
                        if (n && n.key === r)
                            e._maybeAddToChanges(n, t),
                            e._appendAfter = n,
                            n = n._next;
                        else {
                            var i = e._getOrCreateRecordForKey(r, t);
                            n = e._insertBeforeOrAppend(n, i)
                        }
                    }
                    )),
                    n) {
                        n._prev && (n._prev._next = null),
                        this._removalsHead = n;
                        for (var r = n; null !== r; r = r._nextRemoved)
                            r === this._mapHead && (this._mapHead = null),
                            this._records.delete(r.key),
                            r._nextRemoved = r._next,
                            r.previousValue = r.currentValue,
                            r.currentValue = null,
                            r._prev = null,
                            r._next = null
                    }
                    return this._changesTail && (this._changesTail._nextChanged = null),
                    this._additionsTail && (this._additionsTail._nextAdded = null),
                    this.isDirty
                }
            }, {
                key: "_insertBeforeOrAppend",
                value: function(t, e) {
                    if (t) {
                        var n = t._prev;
                        return e._next = t,
                        e._prev = n,
                        t._prev = e,
                        n && (n._next = e),
                        t === this._mapHead && (this._mapHead = e),
                        this._appendAfter = t,
                        t
                    }
                    return this._appendAfter ? (this._appendAfter._next = e,
                    e._prev = this._appendAfter) : this._mapHead = e,
                    this._appendAfter = e,
                    null
                }
            }, {
                key: "_getOrCreateRecordForKey",
                value: function(t, e) {
                    if (this._records.has(t)) {
                        var n = this._records.get(t);
                        this._maybeAddToChanges(n, e);
                        var r = n._prev
                          , i = n._next;
                        return r && (r._next = i),
                        i && (i._prev = r),
                        n._next = null,
                        n._prev = null,
                        n
                    }
                    var o = new ta(t);
                    return this._records.set(t, o),
                    o.currentValue = e,
                    this._addToAdditions(o),
                    o
                }
            }, {
                key: "_reset",
                value: function() {
                    if (this.isDirty) {
                        var t;
                        for (this._previousMapHead = this._mapHead,
                        t = this._previousMapHead; null !== t; t = t._next)
                            t._nextPrevious = t._next;
                        for (t = this._changesHead; null !== t; t = t._nextChanged)
                            t.previousValue = t.currentValue;
                        for (t = this._additionsHead; null != t; t = t._nextAdded)
                            t.previousValue = t.currentValue;
                        this._changesHead = this._changesTail = null,
                        this._additionsHead = this._additionsTail = null,
                        this._removalsHead = null
                    }
                }
            }, {
                key: "_maybeAddToChanges",
                value: function(t, e) {
                    Object.is(e, t.currentValue) || (t.previousValue = t.currentValue,
                    t.currentValue = e,
                    this._addToChanges(t))
                }
            }, {
                key: "_addToAdditions",
                value: function(t) {
                    null === this._additionsHead ? this._additionsHead = this._additionsTail = t : (this._additionsTail._nextAdded = t,
                    this._additionsTail = t)
                }
            }, {
                key: "_addToChanges",
                value: function(t) {
                    null === this._changesHead ? this._changesHead = this._changesTail = t : (this._changesTail._nextChanged = t,
                    this._changesTail = t)
                }
            }, {
                key: "_forEach",
                value: function(t, e) {
                    t instanceof Map ? t.forEach(e) : Object.keys(t).forEach((function(n) {
                        return e(t[n], n)
                    }
                    ))
                }
            }, {
                key: "isDirty",
                get: function() {
                    return null !== this._additionsHead || null !== this._changesHead || null !== this._removalsHead
                }
            }]),
            t
        }()
          , ta = function t(e) {
            m(this, t),
            this.key = e,
            this.previousValue = null,
            this.currentValue = null,
            this._nextPrevious = null,
            this._next = null,
            this._prev = null,
            this._nextAdded = null,
            this._nextRemoved = null,
            this._nextChanged = null
        }
          , ea = function() {
            var t = function() {
                function t(e) {
                    m(this, t),
                    this.factories = e
                }
                return _(t, [{
                    key: "find",
                    value: function(t) {
                        var e, n = this.factories.find((function(e) {
                            return e.supports(t)
                        }
                        ));
                        if (null != n)
                            return n;
                        throw new Error("Cannot find a differ supporting object '".concat(t, "' of type '").concat((e = t).name || typeof e, "'"))
                    }
                }], [{
                    key: "create",
                    value: function(e, n) {
                        if (null != n) {
                            var r = n.factories.slice();
                            e = e.concat(r)
                        }
                        return new t(e)
                    }
                }, {
                    key: "extend",
                    value: function(e) {
                        return {
                            provide: t,
                            useFactory: function(n) {
                                if (!n)
                                    throw new Error("Cannot extend IterableDiffers without a parent injector");
                                return t.create(e, n)
                            },
                            deps: [[t, new St, new bt]]
                        }
                    }
                }]),
                t
            }();
            return t.\u0275prov = Tt({
                token: t,
                providedIn: "root",
                factory: function() {
                    return new t([new Zo])
                }
            }),
            t
        }()
          , na = function() {
            var t = function() {
                function t(e) {
                    m(this, t),
                    this.factories = e
                }
                return _(t, [{
                    key: "find",
                    value: function(t) {
                        var e = this.factories.find((function(e) {
                            return e.supports(t)
                        }
                        ));
                        if (e)
                            return e;
                        throw new Error("Cannot find a differ supporting object '".concat(t, "'"))
                    }
                }], [{
                    key: "create",
                    value: function(e, n) {
                        if (n) {
                            var r = n.factories.slice();
                            e = e.concat(r)
                        }
                        return new t(e)
                    }
                }, {
                    key: "extend",
                    value: function(e) {
                        return {
                            provide: t,
                            useFactory: function(n) {
                                if (!n)
                                    throw new Error("Cannot extend KeyValueDiffers without a parent injector");
                                return t.create(e, n)
                            },
                            deps: [[t, new St, new bt]]
                        }
                    }
                }]),
                t
            }();
            return t.\u0275prov = Tt({
                token: t,
                providedIn: "root",
                factory: function() {
                    return new t([new Xo])
                }
            }),
            t
        }()
          , ra = [new Xo]
          , ia = new ea([new Zo])
          , oa = new na(ra)
          , aa = function() {
            var t = function t() {
                m(this, t)
            };
            return t.__NG_ELEMENT_ID__ = function() {
                return ua(t, Lo)
            }
            ,
            t
        }()
          , ua = function(t, e) {
            return function(t, e, n, r) {
                return zi || (zi = function(t) {
                    d(n, t);
                    var e = y(n);
                    function n(t, r, i) {
                        var o;
                        return m(this, n),
                        (o = e.call(this))._declarationView = t,
                        o._declarationTContainer = r,
                        o.elementRef = i,
                        o
                    }
                    return _(n, [{
                        key: "createEmbeddedView",
                        value: function(t) {
                            var e = this._declarationTContainer.tViews
                              , n = Br(this._declarationView, e, t, 16, null, e.declTNode, null, null, null, null);
                            n[17] = this._declarationView[this._declarationTContainer.index];
                            var r = this._declarationView[19];
                            return null !== r && (n[19] = r.createEmbeddedView(e)),
                            Wr(e, n, t),
                            new Bi(n)
                        }
                    }]),
                    n
                }(t)),
                0 === n.type ? new zi(r,n,Wi(e, n, r)) : null
            }(t, e, dn(), hn())
        }
          , sa = function() {
            var t = function t() {
                m(this, t)
            };
            return t.__NG_ELEMENT_ID__ = function() {
                return la(t, Lo)
            }
            ,
            t
        }()
          , la = function(t, e) {
            return function(t, e, n, r) {
                var i;
                qi || (qi = function(t) {
                    d(r, t);
                    var n = y(r);
                    function r(t, e, i) {
                        var o;
                        return m(this, r),
                        (o = n.call(this))._lContainer = t,
                        o._hostTNode = e,
                        o._hostView = i,
                        o
                    }
                    return _(r, [{
                        key: "clear",
                        value: function() {
                            for (; this.length > 0; )
                                this.remove(this.length - 1)
                        }
                    }, {
                        key: "get",
                        value: function(t) {
                            return null !== this._lContainer[8] && this._lContainer[8][t] || null
                        }
                    }, {
                        key: "createEmbeddedView",
                        value: function(t, e, n) {
                            var r = t.createEmbeddedView(e || {});
                            return this.insert(r, n),
                            r
                        }
                    }, {
                        key: "createComponent",
                        value: function(t, e, n, r, i) {
                            var o = n || this.parentInjector;
                            if (!i && null == t.ngModule && o) {
                                var a = o.get(ge, null);
                                a && (i = a)
                            }
                            var u = t.create(o, r, void 0, i);
                            return this.insert(u.hostView, e),
                            u
                        }
                    }, {
                        key: "insert",
                        value: function(t, e) {
                            var n = t._lView
                              , r = n[1];
                            if (t.destroyed)
                                throw new Error("Cannot insert a destroyed View in a ViewContainer!");
                            if (this.allocateContainerIfNeeded(),
                            Le(n[3])) {
                                var i = this.indexOf(t);
                                if (-1 !== i)
                                    this.detach(i);
                                else {
                                    var o = n[3]
                                      , a = new qi(o,o[6],o[3]);
                                    a.detach(a.indexOf(t))
                                }
                            }
                            var u = this._adjustIndex(e)
                              , s = this._lContainer;
                            !function(t, e, n, r) {
                                var i = De + r
                                  , o = n.length;
                                r > 0 && (n[i - 1][4] = e),
                                r < o - De ? (e[4] = n[i],
                                we(n, De + r, e)) : (n.push(e),
                                e[4] = null),
                                e[3] = n;
                                var a = e[17];
                                null !== a && n !== a && function(t, e) {
                                    var n = t[9];
                                    e[16] !== e[3][3][16] && (t[2] = !0),
                                    null === n ? t[9] = [e] : n.push(e)
                                }(a, e);
                                var u = e[19];
                                null !== u && u.insertView(t),
                                e[2] |= 128
                            }(r, n, s, u);
                            var l = function t(e, n) {
                                var r = De + e + 1;
                                if (r < n.length) {
                                    var i = n[r]
                                      , o = i[1].firstChild;
                                    if (null !== o)
                                        return function e(n, r) {
                                            if (null !== r) {
                                                var i = r.type;
                                                if (2 === i)
                                                    return tn(r, n);
                                                if (0 === i)
                                                    return t(-1, n[r.index]);
                                                if (3 === i || 4 === i) {
                                                    var o = r.child;
                                                    if (null !== o)
                                                        return e(n, o);
                                                    var a = n[r.index];
                                                    return Le(a) ? t(-1, a) : $e(a)
                                                }
                                                var u = n[16]
                                                  , s = u[6]
                                                  , l = Ur(u)
                                                  , c = s.projection[r.projection];
                                                return null != c ? e(l, c) : e(n, r.next)
                                            }
                                            return null
                                        }(i, o)
                                }
                                return n[7]
                            }(u, s)
                              , c = n[11]
                              , h = Ni(c, s[7]);
                            return null !== h && function(t, e, n, r, i, o) {
                                r[0] = i,
                                r[6] = e,
                                Di(t, r, n, 1, i, o)
                            }(r, s[6], c, n, h, l),
                            t.attachToViewContainerRef(this),
                            we(s[8], u, t),
                            t
                        }
                    }, {
                        key: "move",
                        value: function(t, e) {
                            if (t.destroyed)
                                throw new Error("Cannot move a destroyed View in a ViewContainer!");
                            return this.insert(t, e)
                        }
                    }, {
                        key: "indexOf",
                        value: function(t) {
                            var e = this._lContainer[8];
                            return null !== e ? e.indexOf(t) : -1
                        }
                    }, {
                        key: "remove",
                        value: function(t) {
                            this.allocateContainerIfNeeded();
                            var e = this._adjustIndex(t, -1)
                              , n = Oi(this._lContainer, e);
                            n && (_e(this._lContainer[8], e),
                            Ai(n[1], n))
                        }
                    }, {
                        key: "detach",
                        value: function(t) {
                            this.allocateContainerIfNeeded();
                            var e = this._adjustIndex(t, -1)
                              , n = Oi(this._lContainer, e);
                            return n && null != _e(this._lContainer[8], e) ? new Bi(n) : null
                        }
                    }, {
                        key: "_adjustIndex",
                        value: function(t) {
                            return null == t ? this.length + (arguments.length > 1 && void 0 !== arguments[1] ? arguments[1] : 0) : t
                        }
                    }, {
                        key: "allocateContainerIfNeeded",
                        value: function() {
                            null === this._lContainer[8] && (this._lContainer[8] = [])
                        }
                    }, {
                        key: "element",
                        get: function() {
                            return Wi(e, this._hostTNode, this._hostView)
                        }
                    }, {
                        key: "injector",
                        get: function() {
                            return new pr(this._hostTNode,this._hostView)
                        }
                    }, {
                        key: "parentInjector",
                        get: function() {
                            var t = ar(this._hostTNode, this._hostView);
                            if (Gn(t)) {
                                var e = Jn(t, this._hostView)
                                  , n = Qn(t);
                                return new pr(e[1].data[n + 8],e)
                            }
                            return new pr(null,this._hostView)
                        }
                    }, {
                        key: "length",
                        get: function() {
                            return this._lContainer.length - De
                        }
                    }]),
                    r
                }(t));
                var o = r[n.index];
                if (Le(o))
                    i = o;
                else {
                    var a;
                    if (3 === n.type)
                        a = $e(o);
                    else if (a = r[11].createComment(""),
                    Be(r)) {
                        var u = r[11]
                          , s = tn(n, r);
                        Pi(u, Ni(u, s), a, function(t, e) {
                            return Ye(t) ? t.nextSibling(e) : e.nextSibling
                        }(u, s))
                    } else
                        Mi(r[1], r, a, n);
                    r[n.index] = i = di(o, r, a, n),
                    gi(r, i)
                }
                return new qi(i,n,r)
            }(t, e, dn(), hn())
        }
          , ca = {}
          , ha = function(t) {
            d(n, t);
            var e = y(n);
            function n(t) {
                var r;
                return m(this, n),
                (r = e.call(this)).ngModule = t,
                r
            }
            return _(n, [{
                key: "resolveComponentFactory",
                value: function(t) {
                    var e = je(t);
                    return new va(e,this.ngModule)
                }
            }]),
            n
        }(Ho);
        function fa(t) {
            var e = [];
            for (var n in t)
                t.hasOwnProperty(n) && e.push({
                    propName: t[n],
                    templateName: n
                });
            return e
        }
        var da = new ee("SCHEDULER_TOKEN",{
            providedIn: "root",
            factory: function() {
                return Xn
            }
        })
          , va = function(t) {
            d(n, t);
            var e = y(n);
            function n(t, r) {
                var i;
                return m(this, n),
                (i = e.call(this)).componentDef = t,
                i.ngModule = r,
                i.componentType = t.type,
                i.selector = t.selectors.map(Nr).join(","),
                i.ngContentSelectors = t.ngContentSelectors ? t.ngContentSelectors : [],
                i.isBoundToModule = !!r,
                i
            }
            return _(n, [{
                key: "create",
                value: function(t, e, n, r) {
                    var i, o, a = (r = r || this.ngModule) ? function(t, e) {
                        return {
                            get: function(n, r, i) {
                                var o = t.get(n, ca, i);
                                return o !== ca || r === ca ? o : e.get(n, r, i)
                            }
                        }
                    }(t, r.injector) : t, u = a.get(Vo, Xe), s = a.get(qo, null), l = u.createRenderer(null, this.componentDef), c = this.componentDef.selectors[0][0] || "div", h = n ? function(t, e, n) {
                        if (Ye(t))
                            return t.selectRootElement(e, n === be.ShadowDom);
                        var r = "string" == typeof e ? t.querySelector(e) : e;
                        return r.textContent = "",
                        r
                    }(l, n, this.componentDef.encapsulation) : qr(c, u.createRenderer(null, this.componentDef), function(t) {
                        var e = t.toLowerCase();
                        return "svg" === e ? Je : "math" === e ? "http://www.w3.org/1998/MathML/" : null
                    }(c)), f = this.componentDef.onPush ? 576 : 528, d = {
                        components: [],
                        scheduler: Xn,
                        clean: ki,
                        playerHandler: null,
                        flags: 0
                    }, v = $r(0, null, null, 1, 0, null, null, null, null, null), p = Br(null, v, d, f, null, null, u, l, s, a);
                    Cn(p);
                    try {
                        var g = function(t, e, n, r, i, o) {
                            var a = n[1];
                            n[20] = t;
                            var u = Zr(a, 0, 2, null, null)
                              , s = u.mergedAttrs = e.hostAttrs;
                            null !== s && (vo(u, s, !0),
                            null !== t && (zn(i, t, s),
                            null !== u.classes && Fi(i, t, u.classes),
                            null !== u.styles && Li(i, t, u.styles)));
                            var l = r.createRenderer(t, e)
                              , c = Br(n, Xr(e), null, e.onPush ? 64 : 16, n[20], u, r, l, null, null);
                            return a.firstCreatePass && (ur(rr(u, n), a, e.type),
                            ai(a, u),
                            si(u, n.length, 1)),
                            gi(n, c),
                            n[20] = c
                        }(h, this.componentDef, p, u, l);
                        if (h)
                            if (n)
                                zn(l, h, ["ng-version", Bo.full]);
                            else {
                                var y = function(t) {
                                    for (var e = [], n = [], r = 1, i = 2; r < t.length; ) {
                                        var o = t[r];
                                        if ("string" == typeof o)
                                            2 === i ? "" !== o && e.push(o, t[++r]) : 8 === i && n.push(o);
                                        else {
                                            if (!Rr(i))
                                                break;
                                            i = o
                                        }
                                        r++
                                    }
                                    return {
                                        attrs: e,
                                        classes: n
                                    }
                                }(this.componentDef.selectors[0])
                                  , m = y.attrs
                                  , w = y.classes;
                                m && zn(l, h, m),
                                w && w.length > 0 && Fi(l, h, w.join(" "))
                            }
                        if (o = en(v, 0),
                        void 0 !== e)
                            for (var _ = o.projection = [], k = 0; k < this.ngContentSelectors.length; k++) {
                                var b = e[k];
                                _.push(null != b ? Array.from(b) : null)
                            }
                        i = function(t, e, n, r, i) {
                            var o = n[1]
                              , a = function(t, e, n) {
                                var r = dn();
                                t.firstCreatePass && (n.providersResolver && n.providersResolver(n),
                                oi(t, r, 1),
                                li(t, e, n));
                                var i = hr(e, t, e.length - 1, r);
                                Cr(i, e);
                                var o = tn(r, e);
                                return o && Cr(o, e),
                                i
                            }(o, n, e);
                            r.components.push(a),
                            t[8] = a,
                            i && i.forEach((function(t) {
                                return t(a, e)
                            }
                            )),
                            e.contentQueries && e.contentQueries(1, a, n.length - 1);
                            var u = dn();
                            if (o.firstCreatePass && (null !== e.hostBindings || null !== e.hostAttrs)) {
                                Pn(u.index - Ue);
                                var s = n[1];
                                ni(s, e),
                                ri(s, n, e.hostVars),
                                ii(e, a)
                            }
                            return a
                        }(g, this.componentDef, p, d, [po]),
                        Wr(v, p, null)
                    } finally {
                        On()
                    }
                    return new pa(this.componentType,i,Wi(Lo, o, p),p,o)
                }
            }, {
                key: "inputs",
                get: function() {
                    return fa(this.componentDef.inputs)
                }
            }, {
                key: "outputs",
                get: function() {
                    return fa(this.componentDef.outputs)
                }
            }]),
            n
        }(Uo)
          , pa = function(t) {
            d(n, t);
            var e = y(n);
            function n(t, r, i, o, a) {
                var u;
                return m(this, n),
                (u = e.call(this)).location = i,
                u._rootLView = o,
                u._tNode = a,
                u.destroyCbs = [],
                u.instance = r,
                u.hostView = u.changeDetectorRef = new Zi(o),
                u.componentType = t,
                u
            }
            return _(n, [{
                key: "destroy",
                value: function() {
                    this.destroyCbs && (this.destroyCbs.forEach((function(t) {
                        return t()
                    }
                    )),
                    this.destroyCbs = null,
                    !this.hostView.destroyed && this.hostView.destroy())
                }
            }, {
                key: "onDestroy",
                value: function(t) {
                    this.destroyCbs && this.destroyCbs.push(t)
                }
            }, {
                key: "injector",
                get: function() {
                    return new pr(this._tNode,this._rootLView)
                }
            }]),
            n
        }(Mo)
          , ga = void 0
          , ya = ["en", [["a", "p"], ["AM", "PM"], ga], [["AM", "PM"], ga, ga], [["S", "M", "T", "W", "T", "F", "S"], ["Sun", "Mon", "Tue", "Wed", "Thu", "Fri", "Sat"], ["Sunday", "Monday", "Tuesday", "Wednesday", "Thursday", "Friday", "Saturday"], ["Su", "Mo", "Tu", "We", "Th", "Fr", "Sa"]], ga, [["J", "F", "M", "A", "M", "J", "J", "A", "S", "O", "N", "D"], ["Jan", "Feb", "Mar", "Apr", "May", "Jun", "Jul", "Aug", "Sep", "Oct", "Nov", "Dec"], ["January", "February", "March", "April", "May", "June", "July", "August", "September", "October", "November", "December"]], ga, [["B", "A"], ["BC", "AD"], ["Before Christ", "Anno Domini"]], 0, [6, 0], ["M/d/yy", "MMM d, y", "MMMM d, y", "EEEE, MMMM d, y"], ["h:mm a", "h:mm:ss a", "h:mm:ss a z", "h:mm:ss a zzzz"], ["{1}, {0}", ga, "{1} 'at' {0}", ga], [".", ",", ";", "%", "+", "-", "E", "\xd7", "\u2030", "\u221e", "NaN", ":"], ["#,##0.###", "#,##0%", "\xa4#,##0.00", "#E0"], "USD", "$", "US Dollar", {}, "ltr", function(t) {
            var e = Math.floor(Math.abs(t))
              , n = t.toString().replace(/^[^.]*\.?/, "").length;
            return 1 === e && 0 === n ? 1 : 5
        }
        ]
          , ma = {};
        function wa(t) {
            return t in ma || (ma[t] = Wt.ng && Wt.ng.common && Wt.ng.common.locales && Wt.ng.common.locales[t]),
            ma[t]
        }
        var _a = function(t) {
            return t[t.LocaleId = 0] = "LocaleId",
            t[t.DayPeriodsFormat = 1] = "DayPeriodsFormat",
            t[t.DayPeriodsStandalone = 2] = "DayPeriodsStandalone",
            t[t.DaysFormat = 3] = "DaysFormat",
            t[t.DaysStandalone = 4] = "DaysStandalone",
            t[t.MonthsFormat = 5] = "MonthsFormat",
            t[t.MonthsStandalone = 6] = "MonthsStandalone",
            t[t.Eras = 7] = "Eras",
            t[t.FirstDayOfWeek = 8] = "FirstDayOfWeek",
            t[t.WeekendRange = 9] = "WeekendRange",
            t[t.DateFormat = 10] = "DateFormat",
            t[t.TimeFormat = 11] = "TimeFormat",
            t[t.DateTimeFormat = 12] = "DateTimeFormat",
            t[t.NumberSymbols = 13] = "NumberSymbols",
            t[t.NumberFormats = 14] = "NumberFormats",
            t[t.CurrencyCode = 15] = "CurrencyCode",
            t[t.CurrencySymbol = 16] = "CurrencySymbol",
            t[t.CurrencyName = 17] = "CurrencyName",
            t[t.Currencies = 18] = "Currencies",
            t[t.Directionality = 19] = "Directionality",
            t[t.PluralCase = 20] = "PluralCase",
            t[t.ExtraData = 21] = "ExtraData",
            t
        }({})
          , ka = "en-US";
        function ba(t) {
            var e, n;
            n = "Expected localeId to be defined",
            null == (e = t) && function(t, e, n, r) {
                throw new Error("ASSERTION ERROR: ".concat(t) + " [Expected=> ".concat(null, " ").concat("!=", " ").concat(e, " <=Actual]"))
            }(n, e),
            "string" == typeof t && t.toLowerCase().replace(/_/g, "-")
        }
        var Ca = new Map
          , Sa = function(t) {
            d(n, t);
            var e = y(n);
            function n(t, r) {
                var i;
                m(this, n),
                (i = e.call(this))._parent = r,
                i._bootstrapComponents = [],
                i.injector = o(i),
                i.destroyCbs = [],
                i.componentFactoryResolver = new ha(o(i));
                var a = Me(t)
                  , u = t[Yt] || null;
                return u && ba(u),
                i._bootstrapComponents = $n(a.bootstrap),
                i._r3Injector = ro(t, r, [{
                    provide: ge,
                    useValue: o(i)
                }, {
                    provide: Ho,
                    useValue: i.componentFactoryResolver
                }], Dt(t)),
                i._r3Injector._resolveInjectorDefTypes(),
                i.instance = i.get(t),
                i
            }
            return _(n, [{
                key: "get",
                value: function(t) {
                    var e = arguments.length > 1 && void 0 !== arguments[1] ? arguments[1] : ho.THROW_IF_NOT_FOUND
                      , n = arguments.length > 2 && void 0 !== arguments[2] ? arguments[2] : xt.Default;
                    return t === ho || t === ge || t === ne ? this : this._r3Injector.get(t, e, n)
                }
            }, {
                key: "destroy",
                value: function() {
                    var t = this._r3Injector;
                    !t.destroyed && t.destroy(),
                    this.destroyCbs.forEach((function(t) {
                        return t()
                    }
                    )),
                    this.destroyCbs = null
                }
            }, {
                key: "onDestroy",
                value: function(t) {
                    this.destroyCbs.push(t)
                }
            }]),
            n
        }(ge)
          , xa = function(t) {
            d(n, t);
            var e = y(n);
            function n(t) {
                var r;
                return m(this, n),
                (r = e.call(this)).moduleType = t,
                null !== Me(t) && function t(e) {
                    if (null !== e.\u0275mod.id) {
                        var n = e.\u0275mod.id;
                        (function(t, e, n) {
                            if (e && e !== n)
                                throw new Error("Duplicate module registered for ".concat(t, " - ").concat(Dt(e), " vs ").concat(Dt(e.name)))
                        }
                        )(n, Ca.get(n), e),
                        Ca.set(n, e)
                    }
                    var r = e.\u0275mod.imports;
                    r instanceof Function && (r = r()),
                    r && r.forEach((function(e) {
                        return t(e)
                    }
                    ))
                }(t),
                r
            }
            return _(n, [{
                key: "create",
                value: function(t) {
                    return new Sa(this.moduleType,t)
                }
            }]),
            n
        }(ye)
          , Ea = function(t) {
            d(n, t);
            var e = y(n);
            function n() {
                var t, r = arguments.length > 0 && void 0 !== arguments[0] && arguments[0];
                return m(this, n),
                (t = e.call(this)).__isAsync = r,
                t
            }
            return _(n, [{
                key: "emit",
                value: function(t) {
                    i(r(n.prototype), "next", this).call(this, t)
                }
            }, {
                key: "subscribe",
                value: function(t, e, o) {
                    var a, u = function(t) {
                        return null
                    }, s = function() {
                        return null
                    };
                    t && "object" == typeof t ? (a = this.__isAsync ? function(e) {
                        setTimeout((function() {
                            return t.next(e)
                        }
                        ))
                    }
                    : function(e) {
                        t.next(e)
                    }
                    ,
                    t.error && (u = this.__isAsync ? function(e) {
                        setTimeout((function() {
                            return t.error(e)
                        }
                        ))
                    }
                    : function(e) {
                        t.error(e)
                    }
                    ),
                    t.complete && (s = this.__isAsync ? function() {
                        setTimeout((function() {
                            return t.complete()
                        }
                        ))
                    }
                    : function() {
                        t.complete()
                    }
                    )) : (a = this.__isAsync ? function(e) {
                        setTimeout((function() {
                            return t(e)
                        }
                        ))
                    }
                    : function(e) {
                        t(e)
                    }
                    ,
                    e && (u = this.__isAsync ? function(t) {
                        setTimeout((function() {
                            return e(t)
                        }
                        ))
                    }
                    : function(t) {
                        e(t)
                    }
                    ),
                    o && (s = this.__isAsync ? function() {
                        setTimeout((function() {
                            return o()
                        }
                        ))
                    }
                    : function() {
                        o()
                    }
                    ));
                    var l = i(r(n.prototype), "subscribe", this).call(this, a, u, s);
                    return t instanceof E && t.add(l),
                    l
                }
            }]),
            n
        }(q)
          , Ta = new ee("Application Initializer")
          , Oa = function() {
            var t = function() {
                function t(e) {
                    var n = this;
                    m(this, t),
                    this.appInits = e,
                    this.initialized = !1,
                    this.done = !1,
                    this.donePromise = new Promise((function(t, e) {
                        n.resolve = t,
                        n.reject = e
                    }
                    ))
                }
                return _(t, [{
                    key: "runInitializers",
                    value: function() {
                        var t = this;
                        if (!this.initialized) {
                            var e = []
                              , n = function() {
                                t.done = !0,
                                t.resolve()
                            };
                            if (this.appInits)
                                for (var r = 0; r < this.appInits.length; r++) {
                                    var i = this.appInits[r]();
                                    To(i) && e.push(i)
                                }
                            Promise.all(e).then((function() {
                                n()
                            }
                            )).catch((function(e) {
                                t.reject(e)
                            }
                            )),
                            0 === e.length && n(),
                            this.initialized = !0
                        }
                    }
                }]),
                t
            }();
            return t.\u0275fac = function(e) {
                return new (e || t)(he(Ta, 8))
            }
            ,
            t.\u0275prov = Tt({
                token: t,
                factory: t.\u0275fac
            }),
            t
        }()
          , Aa = new ee("AppId")
          , Ra = {
            provide: Aa,
            useFactory: function() {
                return "".concat(Pa()).concat(Pa()).concat(Pa())
            },
            deps: []
        };
        function Pa() {
            return String.fromCharCode(97 + Math.floor(25 * Math.random()))
        }
        var Ia = new ee("Platform Initializer")
          , ja = new ee("Platform ID")
          , Na = new ee("appBootstrapListener")
          , Ma = function() {
            var t = function() {
                function t() {
                    m(this, t)
                }
                return _(t, [{
                    key: "log",
                    value: function(t) {
                        console.log(t)
                    }
                }, {
                    key: "warn",
                    value: function(t) {
                        console.warn(t)
                    }
                }]),
                t
            }();
            return t.\u0275fac = function(e) {
                return new (e || t)
            }
            ,
            t.\u0275prov = Tt({
                token: t,
                factory: t.\u0275fac
            }),
            t
        }()
          , Ua = new ee("LocaleId")
          , Da = new ee("DefaultCurrencyCode")
          , Ha = function t(e, n) {
            m(this, t),
            this.ngModuleFactory = e,
            this.componentFactories = n
        }
          , La = function(t) {
            return new xa(t)
        }
          , Fa = La
          , Va = function(t) {
            return Promise.resolve(La(t))
        }
          , za = function(t) {
            var e = La(t)
              , n = $n(Me(t).declarations).reduce((function(t, e) {
                var n = je(e);
                return n && t.push(new va(n)),
                t
            }
            ), []);
            return new Ha(e,n)
        }
          , qa = za
          , Ba = function(t) {
            return Promise.resolve(za(t))
        }
          , Za = function() {
            var t = function() {
                function t() {
                    m(this, t),
                    this.compileModuleSync = Fa,
                    this.compileModuleAsync = Va,
                    this.compileModuleAndAllComponentsSync = qa,
                    this.compileModuleAndAllComponentsAsync = Ba
                }
                return _(t, [{
                    key: "clearCache",
                    value: function() {}
                }, {
                    key: "clearCacheFor",
                    value: function(t) {}
                }, {
                    key: "getModuleId",
                    value: function(t) {}
                }]),
                t
            }();
            return t.\u0275fac = function(e) {
                return new (e || t)
            }
            ,
            t.\u0275prov = Tt({
                token: t,
                factory: t.\u0275fac
            }),
            t
        }()
          , Wa = function() {
            return Promise.resolve(0)
        }();
        function Ga(t) {
            "undefined" == typeof Zone ? Wa.then((function() {
                t && t.apply(null, null)
            }
            )) : Zone.current.scheduleMicroTask("scheduleMicrotask", t)
        }
        var Qa = function() {
            function t(e) {
                var n = e.enableLongStackTrace
                  , r = void 0 !== n && n
                  , i = e.shouldCoalesceEventChangeDetection
                  , o = void 0 !== i && i;
                if (m(this, t),
                this.hasPendingMacrotasks = !1,
                this.hasPendingMicrotasks = !1,
                this.isStable = !0,
                this.onUnstable = new Ea(!1),
                this.onMicrotaskEmpty = new Ea(!1),
                this.onStable = new Ea(!1),
                this.onError = new Ea(!1),
                "undefined" == typeof Zone)
                    throw new Error("In this configuration Angular requires Zone.js");
                Zone.assertZonePatched();
                var a, u, s, l = this;
                l._nesting = 0,
                l._outer = l._inner = Zone.current,
                Zone.wtfZoneSpec && (l._inner = l._inner.fork(Zone.wtfZoneSpec)),
                Zone.TaskTrackingZoneSpec && (l._inner = l._inner.fork(new Zone.TaskTrackingZoneSpec)),
                r && Zone.longStackTraceZoneSpec && (l._inner = l._inner.fork(Zone.longStackTraceZoneSpec)),
                l.shouldCoalesceEventChangeDetection = o,
                l.lastRequestAnimationFrameId = -1,
                l.nativeRequestAnimationFrame = function() {
                    var t = Wt.requestAnimationFrame
                      , e = Wt.cancelAnimationFrame;
                    if ("undefined" != typeof Zone && t && e) {
                        var n = t[Zone.__symbol__("OriginalDelegate")];
                        n && (t = n);
                        var r = e[Zone.__symbol__("OriginalDelegate")];
                        r && (e = r)
                    }
                    return {
                        nativeRequestAnimationFrame: t,
                        nativeCancelAnimationFrame: e
                    }
                }().nativeRequestAnimationFrame,
                u = function() {
                    !function(t) {
                        -1 === t.lastRequestAnimationFrameId && (t.lastRequestAnimationFrameId = t.nativeRequestAnimationFrame.call(Wt, (function() {
                            t.fakeTopEventTask || (t.fakeTopEventTask = Zone.root.scheduleEventTask("fakeTopEventTask", (function() {
                                t.lastRequestAnimationFrameId = -1,
                                Xa(t),
                                Ya(t)
                            }
                            ), void 0, (function() {}
                            ), (function() {}
                            ))),
                            t.fakeTopEventTask.invoke()
                        }
                        )),
                        Xa(t))
                    }(a)
                }
                ,
                (a = l)._inner = a._inner.fork({
                    name: "angular",
                    properties: {
                        isAngularZone: !0,
                        maybeDelayChangeDetection: s = !!a.shouldCoalesceEventChangeDetection && a.nativeRequestAnimationFrame && u
                    },
                    onInvokeTask: function(t, e, n, r, i, o) {
                        try {
                            return $a(a),
                            t.invokeTask(n, r, i, o)
                        } finally {
                            s && "eventTask" === r.type && s(),
                            tu(a)
                        }
                    },
                    onInvoke: function(t, e, n, r, i, o, u) {
                        try {
                            return $a(a),
                            t.invoke(n, r, i, o, u)
                        } finally {
                            tu(a)
                        }
                    },
                    onHasTask: function(t, e, n, r) {
                        t.hasTask(n, r),
                        e === n && ("microTask" == r.change ? (a._hasPendingMicrotasks = r.microTask,
                        Xa(a),
                        Ya(a)) : "macroTask" == r.change && (a.hasPendingMacrotasks = r.macroTask))
                    },
                    onHandleError: function(t, e, n, r) {
                        return t.handleError(n, r),
                        a.runOutsideAngular((function() {
                            return a.onError.emit(r)
                        }
                        )),
                        !1
                    }
                })
            }
            return _(t, [{
                key: "run",
                value: function(t, e, n) {
                    return this._inner.run(t, e, n)
                }
            }, {
                key: "runTask",
                value: function(t, e, n, r) {
                    var i = this._inner
                      , o = i.scheduleEventTask("NgZoneEvent: " + r, t, Ka, Ja, Ja);
                    try {
                        return i.runTask(o, e, n)
                    } finally {
                        i.cancelTask(o)
                    }
                }
            }, {
                key: "runGuarded",
                value: function(t, e, n) {
                    return this._inner.runGuarded(t, e, n)
                }
            }, {
                key: "runOutsideAngular",
                value: function(t) {
                    return this._outer.run(t)
                }
            }], [{
                key: "isInAngularZone",
                value: function() {
                    return !0 === Zone.current.get("isAngularZone")
                }
            }, {
                key: "assertInAngularZone",
                value: function() {
                    if (!t.isInAngularZone())
                        throw new Error("Expected to be in Angular Zone, but it is not!")
                }
            }, {
                key: "assertNotInAngularZone",
                value: function() {
                    if (t.isInAngularZone())
                        throw new Error("Expected to not be in Angular Zone, but it is!")
                }
            }]),
            t
        }();
        function Ja() {}
        var Ka = {};
        function Ya(t) {
            if (0 == t._nesting && !t.hasPendingMicrotasks && !t.isStable)
                try {
                    t._nesting++,
                    t.onMicrotaskEmpty.emit(null)
                } finally {
                    if (t._nesting--,
                    !t.hasPendingMicrotasks)
                        try {
                            t.runOutsideAngular((function() {
                                return t.onStable.emit(null)
                            }
                            ))
                        } finally {
                            t.isStable = !0
                        }
                }
        }
        function Xa(t) {
            t.hasPendingMicrotasks = !!(t._hasPendingMicrotasks || t.shouldCoalesceEventChangeDetection && -1 !== t.lastRequestAnimationFrameId)
        }
        function $a(t) {
            t._nesting++,
            t.isStable && (t.isStable = !1,
            t.onUnstable.emit(null))
        }
        function tu(t) {
            t._nesting--,
            Ya(t)
        }
        var eu, nu = function() {
            function t() {
                m(this, t),
                this.hasPendingMicrotasks = !1,
                this.hasPendingMacrotasks = !1,
                this.isStable = !0,
                this.onUnstable = new Ea,
                this.onMicrotaskEmpty = new Ea,
                this.onStable = new Ea,
                this.onError = new Ea
            }
            return _(t, [{
                key: "run",
                value: function(t, e, n) {
                    return t.apply(e, n)
                }
            }, {
                key: "runGuarded",
                value: function(t, e, n) {
                    return t.apply(e, n)
                }
            }, {
                key: "runOutsideAngular",
                value: function(t) {
                    return t()
                }
            }, {
                key: "runTask",
                value: function(t, e, n, r) {
                    return t.apply(e, n)
                }
            }]),
            t
        }(), ru = function() {
            var t = function() {
                function t(e) {
                    var n = this;
                    m(this, t),
                    this._ngZone = e,
                    this._pendingCount = 0,
                    this._isZoneStable = !0,
                    this._didWork = !1,
                    this._callbacks = [],
                    this.taskTrackingZone = null,
                    this._watchAngularEvents(),
                    e.run((function() {
                        n.taskTrackingZone = "undefined" == typeof Zone ? null : Zone.current.get("TaskTrackingZone")
                    }
                    ))
                }
                return _(t, [{
                    key: "_watchAngularEvents",
                    value: function() {
                        var t = this;
                        this._ngZone.onUnstable.subscribe({
                            next: function() {
                                t._didWork = !0,
                                t._isZoneStable = !1
                            }
                        }),
                        this._ngZone.runOutsideAngular((function() {
                            t._ngZone.onStable.subscribe({
                                next: function() {
                                    Qa.assertNotInAngularZone(),
                                    Ga((function() {
                                        t._isZoneStable = !0,
                                        t._runCallbacksIfReady()
                                    }
                                    ))
                                }
                            })
                        }
                        ))
                    }
                }, {
                    key: "increasePendingRequestCount",
                    value: function() {
                        return this._pendingCount += 1,
                        this._didWork = !0,
                        this._pendingCount
                    }
                }, {
                    key: "decreasePendingRequestCount",
                    value: function() {
                        if (this._pendingCount -= 1,
                        this._pendingCount < 0)
                            throw new Error("pending async requests below zero");
                        return this._runCallbacksIfReady(),
                        this._pendingCount
                    }
                }, {
                    key: "isStable",
                    value: function() {
                        return this._isZoneStable && 0 === this._pendingCount && !this._ngZone.hasPendingMacrotasks
                    }
                }, {
                    key: "_runCallbacksIfReady",
                    value: function() {
                        var t = this;
                        if (this.isStable())
                            Ga((function() {
                                for (; 0 !== t._callbacks.length; ) {
                                    var e = t._callbacks.pop();
                                    clearTimeout(e.timeoutId),
                                    e.doneCb(t._didWork)
                                }
                                t._didWork = !1
                            }
                            ));
                        else {
                            var e = this.getPendingTasks();
                            this._callbacks = this._callbacks.filter((function(t) {
                                return !t.updateCb || !t.updateCb(e) || (clearTimeout(t.timeoutId),
                                !1)
                            }
                            )),
                            this._didWork = !0
                        }
                    }
                }, {
                    key: "getPendingTasks",
                    value: function() {
                        return this.taskTrackingZone ? this.taskTrackingZone.macroTasks.map((function(t) {
                            return {
                                source: t.source,
                                creationLocation: t.creationLocation,
                                data: t.data
                            }
                        }
                        )) : []
                    }
                }, {
                    key: "addCallback",
                    value: function(t, e, n) {
                        var r = this
                          , i = -1;
                        e && e > 0 && (i = setTimeout((function() {
                            r._callbacks = r._callbacks.filter((function(t) {
                                return t.timeoutId !== i
                            }
                            )),
                            t(r._didWork, r.getPendingTasks())
                        }
                        ), e)),
                        this._callbacks.push({
                            doneCb: t,
                            timeoutId: i,
                            updateCb: n
                        })
                    }
                }, {
                    key: "whenStable",
                    value: function(t, e, n) {
                        if (n && !this.taskTrackingZone)
                            throw new Error('Task tracking zone is required when passing an update callback to whenStable(). Is "zone.js/dist/task-tracking.js" loaded?');
                        this.addCallback(t, e, n),
                        this._runCallbacksIfReady()
                    }
                }, {
                    key: "getPendingRequestCount",
                    value: function() {
                        return this._pendingCount
                    }
                }, {
                    key: "findProviders",
                    value: function(t, e, n) {
                        return []
                    }
                }]),
                t
            }();
            return t.\u0275fac = function(e) {
                return new (e || t)(he(Qa))
            }
            ,
            t.\u0275prov = Tt({
                token: t,
                factory: t.\u0275fac
            }),
            t
        }(), iu = function() {
            var t = function() {
                function t() {
                    m(this, t),
                    this._applications = new Map,
                    ou.addToWindow(this)
                }
                return _(t, [{
                    key: "registerApplication",
                    value: function(t, e) {
                        this._applications.set(t, e)
                    }
                }, {
                    key: "unregisterApplication",
                    value: function(t) {
                        this._applications.delete(t)
                    }
                }, {
                    key: "unregisterAllApplications",
                    value: function() {
                        this._applications.clear()
                    }
                }, {
                    key: "getTestability",
                    value: function(t) {
                        return this._applications.get(t) || null
                    }
                }, {
                    key: "getAllTestabilities",
                    value: function() {
                        return Array.from(this._applications.values())
                    }
                }, {
                    key: "getAllRootElements",
                    value: function() {
                        return Array.from(this._applications.keys())
                    }
                }, {
                    key: "findTestabilityInTree",
                    value: function(t) {
                        var e = !(arguments.length > 1 && void 0 !== arguments[1]) || arguments[1];
                        return ou.findTestabilityInTree(this, t, e)
                    }
                }]),
                t
            }();
            return t.\u0275fac = function(e) {
                return new (e || t)
            }
            ,
            t.\u0275prov = Tt({
                token: t,
                factory: t.\u0275fac
            }),
            t
        }(), ou = new (function() {
            function t() {
                m(this, t)
            }
            return _(t, [{
                key: "addToWindow",
                value: function(t) {}
            }, {
                key: "findTestabilityInTree",
                value: function(t, e, n) {
                    return null
                }
            }]),
            t
        }()), au = function(t, e, n) {
            var r = new xa(n);
            return Promise.resolve(r)
        }, uu = new ee("AllowMultipleToken"), su = function t(e, n) {
            m(this, t),
            this.name = e,
            this.token = n
        };
        function lu(t) {
            if (eu && !eu.destroyed && !eu.injector.get(uu, !1))
                throw new Error("There can be only one platform. Destroy the previous one to create a new one.");
            eu = t.get(du);
            var e = t.get(Ia, null);
            return e && e.forEach((function(t) {
                return t()
            }
            )),
            eu
        }
        function cu(t, e) {
            var n = arguments.length > 2 && void 0 !== arguments[2] ? arguments[2] : []
              , r = "Platform: ".concat(e)
              , i = new ee(r);
            return function() {
                var e = arguments.length > 0 && void 0 !== arguments[0] ? arguments[0] : []
                  , o = fu();
                if (!o || o.injector.get(uu, !1))
                    if (t)
                        t(n.concat(e).concat({
                            provide: i,
                            useValue: !0
                        }));
                    else {
                        var a = n.concat(e).concat({
                            provide: i,
                            useValue: !0
                        }, {
                            provide: Yi,
                            useValue: "platform"
                        });
                        lu(ho.create({
                            providers: a,
                            name: r
                        }))
                    }
                return hu(i)
            }
        }
        function hu(t) {
            var e = fu();
            if (!e)
                throw new Error("No platform exists!");
            if (!e.injector.get(t, null))
                throw new Error("A platform with a different configuration has been created. Please destroy it first.");
            return e
        }
        function fu() {
            return eu && !eu.destroyed ? eu : null
        }
        var du = function() {
            var t = function() {
                function t(e) {
                    m(this, t),
                    this._injector = e,
                    this._modules = [],
                    this._destroyListeners = [],
                    this._destroyed = !1
                }
                return _(t, [{
                    key: "bootstrapModuleFactory",
                    value: function(t, e) {
                        var n, r, i = this, o = (r = e && e.ngZoneEventCoalescing || !1,
                        "noop" === (n = e ? e.ngZone : void 0) ? new nu : ("zone.js" === n ? void 0 : n) || new Qa({
                            enableLongStackTrace: br(),
                            shouldCoalesceEventChangeDetection: r
                        })), a = [{
                            provide: Qa,
                            useValue: o
                        }];
                        return o.run((function() {
                            var e = ho.create({
                                providers: a,
                                parent: i.injector,
                                name: t.moduleType.name
                            })
                              , n = t.create(e)
                              , r = n.injector.get(wr, null);
                            if (!r)
                                throw new Error("No ErrorHandler. Is platform module (BrowserModule) included?");
                            return n.onDestroy((function() {
                                return gu(i._modules, n)
                            }
                            )),
                            o.runOutsideAngular((function() {
                                return o.onError.subscribe({
                                    next: function(t) {
                                        r.handleError(t)
                                    }
                                })
                            }
                            )),
                            function(t, e, r) {
                                try {
                                    var o = ((a = n.injector.get(Oa)).runInitializers(),
                                    a.donePromise.then((function() {
                                        return ba(n.injector.get(Ua, ka) || ka),
                                        i._moduleDoBootstrap(n),
                                        n
                                    }
                                    )));
                                    return To(o) ? o.catch((function(n) {
                                        throw e.runOutsideAngular((function() {
                                            return t.handleError(n)
                                        }
                                        )),
                                        n
                                    }
                                    )) : o
                                } catch (u) {
                                    throw e.runOutsideAngular((function() {
                                        return t.handleError(u)
                                    }
                                    )),
                                    u
                                }
                                var a
                            }(r, o)
                        }
                        ))
                    }
                }, {
                    key: "bootstrapModule",
                    value: function(t) {
                        var e = this
                          , n = arguments.length > 1 && void 0 !== arguments[1] ? arguments[1] : []
                          , r = vu({}, n);
                        return au(0, 0, t).then((function(t) {
                            return e.bootstrapModuleFactory(t, r)
                        }
                        ))
                    }
                }, {
                    key: "_moduleDoBootstrap",
                    value: function(t) {
                        var e = t.injector.get(pu);
                        if (t._bootstrapComponents.length > 0)
                            t._bootstrapComponents.forEach((function(t) {
                                return e.bootstrap(t)
                            }
                            ));
                        else {
                            if (!t.instance.ngDoBootstrap)
                                throw new Error("The module ".concat(Dt(t.instance.constructor), ' was bootstrapped, but it does not declare "@NgModule.bootstrap" components nor a "ngDoBootstrap" method. ') + "Please define one of these.");
                            t.instance.ngDoBootstrap(e)
                        }
                        this._modules.push(t)
                    }
                }, {
                    key: "onDestroy",
                    value: function(t) {
                        this._destroyListeners.push(t)
                    }
                }, {
                    key: "destroy",
                    value: function() {
                        if (this._destroyed)
                            throw new Error("The platform has already been destroyed!");
                        this._modules.slice().forEach((function(t) {
                            return t.destroy()
                        }
                        )),
                        this._destroyListeners.forEach((function(t) {
                            return t()
                        }
                        )),
                        this._destroyed = !0
                    }
                }, {
                    key: "injector",
                    get: function() {
                        return this._injector
                    }
                }, {
                    key: "destroyed",
                    get: function() {
                        return this._destroyed
                    }
                }]),
                t
            }();
            return t.\u0275fac = function(e) {
                return new (e || t)(he(ho))
            }
            ,
            t.\u0275prov = Tt({
                token: t,
                factory: t.\u0275fac
            }),
            t
        }();
        function vu(t, e) {
            return Array.isArray(e) ? e.reduce(vu, t) : Object.assign(Object.assign({}, t), e)
        }
        var pu = function() {
            var t = function() {
                function t(e, n, r, i, o, a) {
                    var u = this;
                    m(this, t),
                    this._zone = e,
                    this._console = n,
                    this._injector = r,
                    this._exceptionHandler = i,
                    this._componentFactoryResolver = o,
                    this._initStatus = a,
                    this._bootstrapListeners = [],
                    this._views = [],
                    this._runningTick = !1,
                    this._enforceNoNewChanges = !1,
                    this._stable = !0,
                    this.componentTypes = [],
                    this.components = [],
                    this._enforceNoNewChanges = br(),
                    this._zone.onMicrotaskEmpty.subscribe({
                        next: function() {
                            u._zone.run((function() {
                                u.tick()
                            }
                            ))
                        }
                    });
                    var s = new H((function(t) {
                        u._stable = u._zone.isStable && !u._zone.hasPendingMacrotasks && !u._zone.hasPendingMicrotasks,
                        u._zone.runOutsideAngular((function() {
                            t.next(u._stable),
                            t.complete()
                        }
                        ))
                    }
                    ))
                      , l = new H((function(t) {
                        var e;
                        u._zone.runOutsideAngular((function() {
                            e = u._zone.onStable.subscribe((function() {
                                Qa.assertNotInAngularZone(),
                                Ga((function() {
                                    u._stable || u._zone.hasPendingMacrotasks || u._zone.hasPendingMicrotasks || (u._stable = !0,
                                    t.next(!0))
                                }
                                ))
                            }
                            ))
                        }
                        ));
                        var n = u._zone.onUnstable.subscribe((function() {
                            Qa.assertInAngularZone(),
                            u._stable && (u._stable = !1,
                            u._zone.runOutsideAngular((function() {
                                t.next(!1)
                            }
                            )))
                        }
                        ));
                        return function() {
                            e.unsubscribe(),
                            n.unsubscribe()
                        }
                    }
                    ));
                    this.isStable = function() {
                        for (var t = Number.POSITIVE_INFINITY, e = null, n = arguments.length, r = new Array(n), i = 0; i < n; i++)
                            r[i] = arguments[i];
                        var o = r[r.length - 1];
                        return Z(o) ? (e = r.pop(),
                        r.length > 1 && "number" == typeof r[r.length - 1] && (t = r.pop())) : "number" == typeof o && (t = r.pop()),
                        null === e && 1 === r.length && r[0]instanceof H ? r[0] : lt(t)(ct(r, e))
                    }(s, l.pipe((function(t) {
                        return ht()((e = yt,
                        function(t) {
                            var n;
                            n = "function" == typeof e ? e : function() {
                                return e
                            }
                            ;
                            var r = Object.create(t, pt);
                            return r.source = t,
                            r.subjectFactory = n,
                            r
                        }
                        )(t));
                        var e
                    }
                    )))
                }
                return _(t, [{
                    key: "bootstrap",
                    value: function(t, e) {
                        var n, r = this;
                        if (!this._initStatus.done)
                            throw new Error("Cannot bootstrap as there are still asynchronous initializers running. Bootstrap components in the `ngDoBootstrap` method of the root module.");
                        n = t instanceof Uo ? t : this._componentFactoryResolver.resolveComponentFactory(t),
                        this.componentTypes.push(n.componentType);
                        var i = n.isBoundToModule ? void 0 : this._injector.get(ge)
                          , o = n.create(ho.NULL, [], e || n.selector, i);
                        o.onDestroy((function() {
                            r._unloadComponent(o)
                        }
                        ));
                        var a = o.injector.get(ru, null);
                        return a && o.injector.get(iu).registerApplication(o.location.nativeElement, a),
                        this._loadComponent(o),
                        br() && this._console.log("Angular is running in development mode. Call enableProdMode() to enable production mode."),
                        o
                    }
                }, {
                    key: "tick",
                    value: function() {
                        var t = this;
                        if (this._runningTick)
                            throw new Error("ApplicationRef.tick is called recursively");
                        try {
                            this._runningTick = !0;
                            var e, n = h(this._views);
                            try {
                                for (n.s(); !(e = n.n()).done; )
                                    e.value.detectChanges()
                            } catch (o) {
                                n.e(o)
                            } finally {
                                n.f()
                            }
                            if (this._enforceNoNewChanges) {
                                var r, i = h(this._views);
                                try {
                                    for (i.s(); !(r = i.n()).done; )
                                        r.value.checkNoChanges()
                                } catch (o) {
                                    i.e(o)
                                } finally {
                                    i.f()
                                }
                            }
                        } catch (a) {
                            this._zone.runOutsideAngular((function() {
                                return t._exceptionHandler.handleError(a)
                            }
                            ))
                        } finally {
                            this._runningTick = !1
                        }
                    }
                }, {
                    key: "attachView",
                    value: function(t) {
                        var e = t;
                        this._views.push(e),
                        e.attachToAppRef(this)
                    }
                }, {
                    key: "detachView",
                    value: function(t) {
                        var e = t;
                        gu(this._views, e),
                        e.detachFromAppRef()
                    }
                }, {
                    key: "_loadComponent",
                    value: function(t) {
                        this.attachView(t.hostView),
                        this.tick(),
                        this.components.push(t),
                        this._injector.get(Na, []).concat(this._bootstrapListeners).forEach((function(e) {
                            return e(t)
                        }
                        ))
                    }
                }, {
                    key: "_unloadComponent",
                    value: function(t) {
                        this.detachView(t.hostView),
                        gu(this.components, t)
                    }
                }, {
                    key: "ngOnDestroy",
                    value: function() {
                        this._views.slice().forEach((function(t) {
                            return t.destroy()
                        }
                        ))
                    }
                }, {
                    key: "viewCount",
                    get: function() {
                        return this._views.length
                    }
                }]),
                t
            }();
            return t.\u0275fac = function(e) {
                return new (e || t)(he(Qa),he(Ma),he(ho),he(wr),he(Ho),he(Oa))
            }
            ,
            t.\u0275prov = Tt({
                token: t,
                factory: t.\u0275fac
            }),
            t
        }();
        function gu(t, e) {
            var n = t.indexOf(e);
            n > -1 && t.splice(n, 1)
        }
        var yu = function t() {
            m(this, t)
        }
          , mu = function t() {
            m(this, t)
        }
          , wu = {
            factoryPathPrefix: "",
            factoryPathSuffix: ".ngfactory"
        }
          , _u = function() {
            var t = function() {
                function t(e, n) {
                    m(this, t),
                    this._compiler = e,
                    this._config = n || wu
                }
                return _(t, [{
                    key: "load",
                    value: function(t) {
                        return this.loadAndCompile(t)
                    }
                }, {
                    key: "loadAndCompile",
                    value: function(t) {
                        var e = this
                          , r = s(t.split("#"), 2)
                          , i = r[0]
                          , o = r[1];
                        return void 0 === o && (o = "default"),
                        n("zn8P")(i).then((function(t) {
                            return t[o]
                        }
                        )).then((function(t) {
                            return ku(t, i, o)
                        }
                        )).then((function(t) {
                            return e._compiler.compileModuleAsync(t)
                        }
                        ))
                    }
                }, {
                    key: "loadFactory",
                    value: function(t) {
                        var e = s(t.split("#"), 2)
                          , r = e[0]
                          , i = e[1]
                          , o = "NgFactory";
                        return void 0 === i && (i = "default",
                        o = ""),
                        n("zn8P")(this._config.factoryPathPrefix + r + this._config.factoryPathSuffix).then((function(t) {
                            return t[i + o]
                        }
                        )).then((function(t) {
                            return ku(t, r, i)
                        }
                        ))
                    }
                }]),
                t
            }();
            return t.\u0275fac = function(e) {
                return new (e || t)(he(Za),he(mu, 8))
            }
            ,
            t.\u0275prov = Tt({
                token: t,
                factory: t.\u0275fac
            }),
            t
        }();
        function ku(t, e, n) {
            if (!t)
                throw new Error("Cannot find '".concat(n, "' in '").concat(e, "'"));
            return t
        }
        var bu = cu(null, "core", [{
            provide: ja,
            useValue: "unknown"
        }, {
            provide: du,
            deps: [ho]
        }, {
            provide: iu,
            deps: []
        }, {
            provide: Ma,
            deps: []
        }])
          , Cu = [{
            provide: pu,
            useClass: pu,
            deps: [Qa, Ma, ho, wr, Ho, Oa]
        }, {
            provide: da,
            deps: [Qa],
            useFactory: function(t) {
                var e = [];
                return t.onStable.subscribe((function() {
                    for (; e.length; )
                        e.pop()()
                }
                )),
                function(t) {
                    e.push(t)
                }
            }
        }, {
            provide: Oa,
            useClass: Oa,
            deps: [[new bt, Ta]]
        }, {
            provide: Za,
            useClass: Za,
            deps: []
        }, Ra, {
            provide: ea,
            useFactory: function() {
                return ia
            },
            deps: []
        }, {
            provide: na,
            useFactory: function() {
                return oa
            },
            deps: []
        }, {
            provide: Ua,
            useFactory: function(t) {
                return ba(t = t || "undefined" != typeof $localize && $localize.locale || ka),
                t
            },
            deps: [[new kt(Ua), new bt, new St]]
        }, {
            provide: Da,
            useValue: "USD"
        }]
          , Su = function() {
            var t = function t(e) {
                m(this, t)
            };
            return t.\u0275mod = Re({
                type: t
            }),
            t.\u0275inj = Ot({
                factory: function(e) {
                    return new (e || t)(he(pu))
                },
                providers: Cu
            }),
            t
        }()
          , xu = "https://webportal.jiit.ac.in"
          , Eu = null;
        function Tu() {
            return Eu
        }
        var Ou = function t() {
            m(this, t)
        }
          , Au = new ee("DocumentToken")
          , Ru = function() {
            var t = function t() {
                m(this, t)
            };
            return t.\u0275fac = function(e) {
                return new (e || t)
            }
            ,
            t.\u0275prov = Tt({
                factory: Pu,
                token: t,
                providedIn: "platform"
            }),
            t
        }();
        function Pu() {
            return he(ju)
        }
        var Iu = new ee("Location Initialized")
          , ju = function() {
            var t = function(t) {
                d(n, t);
                var e = y(n);
                function n(t) {
                    var r;
                    return m(this, n),
                    (r = e.call(this))._doc = t,
                    r._init(),
                    r
                }
                return _(n, [{
                    key: "_init",
                    value: function() {
                        this.location = Tu().getLocation(),
                        this._history = Tu().getHistory()
                    }
                }, {
                    key: "getBaseHrefFromDOM",
                    value: function() {
                        return Tu().getBaseHref(this._doc)
                    }
                }, {
                    key: "onPopState",
                    value: function(t) {
                        Tu().getGlobalEventTarget(this._doc, "window").addEventListener("popstate", t, !1)
                    }
                }, {
                    key: "onHashChange",
                    value: function(t) {
                        Tu().getGlobalEventTarget(this._doc, "window").addEventListener("hashchange", t, !1)
                    }
                }, {
                    key: "pushState",
                    value: function(t, e, n) {
                        Nu() ? this._history.pushState(t, e, n) : this.location.hash = n
                    }
                }, {
                    key: "replaceState",
                    value: function(t, e, n) {
                        Nu() ? this._history.replaceState(t, e, n) : this.location.hash = n
                    }
                }, {
                    key: "forward",
                    value: function() {
                        this._history.forward()
                    }
                }, {
                    key: "back",
                    value: function() {
                        this._history.back()
                    }
                }, {
                    key: "getState",
                    value: function() {
                        return this._history.state
                    }
                }, {
                    key: "href",
                    get: function() {
                        return this.location.href
                    }
                }, {
                    key: "protocol",
                    get: function() {
                        return this.location.protocol
                    }
                }, {
                    key: "hostname",
                    get: function() {
                        return this.location.hostname
                    }
                }, {
                    key: "port",
                    get: function() {
                        return this.location.port
                    }
                }, {
                    key: "pathname",
                    get: function() {
                        return this.location.pathname
                    },
                    set: function(t) {
                        this.location.pathname = t
                    }
                }, {
                    key: "search",
                    get: function() {
                        return this.location.search
                    }
                }, {
                    key: "hash",
                    get: function() {
                        return this.location.hash
                    }
                }]),
                n
            }(Ru);
            return t.\u0275fac = function(e) {
                return new (e || t)(he(Au))
            }
            ,
            t.\u0275prov = Tt({
                factory: Mu,
                token: t,
                providedIn: "platform"
            }),
            t
        }();
        function Nu() {
            return !!window.history.pushState
        }
        function Mu() {
            return new ju(he(Au))
        }
        function Uu(t, e) {
            if (0 == t.length)
                return e;
            if (0 == e.length)
                return t;
            var n = 0;
            return t.endsWith("/") && n++,
            e.startsWith("/") && n++,
            2 == n ? t + e.substring(1) : 1 == n ? t + e : t + "/" + e
        }
        function Du(t) {
            var e = t.match(/#|\?|$/)
              , n = e && e.index || t.length;
            return t.slice(0, n - ("/" === t[n - 1] ? 1 : 0)) + t.slice(n)
        }
        function Hu(t) {
            return t && "?" !== t[0] ? "?" + t : t
        }
        var Lu = function() {
            var t = function t() {
                m(this, t)
            };
            return t.\u0275fac = function(e) {
                return new (e || t)
            }
            ,
            t.\u0275prov = Tt({
                factory: Fu,
                token: t,
                providedIn: "root"
            }),
            t
        }();
        function Fu(t) {
            var e = he(Au).location;
            return new zu(he(Ru),e && e.origin || "")
        }
        var Vu = new ee("appBaseHref")
          , zu = function() {
            var t = function(t) {
                d(n, t);
                var e = y(n);
                function n(t, r) {
                    var i;
                    if (m(this, n),
                    (i = e.call(this))._platformLocation = t,
                    null == r && (r = i._platformLocation.getBaseHrefFromDOM()),
                    null == r)
                        throw new Error("No base href set. Please provide a value for the APP_BASE_HREF token or add a base element to the document.");
                    return i._baseHref = r,
                    i
                }
                return _(n, [{
                    key: "onPopState",
                    value: function(t) {
                        this._platformLocation.onPopState(t),
                        this._platformLocation.onHashChange(t)
                    }
                }, {
                    key: "getBaseHref",
                    value: function() {
                        return this._baseHref
                    }
                }, {
                    key: "prepareExternalUrl",
                    value: function(t) {
                        return Uu(this._baseHref, t)
                    }
                }, {
                    key: "path",
                    value: function() {
                        var t = arguments.length > 0 && void 0 !== arguments[0] && arguments[0]
                          , e = this._platformLocation.pathname + Hu(this._platformLocation.search)
                          , n = this._platformLocation.hash;
                        return n && t ? "".concat(e).concat(n) : e
                    }
                }, {
                    key: "pushState",
                    value: function(t, e, n, r) {
                        var i = this.prepareExternalUrl(n + Hu(r));
                        this._platformLocation.pushState(t, e, i)
                    }
                }, {
                    key: "replaceState",
                    value: function(t, e, n, r) {
                        var i = this.prepareExternalUrl(n + Hu(r));
                        this._platformLocation.replaceState(t, e, i)
                    }
                }, {
                    key: "forward",
                    value: function() {
                        this._platformLocation.forward()
                    }
                }, {
                    key: "back",
                    value: function() {
                        this._platformLocation.back()
                    }
                }]),
                n
            }(Lu);
            return t.\u0275fac = function(e) {
                return new (e || t)(he(Ru),he(Vu, 8))
            }
            ,
            t.\u0275prov = Tt({
                token: t,
                factory: t.\u0275fac
            }),
            t
        }()
          , qu = function() {
            var t = function(t) {
                d(n, t);
                var e = y(n);
                function n(t, r) {
                    var i;
                    return m(this, n),
                    (i = e.call(this))._platformLocation = t,
                    i._baseHref = "",
                    null != r && (i._baseHref = r),
                    i
                }
                return _(n, [{
                    key: "onPopState",
                    value: function(t) {
                        this._platformLocation.onPopState(t),
                        this._platformLocation.onHashChange(t)
                    }
                }, {
                    key: "getBaseHref",
                    value: function() {
                        return this._baseHref
                    }
                }, {
                    key: "path",
                    value: function() {
                        var t = this._platformLocation.hash;
                        return null == t && (t = "#"),
                        t.length > 0 ? t.substring(1) : t
                    }
                }, {
                    key: "prepareExternalUrl",
                    value: function(t) {
                        var e = Uu(this._baseHref, t);
                        return e.length > 0 ? "#" + e : e
                    }
                }, {
                    key: "pushState",
                    value: function(t, e, n, r) {
                        var i = this.prepareExternalUrl(n + Hu(r));
                        0 == i.length && (i = this._platformLocation.pathname),
                        this._platformLocation.pushState(t, e, i)
                    }
                }, {
                    key: "replaceState",
                    value: function(t, e, n, r) {
                        var i = this.prepareExternalUrl(n + Hu(r));
                        0 == i.length && (i = this._platformLocation.pathname),
                        this._platformLocation.replaceState(t, e, i)
                    }
                }, {
                    key: "forward",
                    value: function() {
                        this._platformLocation.forward()
                    }
                }, {
                    key: "back",
                    value: function() {
                        this._platformLocation.back()
                    }
                }]),
                n
            }(Lu);
            return t.\u0275fac = function(e) {
                return new (e || t)(he(Ru),he(Vu, 8))
            }
            ,
            t.\u0275prov = Tt({
                token: t,
                factory: t.\u0275fac
            }),
            t
        }()
          , Bu = function() {
            var t = function() {
                function t(e, n) {
                    var r = this;
                    m(this, t),
                    this._subject = new Ea,
                    this._urlChangeListeners = [],
                    this._platformStrategy = e;
                    var i = this._platformStrategy.getBaseHref();
                    this._platformLocation = n,
                    this._baseHref = Du(Wu(i)),
                    this._platformStrategy.onPopState((function(t) {
                        r._subject.emit({
                            url: r.path(!0),
                            pop: !0,
                            state: t.state,
                            type: t.type
                        })
                    }
                    ))
                }
                return _(t, [{
                    key: "path",
                    value: function() {
                        var t = arguments.length > 0 && void 0 !== arguments[0] && arguments[0];
                        return this.normalize(this._platformStrategy.path(t))
                    }
                }, {
                    key: "getState",
                    value: function() {
                        return this._platformLocation.getState()
                    }
                }, {
                    key: "isCurrentPathEqualTo",
                    value: function(t) {
                        var e = arguments.length > 1 && void 0 !== arguments[1] ? arguments[1] : "";
                        return this.path() == this.normalize(t + Hu(e))
                    }
                }, {
                    key: "normalize",
                    value: function(e) {
                        return t.stripTrailingSlash(function(t, e) {
                            return t && e.startsWith(t) ? e.substring(t.length) : e
                        }(this._baseHref, Wu(e)))
                    }
                }, {
                    key: "prepareExternalUrl",
                    value: function(t) {
                        return t && "/" !== t[0] && (t = "/" + t),
                        this._platformStrategy.prepareExternalUrl(t)
                    }
                }, {
                    key: "go",
                    value: function(t) {
                        var e = arguments.length > 1 && void 0 !== arguments[1] ? arguments[1] : ""
                          , n = arguments.length > 2 && void 0 !== arguments[2] ? arguments[2] : null;
                        this._platformStrategy.pushState(n, "", t, e),
                        this._notifyUrlChangeListeners(this.prepareExternalUrl(t + Hu(e)), n)
                    }
                }, {
                    key: "replaceState",
                    value: function(t) {
                        var e = arguments.length > 1 && void 0 !== arguments[1] ? arguments[1] : ""
                          , n = arguments.length > 2 && void 0 !== arguments[2] ? arguments[2] : null;
                        this._platformStrategy.replaceState(n, "", t, e),
                        this._notifyUrlChangeListeners(this.prepareExternalUrl(t + Hu(e)), n)
                    }
                }, {
                    key: "forward",
                    value: function() {
                        this._platformStrategy.forward()
                    }
                }, {
                    key: "back",
                    value: function() {
                        this._platformStrategy.back()
                    }
                }, {
                    key: "onUrlChange",
                    value: function(t) {
                        var e = this;
                        this._urlChangeListeners.push(t),
                        this._urlChangeSubscription || (this._urlChangeSubscription = this.subscribe((function(t) {
                            e._notifyUrlChangeListeners(t.url, t.state)
                        }
                        )))
                    }
                }, {
                    key: "_notifyUrlChangeListeners",
                    value: function() {
                        var t = arguments.length > 0 && void 0 !== arguments[0] ? arguments[0] : ""
                          , e = arguments.length > 1 ? arguments[1] : void 0;
                        this._urlChangeListeners.forEach((function(n) {
                            return n(t, e)
                        }
                        ))
                    }
                }, {
                    key: "subscribe",
                    value: function(t, e, n) {
                        return this._subject.subscribe({
                            next: t,
                            error: e,
                            complete: n
                        })
                    }
                }]),
                t
            }();
            return t.\u0275fac = function(e) {
                return new (e || t)(he(Lu),he(Ru))
            }
            ,
            t.normalizeQueryParams = Hu,
            t.joinWithSlash = Uu,
            t.stripTrailingSlash = Du,
            t.\u0275prov = Tt({
                factory: Zu,
                token: t,
                providedIn: "root"
            }),
            t
        }();
        function Zu() {
            return new Bu(he(Lu),he(Ru))
        }
        function Wu(t) {
            return t.replace(/\/index.html$/, "")
        }
        var Gu = function(t) {
            return t[t.Zero = 0] = "Zero",
            t[t.One = 1] = "One",
            t[t.Two = 2] = "Two",
            t[t.Few = 3] = "Few",
            t[t.Many = 4] = "Many",
            t[t.Other = 5] = "Other",
            t
        }({})
          , Qu = function t() {
            m(this, t)
        }
          , Ju = function() {
            var t = function(t) {
                d(n, t);
                var e = y(n);
                function n(t) {
                    var r;
                    return m(this, n),
                    (r = e.call(this)).locale = t,
                    r
                }
                return _(n, [{
                    key: "getPluralCategory",
                    value: function(t, e) {
                        switch (function(t) {
                            return function(t) {
                                var e = function(t) {
                                    return t.toLowerCase().replace(/_/g, "-")
                                }(t)
                                  , n = wa(e);
                                if (n)
                                    return n;
                                var r = e.split("-")[0];
                                if (n = wa(r))
                                    return n;
                                if ("en" === r)
                                    return ya;
                                throw new Error('Missing locale data for the locale "'.concat(t, '".'))
                            }(t)[_a.PluralCase]
                        }(e || this.locale)(t)) {
                        case Gu.Zero:
                            return "zero";
                        case Gu.One:
                            return "one";
                        case Gu.Two:
                            return "two";
                        case Gu.Few:
                            return "few";
                        case Gu.Many:
                            return "many";
                        default:
                            return "other"
                        }
                    }
                }]),
                n
            }(Qu);
            return t.\u0275fac = function(e) {
                return new (e || t)(he(Ua))
            }
            ,
            t.\u0275prov = Tt({
                token: t,
                factory: t.\u0275fac
            }),
            t
        }();
        function Ku(t, e) {
            e = encodeURIComponent(e);
            var n, r = h(t.split(";"));
            try {
                for (r.s(); !(n = r.n()).done; ) {
                    var i = n.value
                      , o = i.indexOf("=")
                      , a = s(-1 == o ? [i, ""] : [i.slice(0, o), i.slice(o + 1)], 2)
                      , u = a[1];
                    if (a[0].trim() === e)
                        return decodeURIComponent(u)
                }
            } catch (l) {
                r.e(l)
            } finally {
                r.f()
            }
            return null
        }
        var Yu = function() {
            var t = function() {
                function t(e, n) {
                    m(this, t),
                    this._viewContainer = e,
                    this._context = new Xu,
                    this._thenTemplateRef = null,
                    this._elseTemplateRef = null,
                    this._thenViewRef = null,
                    this._elseViewRef = null,
                    this._thenTemplateRef = n
                }
                return _(t, [{
                    key: "_updateView",
                    value: function() {
                        this._context.$implicit ? this._thenViewRef || (this._viewContainer.clear(),
                        this._elseViewRef = null,
                        this._thenTemplateRef && (this._thenViewRef = this._viewContainer.createEmbeddedView(this._thenTemplateRef, this._context))) : this._elseViewRef || (this._viewContainer.clear(),
                        this._thenViewRef = null,
                        this._elseTemplateRef && (this._elseViewRef = this._viewContainer.createEmbeddedView(this._elseTemplateRef, this._context)))
                    }
                }, {
                    key: "ngIf",
                    set: function(t) {
                        this._context.$implicit = this._context.ngIf = t,
                        this._updateView()
                    }
                }, {
                    key: "ngIfThen",
                    set: function(t) {
                        $u("ngIfThen", t),
                        this._thenTemplateRef = t,
                        this._thenViewRef = null,
                        this._updateView()
                    }
                }, {
                    key: "ngIfElse",
                    set: function(t) {
                        $u("ngIfElse", t),
                        this._elseTemplateRef = t,
                        this._elseViewRef = null,
                        this._updateView()
                    }
                }], [{
                    key: "ngTemplateContextGuard",
                    value: function(t, e) {
                        return !0
                    }
                }]),
                t
            }();
            return t.\u0275fac = function(e) {
                return new (e || t)(ko(sa),ko(aa))
            }
            ,
            t.\u0275dir = Ie({
                type: t,
                selectors: [["", "ngIf", ""]],
                inputs: {
                    ngIf: "ngIf",
                    ngIfThen: "ngIfThen",
                    ngIfElse: "ngIfElse"
                }
            }),
            t
        }()
          , Xu = function t() {
            m(this, t),
            this.$implicit = null,
            this.ngIf = null
        };
        function $u(t, e) {
            if (e && !e.createEmbeddedView)
                throw new Error("".concat(t, " must be a TemplateRef, but received '").concat(Dt(e), "'."))
        }
        var ts = function() {
            var t = function t() {
                m(this, t)
            };
            return t.\u0275mod = Re({
                type: t
            }),
            t.\u0275inj = Ot({
                factory: function(e) {
                    return new (e || t)
                },
                providers: [{
                    provide: Qu,
                    useClass: Ju
                }]
            }),
            t
        }()
          , es = function() {
            var t = function t() {
                m(this, t)
            };
            return t.\u0275prov = Tt({
                token: t,
                providedIn: "root",
                factory: function() {
                    return new ns(he(Au),window,he(wr))
                }
            }),
            t
        }()
          , ns = function() {
            function t(e, n, r) {
                m(this, t),
                this.document = e,
                this.window = n,
                this.errorHandler = r,
                this.offset = function() {
                    return [0, 0]
                }
            }
            return _(t, [{
                key: "setOffset",
                value: function(t) {
                    this.offset = Array.isArray(t) ? function() {
                        return t
                    }
                    : t
                }
            }, {
                key: "getScrollPosition",
                value: function() {
                    return this.supportsScrolling() ? [this.window.scrollX, this.window.scrollY] : [0, 0]
                }
            }, {
                key: "scrollToPosition",
                value: function(t) {
                    this.supportsScrolling() && this.window.scrollTo(t[0], t[1])
                }
            }, {
                key: "scrollToAnchor",
                value: function(t) {
                    if (this.supportsScrolling()) {
                        var e = this.document.getElementById(t) || this.document.getElementsByName(t)[0];
                        e && this.scrollToElement(e)
                    }
                }
            }, {
                key: "setHistoryScrollRestoration",
                value: function(t) {
                    if (this.supportScrollRestoration()) {
                        var e = this.window.history;
                        e && e.scrollRestoration && (e.scrollRestoration = t)
                    }
                }
            }, {
                key: "scrollToElement",
                value: function(t) {
                    var e = t.getBoundingClientRect()
                      , n = e.left + this.window.pageXOffset
                      , r = e.top + this.window.pageYOffset
                      , i = this.offset();
                    this.window.scrollTo(n - i[0], r - i[1])
                }
            }, {
                key: "supportScrollRestoration",
                value: function() {
                    try {
                        if (!this.window || !this.window.scrollTo)
                            return !1;
                        var t = rs(this.window.history) || rs(Object.getPrototypeOf(this.window.history));
                        return !(!t || !t.writable && !t.set)
                    } catch (e) {
                        return !1
                    }
                }
            }, {
                key: "supportsScrolling",
                value: function() {
                    try {
                        return !!this.window.scrollTo
                    } catch (t) {
                        return !1
                    }
                }
            }]),
            t
        }();
        function rs(t) {
            return Object.getOwnPropertyDescriptor(t, "scrollRestoration")
        }
        var is, os = function(t) {
            d(n, t);
            var e = y(n);
            function n() {
                return m(this, n),
                e.apply(this, arguments)
            }
            return _(n, [{
                key: "getProperty",
                value: function(t, e) {
                    return t[e]
                }
            }, {
                key: "log",
                value: function(t) {
                    window.console && window.console.log && window.console.log(t)
                }
            }, {
                key: "logGroup",
                value: function(t) {
                    window.console && window.console.group && window.console.group(t)
                }
            }, {
                key: "logGroupEnd",
                value: function() {
                    window.console && window.console.groupEnd && window.console.groupEnd()
                }
            }, {
                key: "onAndCancel",
                value: function(t, e, n) {
                    return t.addEventListener(e, n, !1),
                    function() {
                        t.removeEventListener(e, n, !1)
                    }
                }
            }, {
                key: "dispatchEvent",
                value: function(t, e) {
                    t.dispatchEvent(e)
                }
            }, {
                key: "remove",
                value: function(t) {
                    return t.parentNode && t.parentNode.removeChild(t),
                    t
                }
            }, {
                key: "getValue",
                value: function(t) {
                    return t.value
                }
            }, {
                key: "createElement",
                value: function(t, e) {
                    return (e = e || this.getDefaultDocument()).createElement(t)
                }
            }, {
                key: "createHtmlDocument",
                value: function() {
                    return document.implementation.createHTMLDocument("fakeTitle")
                }
            }, {
                key: "getDefaultDocument",
                value: function() {
                    return document
                }
            }, {
                key: "isElementNode",
                value: function(t) {
                    return t.nodeType === Node.ELEMENT_NODE
                }
            }, {
                key: "isShadowRoot",
                value: function(t) {
                    return t instanceof DocumentFragment
                }
            }, {
                key: "getGlobalEventTarget",
                value: function(t, e) {
                    return "window" === e ? window : "document" === e ? t : "body" === e ? t.body : null
                }
            }, {
                key: "getHistory",
                value: function() {
                    return window.history
                }
            }, {
                key: "getLocation",
                value: function() {
                    return window.location
                }
            }, {
                key: "getBaseHref",
                value: function(t) {
                    var e, n = as || (as = document.querySelector("base")) ? as.getAttribute("href") : null;
                    return null == n ? null : (e = n,
                    is || (is = document.createElement("a")),
                    is.setAttribute("href", e),
                    "/" === is.pathname.charAt(0) ? is.pathname : "/" + is.pathname)
                }
            }, {
                key: "resetBaseElement",
                value: function() {
                    as = null
                }
            }, {
                key: "getUserAgent",
                value: function() {
                    return window.navigator.userAgent
                }
            }, {
                key: "performanceNow",
                value: function() {
                    return window.performance && window.performance.now ? window.performance.now() : (new Date).getTime()
                }
            }, {
                key: "supportsCookies",
                value: function() {
                    return !0
                }
            }, {
                key: "getCookie",
                value: function(t) {
                    return Ku(document.cookie, t)
                }
            }], [{
                key: "makeCurrent",
                value: function() {
                    var t;
                    t = new n,
                    Eu || (Eu = t)
                }
            }]),
            n
        }(function(t) {
            d(n, t);
            var e = y(n);
            function n() {
                return m(this, n),
                e.call(this)
            }
            return _(n, [{
                key: "supportsDOMEvents",
                value: function() {
                    return !0
                }
            }]),
            n
        }(Ou)), as = null, us = new ee("TRANSITION_ID"), ss = [{
            provide: Ta,
            useFactory: function(t, e, n) {
                return function() {
                    n.get(Oa).donePromise.then((function() {
                        var n = Tu();
                        Array.prototype.slice.apply(e.querySelectorAll("style[ng-transition]")).filter((function(e) {
                            return e.getAttribute("ng-transition") === t
                        }
                        )).forEach((function(t) {
                            return n.remove(t)
                        }
                        ))
                    }
                    ))
                }
            },
            deps: [us, Au, ho],
            multi: !0
        }], ls = function() {
            function t() {
                m(this, t)
            }
            return _(t, [{
                key: "addToWindow",
                value: function(t) {
                    Wt.getAngularTestability = function(e) {
                        var n = !(arguments.length > 1 && void 0 !== arguments[1]) || arguments[1]
                          , r = t.findTestabilityInTree(e, n);
                        if (null == r)
                            throw new Error("Could not find testability for element.");
                        return r
                    }
                    ,
                    Wt.getAllAngularTestabilities = function() {
                        return t.getAllTestabilities()
                    }
                    ,
                    Wt.getAllAngularRootElements = function() {
                        return t.getAllRootElements()
                    }
                    ,
                    Wt.frameworkStabilizers || (Wt.frameworkStabilizers = []),
                    Wt.frameworkStabilizers.push((function(t) {
                        var e = Wt.getAllAngularTestabilities()
                          , n = e.length
                          , r = !1
                          , i = function(e) {
                            r = r || e,
                            0 == --n && t(r)
                        };
                        e.forEach((function(t) {
                            t.whenStable(i)
                        }
                        ))
                    }
                    ))
                }
            }, {
                key: "findTestabilityInTree",
                value: function(t, e, n) {
                    if (null == e)
                        return null;
                    var r = t.getTestability(e);
                    return null != r ? r : n ? Tu().isShadowRoot(e) ? this.findTestabilityInTree(t, e.host, !0) : this.findTestabilityInTree(t, e.parentElement, !0) : null
                }
            }], [{
                key: "init",
                value: function() {
                    var e;
                    e = new t,
                    ou = e
                }
            }]),
            t
        }(), cs = new ee("EventManagerPlugins"), hs = function() {
            var t = function() {
                function t(e, n) {
                    var r = this;
                    m(this, t),
                    this._zone = n,
                    this._eventNameToPlugin = new Map,
                    e.forEach((function(t) {
                        return t.manager = r
                    }
                    )),
                    this._plugins = e.slice().reverse()
                }
                return _(t, [{
                    key: "addEventListener",
                    value: function(t, e, n) {
                        return this._findPluginFor(e).addEventListener(t, e, n)
                    }
                }, {
                    key: "addGlobalEventListener",
                    value: function(t, e, n) {
                        return this._findPluginFor(e).addGlobalEventListener(t, e, n)
                    }
                }, {
                    key: "getZone",
                    value: function() {
                        return this._zone
                    }
                }, {
                    key: "_findPluginFor",
                    value: function(t) {
                        var e = this._eventNameToPlugin.get(t);
                        if (e)
                            return e;
                        for (var n = this._plugins, r = 0; r < n.length; r++) {
                            var i = n[r];
                            if (i.supports(t))
                                return this._eventNameToPlugin.set(t, i),
                                i
                        }
                        throw new Error("No event manager plugin found for event ".concat(t))
                    }
                }]),
                t
            }();
            return t.\u0275fac = function(e) {
                return new (e || t)(he(cs),he(Qa))
            }
            ,
            t.\u0275prov = Tt({
                token: t,
                factory: t.\u0275fac
            }),
            t
        }(), fs = function() {
            function t(e) {
                m(this, t),
                this._doc = e
            }
            return _(t, [{
                key: "addGlobalEventListener",
                value: function(t, e, n) {
                    var r = Tu().getGlobalEventTarget(this._doc, t);
                    if (!r)
                        throw new Error("Unsupported event target ".concat(r, " for event ").concat(e));
                    return this.addEventListener(r, e, n)
                }
            }]),
            t
        }(), ds = function() {
            var t = function() {
                function t() {
                    m(this, t),
                    this._stylesSet = new Set
                }
                return _(t, [{
                    key: "addStyles",
                    value: function(t) {
                        var e = this
                          , n = new Set;
                        t.forEach((function(t) {
                            e._stylesSet.has(t) || (e._stylesSet.add(t),
                            n.add(t))
                        }
                        )),
                        this.onStylesAdded(n)
                    }
                }, {
                    key: "onStylesAdded",
                    value: function(t) {}
                }, {
                    key: "getAllStyles",
                    value: function() {
                        return Array.from(this._stylesSet)
                    }
                }]),
                t
            }();
            return t.\u0275fac = function(e) {
                return new (e || t)
            }
            ,
            t.\u0275prov = Tt({
                token: t,
                factory: t.\u0275fac
            }),
            t
        }(), vs = function() {
            var t = function(t) {
                d(n, t);
                var e = y(n);
                function n(t) {
                    var r;
                    return m(this, n),
                    (r = e.call(this))._doc = t,
                    r._hostNodes = new Set,
                    r._styleNodes = new Set,
                    r._hostNodes.add(t.head),
                    r
                }
                return _(n, [{
                    key: "_addStylesToHost",
                    value: function(t, e) {
                        var n = this;
                        t.forEach((function(t) {
                            var r = n._doc.createElement("style");
                            r.textContent = t,
                            n._styleNodes.add(e.appendChild(r))
                        }
                        ))
                    }
                }, {
                    key: "addHost",
                    value: function(t) {
                        this._addStylesToHost(this._stylesSet, t),
                        this._hostNodes.add(t)
                    }
                }, {
                    key: "removeHost",
                    value: function(t) {
                        this._hostNodes.delete(t)
                    }
                }, {
                    key: "onStylesAdded",
                    value: function(t) {
                        var e = this;
                        this._hostNodes.forEach((function(n) {
                            return e._addStylesToHost(t, n)
                        }
                        ))
                    }
                }, {
                    key: "ngOnDestroy",
                    value: function() {
                        this._styleNodes.forEach((function(t) {
                            return Tu().remove(t)
                        }
                        ))
                    }
                }]),
                n
            }(ds);
            return t.\u0275fac = function(e) {
                return new (e || t)(he(Au))
            }
            ,
            t.\u0275prov = Tt({
                token: t,
                factory: t.\u0275fac
            }),
            t
        }(), ps = {
            svg: "http://www.w3.org/2000/svg",
            xhtml: "http://www.w3.org/1999/xhtml",
            xlink: "http://www.w3.org/1999/xlink",
            xml: "http://www.w3.org/XML/1998/namespace",
            xmlns: "http://www.w3.org/2000/xmlns/"
        }, gs = /%COMP%/g, ys = "%COMP%", ms = "_nghost-".concat(ys), ws = "_ngcontent-".concat(ys);
        function _s(t, e, n) {
            for (var r = 0; r < e.length; r++) {
                var i = e[r];
                Array.isArray(i) ? _s(t, i, n) : (i = i.replace(gs, t),
                n.push(i))
            }
            return n
        }
        function ks(t) {
            return function(e) {
                if ("__ngUnwrap__" === e)
                    return t;
                !1 === t(e) && (e.preventDefault(),
                e.returnValue = !1)
            }
        }
        var bs = function() {
            var t = function() {
                function t(e, n, r) {
                    m(this, t),
                    this.eventManager = e,
                    this.sharedStylesHost = n,
                    this.appId = r,
                    this.rendererByCompId = new Map,
                    this.defaultRenderer = new Cs(e)
                }
                return _(t, [{
                    key: "createRenderer",
                    value: function(t, e) {
                        if (!t || !e)
                            return this.defaultRenderer;
                        switch (e.encapsulation) {
                        case be.Emulated:
                            var n = this.rendererByCompId.get(e.id);
                            return n || (n = new Ss(this.eventManager,this.sharedStylesHost,e,this.appId),
                            this.rendererByCompId.set(e.id, n)),
                            n.applyToHost(t),
                            n;
                        case be.Native:
                        case be.ShadowDom:
                            return new xs(this.eventManager,this.sharedStylesHost,t,e);
                        default:
                            if (!this.rendererByCompId.has(e.id)) {
                                var r = _s(e.id, e.styles, []);
                                this.sharedStylesHost.addStyles(r),
                                this.rendererByCompId.set(e.id, this.defaultRenderer)
                            }
                            return this.defaultRenderer
                        }
                    }
                }, {
                    key: "begin",
                    value: function() {}
                }, {
                    key: "end",
                    value: function() {}
                }]),
                t
            }();
            return t.\u0275fac = function(e) {
                return new (e || t)(he(hs),he(vs),he(Aa))
            }
            ,
            t.\u0275prov = Tt({
                token: t,
                factory: t.\u0275fac
            }),
            t
        }()
          , Cs = function() {
            function t(e) {
                m(this, t),
                this.eventManager = e,
                this.data = Object.create(null)
            }
            return _(t, [{
                key: "destroy",
                value: function() {}
            }, {
                key: "createElement",
                value: function(t, e) {
                    return e ? document.createElementNS(ps[e] || e, t) : document.createElement(t)
                }
            }, {
                key: "createComment",
                value: function(t) {
                    return document.createComment(t)
                }
            }, {
                key: "createText",
                value: function(t) {
                    return document.createTextNode(t)
                }
            }, {
                key: "appendChild",
                value: function(t, e) {
                    t.appendChild(e)
                }
            }, {
                key: "insertBefore",
                value: function(t, e, n) {
                    t && t.insertBefore(e, n)
                }
            }, {
                key: "removeChild",
                value: function(t, e) {
                    t && t.removeChild(e)
                }
            }, {
                key: "selectRootElement",
                value: function(t, e) {
                    var n = "string" == typeof t ? document.querySelector(t) : t;
                    if (!n)
                        throw new Error('The selector "'.concat(t, '" did not match any elements'));
                    return e || (n.textContent = ""),
                    n
                }
            }, {
                key: "parentNode",
                value: function(t) {
                    return t.parentNode
                }
            }, {
                key: "nextSibling",
                value: function(t) {
                    return t.nextSibling
                }
            }, {
                key: "setAttribute",
                value: function(t, e, n, r) {
                    if (r) {
                        e = r + ":" + e;
                        var i = ps[r];
                        i ? t.setAttributeNS(i, e, n) : t.setAttribute(e, n)
                    } else
                        t.setAttribute(e, n)
                }
            }, {
                key: "removeAttribute",
                value: function(t, e, n) {
                    if (n) {
                        var r = ps[n];
                        r ? t.removeAttributeNS(r, e) : t.removeAttribute("".concat(n, ":").concat(e))
                    } else
                        t.removeAttribute(e)
                }
            }, {
                key: "addClass",
                value: function(t, e) {
                    t.classList.add(e)
                }
            }, {
                key: "removeClass",
                value: function(t, e) {
                    t.classList.remove(e)
                }
            }, {
                key: "setStyle",
                value: function(t, e, n, r) {
                    r & zo.DashCase ? t.style.setProperty(e, n, r & zo.Important ? "important" : "") : t.style[e] = n
                }
            }, {
                key: "removeStyle",
                value: function(t, e, n) {
                    n & zo.DashCase ? t.style.removeProperty(e) : t.style[e] = ""
                }
            }, {
                key: "setProperty",
                value: function(t, e, n) {
                    t[e] = n
                }
            }, {
                key: "setValue",
                value: function(t, e) {
                    t.nodeValue = e
                }
            }, {
                key: "listen",
                value: function(t, e, n) {
                    return "string" == typeof t ? this.eventManager.addGlobalEventListener(t, e, ks(n)) : this.eventManager.addEventListener(t, e, ks(n))
                }
            }]),
            t
        }()
          , Ss = function(t) {
            d(n, t);
            var e = y(n);
            function n(t, r, i, o) {
                var a;
                m(this, n),
                (a = e.call(this, t)).component = i;
                var u = _s(o + "-" + i.id, i.styles, []);
                return r.addStyles(u),
                a.contentAttr = ws.replace(gs, o + "-" + i.id),
                a.hostAttr = ms.replace(gs, o + "-" + i.id),
                a
            }
            return _(n, [{
                key: "applyToHost",
                value: function(t) {
                    i(r(n.prototype), "setAttribute", this).call(this, t, this.hostAttr, "")
                }
            }, {
                key: "createElement",
                value: function(t, e) {
                    var o = i(r(n.prototype), "createElement", this).call(this, t, e);
                    return i(r(n.prototype), "setAttribute", this).call(this, o, this.contentAttr, ""),
                    o
                }
            }]),
            n
        }(Cs)
          , xs = function(t) {
            d(n, t);
            var e = y(n);
            function n(t, r, i, o) {
                var a;
                m(this, n),
                (a = e.call(this, t)).sharedStylesHost = r,
                a.hostEl = i,
                a.component = o,
                a.shadowRoot = o.encapsulation === be.ShadowDom ? i.attachShadow({
                    mode: "open"
                }) : i.createShadowRoot(),
                a.sharedStylesHost.addHost(a.shadowRoot);
                for (var u = _s(o.id, o.styles, []), s = 0; s < u.length; s++) {
                    var l = document.createElement("style");
                    l.textContent = u[s],
                    a.shadowRoot.appendChild(l)
                }
                return a
            }
            return _(n, [{
                key: "nodeOrShadowRoot",
                value: function(t) {
                    return t === this.hostEl ? this.shadowRoot : t
                }
            }, {
                key: "destroy",
                value: function() {
                    this.sharedStylesHost.removeHost(this.shadowRoot)
                }
            }, {
                key: "appendChild",
                value: function(t, e) {
                    return i(r(n.prototype), "appendChild", this).call(this, this.nodeOrShadowRoot(t), e)
                }
            }, {
                key: "insertBefore",
                value: function(t, e, o) {
                    return i(r(n.prototype), "insertBefore", this).call(this, this.nodeOrShadowRoot(t), e, o)
                }
            }, {
                key: "removeChild",
                value: function(t, e) {
                    return i(r(n.prototype), "removeChild", this).call(this, this.nodeOrShadowRoot(t), e)
                }
            }, {
                key: "parentNode",
                value: function(t) {
                    return this.nodeOrShadowRoot(i(r(n.prototype), "parentNode", this).call(this, this.nodeOrShadowRoot(t)))
                }
            }]),
            n
        }(Cs)
          , Es = function() {
            var t = function(t) {
                d(n, t);
                var e = y(n);
                function n(t) {
                    return m(this, n),
                    e.call(this, t)
                }
                return _(n, [{
                    key: "supports",
                    value: function(t) {
                        return !0
                    }
                }, {
                    key: "addEventListener",
                    value: function(t, e, n) {
                        var r = this;
                        return t.addEventListener(e, n, !1),
                        function() {
                            return r.removeEventListener(t, e, n)
                        }
                    }
                }, {
                    key: "removeEventListener",
                    value: function(t, e, n) {
                        return t.removeEventListener(e, n)
                    }
                }]),
                n
            }(fs);
            return t.\u0275fac = function(e) {
                return new (e || t)(he(Au))
            }
            ,
            t.\u0275prov = Tt({
                token: t,
                factory: t.\u0275fac
            }),
            t
        }()
          , Ts = ["alt", "control", "meta", "shift"]
          , Os = {
            "\b": "Backspace",
            "\t": "Tab",
            "\x7f": "Delete",
            "\x1b": "Escape",
            Del: "Delete",
            Esc: "Escape",
            Left: "ArrowLeft",
            Right: "ArrowRight",
            Up: "ArrowUp",
            Down: "ArrowDown",
            Menu: "ContextMenu",
            Scroll: "ScrollLock",
            Win: "OS"
        }
          , As = {
            A: "1",
            B: "2",
            C: "3",
            D: "4",
            E: "5",
            F: "6",
            G: "7",
            H: "8",
            I: "9",
            J: "*",
            K: "+",
            M: "-",
            N: ".",
            O: "/",
            "`": "0",
            "\x90": "NumLock"
        }
          , Rs = {
            alt: function(t) {
                return t.altKey
            },
            control: function(t) {
                return t.ctrlKey
            },
            meta: function(t) {
                return t.metaKey
            },
            shift: function(t) {
                return t.shiftKey
            }
        }
          , Ps = function() {
            var t = function(t) {
                d(n, t);
                var e = y(n);
                function n(t) {
                    return m(this, n),
                    e.call(this, t)
                }
                return _(n, [{
                    key: "supports",
                    value: function(t) {
                        return null != n.parseEventName(t)
                    }
                }, {
                    key: "addEventListener",
                    value: function(t, e, r) {
                        var i = n.parseEventName(e)
                          , o = n.eventCallback(i.fullKey, r, this.manager.getZone());
                        return this.manager.getZone().runOutsideAngular((function() {
                            return Tu().onAndCancel(t, i.domEventName, o)
                        }
                        ))
                    }
                }], [{
                    key: "parseEventName",
                    value: function(t) {
                        var e = t.toLowerCase().split(".")
                          , r = e.shift();
                        if (0 === e.length || "keydown" !== r && "keyup" !== r)
                            return null;
                        var i = n._normalizeKey(e.pop())
                          , o = "";
                        if (Ts.forEach((function(t) {
                            var n = e.indexOf(t);
                            n > -1 && (e.splice(n, 1),
                            o += t + ".")
                        }
                        )),
                        o += i,
                        0 != e.length || 0 === i.length)
                            return null;
                        var a = {};
                        return a.domEventName = r,
                        a.fullKey = o,
                        a
                    }
                }, {
                    key: "getEventFullKey",
                    value: function(t) {
                        var e = ""
                          , n = function(t) {
                            var e = t.key;
                            if (null == e) {
                                if (null == (e = t.keyIdentifier))
                                    return "Unidentified";
                                e.startsWith("U+") && (e = String.fromCharCode(parseInt(e.substring(2), 16)),
                                3 === t.location && As.hasOwnProperty(e) && (e = As[e]))
                            }
                            return Os[e] || e
                        }(t);
                        return " " === (n = n.toLowerCase()) ? n = "space" : "." === n && (n = "dot"),
                        Ts.forEach((function(r) {
                            r != n && (0,
                            Rs[r])(t) && (e += r + ".")
                        }
                        )),
                        e += n
                    }
                }, {
                    key: "eventCallback",
                    value: function(t, e, r) {
                        return function(i) {
                            n.getEventFullKey(i) === t && r.runGuarded((function() {
                                return e(i)
                            }
                            ))
                        }
                    }
                }, {
                    key: "_normalizeKey",
                    value: function(t) {
                        switch (t) {
                        case "esc":
                            return "escape";
                        default:
                            return t
                        }
                    }
                }]),
                n
            }(fs);
            return t.\u0275fac = function(e) {
                return new (e || t)(he(Au))
            }
            ,
            t.\u0275prov = Tt({
                token: t,
                factory: t.\u0275fac
            }),
            t
        }()
          , Is = cu(bu, "browser", [{
            provide: ja,
            useValue: "browser"
        }, {
            provide: Ia,
            useValue: function() {
                os.makeCurrent(),
                ls.init()
            },
            multi: !0
        }, {
            provide: Au,
            useFactory: function() {
                return function(t) {
                    Ke = t
                }(document),
                document
            },
            deps: []
        }])
          , js = [[], {
            provide: Yi,
            useValue: "root"
        }, {
            provide: wr,
            useFactory: function() {
                return new wr
            },
            deps: []
        }, {
            provide: cs,
            useClass: Es,
            multi: !0,
            deps: [Au, Qa, ja]
        }, {
            provide: cs,
            useClass: Ps,
            multi: !0,
            deps: [Au]
        }, [], {
            provide: bs,
            useClass: bs,
            deps: [hs, vs, Aa]
        }, {
            provide: Vo,
            useExisting: bs
        }, {
            provide: ds,
            useExisting: vs
        }, {
            provide: vs,
            useClass: vs,
            deps: [Au]
        }, {
            provide: ru,
            useClass: ru,
            deps: [Qa]
        }, {
            provide: hs,
            useClass: hs,
            deps: [cs, Qa]
        }, []]
          , Ns = function() {
            var t = function() {
                function t(e) {
                    if (m(this, t),
                    e)
                        throw new Error("BrowserModule has already been loaded. If you need access to common directives such as NgIf and NgFor from a lazy loaded module, import CommonModule instead.")
                }
                return _(t, null, [{
                    key: "withServerTransition",
                    value: function(e) {
                        return {
                            ngModule: t,
                            providers: [{
                                provide: Aa,
                                useValue: e.appId
                            }, {
                                provide: us,
                                useExisting: Aa
                            }, ss]
                        }
                    }
                }]),
                t
            }();
            return t.\u0275mod = Re({
                type: t
            }),
            t.\u0275inj = Ot({
                factory: function(e) {
                    return new (e || t)(he(t, 12))
                },
                providers: js,
                imports: [ts, Su]
            }),
            t
        }();
        function Ms() {
            for (var t = arguments.length, e = new Array(t), n = 0; n < t; n++)
                e[n] = arguments[n];
            var r = e[e.length - 1];
            return Z(r) ? (e.pop(),
            et(e, r)) : ct(e)
        }
        "undefined" != typeof window && window;
        var Us = function(t) {
            d(n, t);
            var e = y(n);
            function n(t) {
                var r;
                return m(this, n),
                (r = e.call(this))._value = t,
                r
            }
            return _(n, [{
                key: "_subscribe",
                value: function(t) {
                    var e = i(r(n.prototype), "_subscribe", this).call(this, t);
                    return e && !e.closed && t.next(this._value),
                    e
                }
            }, {
                key: "getValue",
                value: function() {
                    if (this.hasError)
                        throw this.thrownError;
                    if (this.closed)
                        throw new F;
                    return this._value
                }
            }, {
                key: "next",
                value: function(t) {
                    i(r(n.prototype), "next", this).call(this, this._value = t)
                }
            }, {
                key: "value",
                get: function() {
                    return this.getValue()
                }
            }]),
            n
        }(q)
          , Ds = function(t) {
            d(n, t);
            var e = y(n);
            function n() {
                return m(this, n),
                e.apply(this, arguments)
            }
            return _(n, [{
                key: "notifyNext",
                value: function(t, e, n, r, i) {
                    this.destination.next(e)
                }
            }, {
                key: "notifyError",
                value: function(t, e) {
                    this.destination.error(t)
                }
            }, {
                key: "notifyComplete",
                value: function(t) {
                    this.destination.complete()
                }
            }]),
            n
        }(j)
          , Hs = function(t) {
            d(n, t);
            var e = y(n);
            function n(t, r, i) {
                var o;
                return m(this, n),
                (o = e.call(this)).parent = t,
                o.outerValue = r,
                o.outerIndex = i,
                o.index = 0,
                o
            }
            return _(n, [{
                key: "_next",
                value: function(t) {
                    this.parent.notifyNext(this.outerValue, t, this.outerIndex, this.index++, this)
                }
            }, {
                key: "_error",
                value: function(t) {
                    this.parent.notifyError(t, this),
                    this.unsubscribe()
                }
            }, {
                key: "_complete",
                value: function() {
                    this.parent.notifyComplete(this),
                    this.unsubscribe()
                }
            }]),
            n
        }(j);
        function Ls(t, e, n, r) {
            var i = arguments.length > 4 && void 0 !== arguments[4] ? arguments[4] : new Hs(t,n,r);
            if (!i.closed)
                return e instanceof H ? e.subscribe(i) : tt(e)(i)
        }
        var Fs = {};
        function Vs() {
            for (var t = arguments.length, e = new Array(t), n = 0; n < t; n++)
                e[n] = arguments[n];
            var r = void 0
              , i = void 0;
            return Z(e[e.length - 1]) && (i = e.pop()),
            "function" == typeof e[e.length - 1] && (r = e.pop()),
            1 === e.length && b(e[0]) && (e = e[0]),
            ct(e, i).lift(new zs(r))
        }
        var zs = function() {
            function t(e) {
                m(this, t),
                this.resultSelector = e
            }
            return _(t, [{
                key: "call",
                value: function(t, e) {
                    return e.subscribe(new qs(t,this.resultSelector))
                }
            }]),
            t
        }()
          , qs = function(t) {
            d(n, t);
            var e = y(n);
            function n(t, r) {
                var i;
                return m(this, n),
                (i = e.call(this, t)).resultSelector = r,
                i.active = 0,
                i.values = [],
                i.observables = [],
                i
            }
            return _(n, [{
                key: "_next",
                value: function(t) {
                    this.values.push(Fs),
                    this.observables.push(t)
                }
            }, {
                key: "_complete",
                value: function() {
                    var t = this.observables
                      , e = t.length;
                    if (0 === e)
                        this.destination.complete();
                    else {
                        this.active = e,
                        this.toRespond = e;
                        for (var n = 0; n < e; n++)
                            this.add(Ls(this, t[n], void 0, n))
                    }
                }
            }, {
                key: "notifyComplete",
                value: function(t) {
                    0 == (this.active -= 1) && this.destination.complete()
                }
            }, {
                key: "notifyNext",
                value: function(t, e, n) {
                    var r = this.values
                      , i = this.toRespond ? r[n] === Fs ? --this.toRespond : this.toRespond : 0;
                    r[n] = e,
                    0 === i && (this.resultSelector ? this._tryResultSelector(r) : this.destination.next(r.slice()))
                }
            }, {
                key: "_tryResultSelector",
                value: function(t) {
                    var e;
                    try {
                        e = this.resultSelector.apply(this, t)
                    } catch (n) {
                        return void this.destination.error(n)
                    }
                    this.destination.next(e)
                }
            }]),
            n
        }(Ds)
          , Bs = function() {
            function t() {
                return Error.call(this),
                this.message = "no elements in sequence",
                this.name = "EmptyError",
                this
            }
            return t.prototype = Object.create(Error.prototype),
            t
        }()
          , Zs = new H((function(t) {
            return t.complete()
        }
        ));
        function Ws(t) {
            return t ? function(t) {
                return new H((function(e) {
                    return t.schedule((function() {
                        return e.complete()
                    }
                    ))
                }
                ))
            }(t) : Zs
        }
        function Gs(t) {
            return new H((function(e) {
                var n;
                try {
                    n = t()
                } catch (r) {
                    return void e.error(r)
                }
                return (n ? nt(n) : Ws()).subscribe(e)
            }
            ))
        }
        function Qs() {
            return lt(1)
        }
        function Js(t, e) {
            return function(n) {
                return n.lift(new Ks(t,e))
            }
        }
        var Ks = function() {
            function t(e, n) {
                m(this, t),
                this.predicate = e,
                this.thisArg = n
            }
            return _(t, [{
                key: "call",
                value: function(t, e) {
                    return e.subscribe(new Ys(t,this.predicate,this.thisArg))
                }
            }]),
            t
        }()
          , Ys = function(t) {
            d(n, t);
            var e = y(n);
            function n(t, r, i) {
                var o;
                return m(this, n),
                (o = e.call(this, t)).predicate = r,
                o.thisArg = i,
                o.count = 0,
                o
            }
            return _(n, [{
                key: "_next",
                value: function(t) {
                    var e;
                    try {
                        e = this.predicate.call(this.thisArg, t, this.count++)
                    } catch (n) {
                        return void this.destination.error(n)
                    }
                    e && this.destination.next(t)
                }
            }]),
            n
        }(j)
          , Xs = function() {
            function t() {
                return Error.call(this),
                this.message = "argument out of range",
                this.name = "ArgumentOutOfRangeError",
                this
            }
            return t.prototype = Object.create(Error.prototype),
            t
        }();
        function $s(t) {
            return function(e) {
                return 0 === t ? Ws() : e.lift(new tl(t))
            }
        }
        var tl = function() {
            function t(e) {
                if (m(this, t),
                this.total = e,
                this.total < 0)
                    throw new Xs
            }
            return _(t, [{
                key: "call",
                value: function(t, e) {
                    return e.subscribe(new el(t,this.total))
                }
            }]),
            t
        }()
          , el = function(t) {
            d(n, t);
            var e = y(n);
            function n(t, r) {
                var i;
                return m(this, n),
                (i = e.call(this, t)).total = r,
                i.ring = new Array,
                i.count = 0,
                i
            }
            return _(n, [{
                key: "_next",
                value: function(t) {
                    var e = this.ring
                      , n = this.total
                      , r = this.count++;
                    e.length < n ? e.push(t) : e[r % n] = t
                }
            }, {
                key: "_complete",
                value: function() {
                    var t = this.destination
                      , e = this.count;
                    if (e > 0)
                        for (var n = this.count >= this.total ? this.total : this.count, r = this.ring, i = 0; i < n; i++) {
                            var o = e++ % n;
                            t.next(r[o])
                        }
                    t.complete()
                }
            }]),
            n
        }(j);
        function nl() {
            var t = arguments.length > 0 && void 0 !== arguments[0] ? arguments[0] : ol;
            return function(e) {
                return e.lift(new rl(t))
            }
        }
        var rl = function() {
            function t(e) {
                m(this, t),
                this.errorFactory = e
            }
            return _(t, [{
                key: "call",
                value: function(t, e) {
                    return e.subscribe(new il(t,this.errorFactory))
                }
            }]),
            t
        }()
          , il = function(t) {
            d(n, t);
            var e = y(n);
            function n(t, r) {
                var i;
                return m(this, n),
                (i = e.call(this, t)).errorFactory = r,
                i.hasValue = !1,
                i
            }
            return _(n, [{
                key: "_next",
                value: function(t) {
                    this.hasValue = !0,
                    this.destination.next(t)
                }
            }, {
                key: "_complete",
                value: function() {
                    if (this.hasValue)
                        return this.destination.complete();
                    var t;
                    try {
                        t = this.errorFactory()
                    } catch (e) {
                        t = e
                    }
                    this.destination.error(t)
                }
            }]),
            n
        }(j);
        function ol() {
            return new Bs
        }
        function al() {
            var t = arguments.length > 0 && void 0 !== arguments[0] ? arguments[0] : null;
            return function(e) {
                return e.lift(new ul(t))
            }
        }
        var ul = function() {
            function t(e) {
                m(this, t),
                this.defaultValue = e
            }
            return _(t, [{
                key: "call",
                value: function(t, e) {
                    return e.subscribe(new sl(t,this.defaultValue))
                }
            }]),
            t
        }()
          , sl = function(t) {
            d(n, t);
            var e = y(n);
            function n(t, r) {
                var i;
                return m(this, n),
                (i = e.call(this, t)).defaultValue = r,
                i.isEmpty = !0,
                i
            }
            return _(n, [{
                key: "_next",
                value: function(t) {
                    this.isEmpty = !1,
                    this.destination.next(t)
                }
            }, {
                key: "_complete",
                value: function() {
                    this.isEmpty && this.destination.next(this.defaultValue),
                    this.destination.complete()
                }
            }]),
            n
        }(j);
        function ll(t, e) {
            return "function" == typeof e ? function(n) {
                return n.pipe(ll((function(n, r) {
                    return nt(t(n, r)).pipe(W((function(t, i) {
                        return e(n, t, r, i)
                    }
                    )))
                }
                )))
            }
            : function(e) {
                return e.lift(new cl(t))
            }
        }
        var cl = function() {
            function t(e) {
                m(this, t),
                this.project = e
            }
            return _(t, [{
                key: "call",
                value: function(t, e) {
                    return e.subscribe(new hl(t,this.project))
                }
            }]),
            t
        }()
          , hl = function(t) {
            d(n, t);
            var e = y(n);
            function n(t, r) {
                var i;
                return m(this, n),
                (i = e.call(this, t)).project = r,
                i.index = 0,
                i
            }
            return _(n, [{
                key: "_next",
                value: function(t) {
                    var e, n = this.index++;
                    try {
                        e = this.project(t, n)
                    } catch (r) {
                        return void this.destination.error(r)
                    }
                    this._innerSub(e)
                }
            }, {
                key: "_innerSub",
                value: function(t) {
                    var e = this.innerSubscription;
                    e && e.unsubscribe();
                    var n = new rt(this)
                      , r = this.destination;
                    r.add(n),
                    this.innerSubscription = ot(t, n),
                    this.innerSubscription !== n && r.add(this.innerSubscription)
                }
            }, {
                key: "_complete",
                value: function() {
                    var t = this.innerSubscription;
                    t && !t.closed || i(r(n.prototype), "_complete", this).call(this),
                    this.unsubscribe()
                }
            }, {
                key: "_unsubscribe",
                value: function() {
                    this.innerSubscription = void 0
                }
            }, {
                key: "notifyComplete",
                value: function() {
                    this.innerSubscription = void 0,
                    this.isStopped && i(r(n.prototype), "_complete", this).call(this)
                }
            }, {
                key: "notifyNext",
                value: function(t) {
                    this.destination.next(t)
                }
            }]),
            n
        }(it);
        function fl(t) {
            return function(e) {
                return 0 === t ? Ws() : e.lift(new dl(t))
            }
        }
        var dl = function() {
            function t(e) {
                if (m(this, t),
                this.total = e,
                this.total < 0)
                    throw new Xs
            }
            return _(t, [{
                key: "call",
                value: function(t, e) {
                    return e.subscribe(new vl(t,this.total))
                }
            }]),
            t
        }()
          , vl = function(t) {
            d(n, t);
            var e = y(n);
            function n(t, r) {
                var i;
                return m(this, n),
                (i = e.call(this, t)).total = r,
                i.count = 0,
                i
            }
            return _(n, [{
                key: "_next",
                value: function(t) {
                    var e = this.total
                      , n = ++this.count;
                    n <= e && (this.destination.next(t),
                    n === e && (this.destination.complete(),
                    this.unsubscribe()))
                }
            }]),
            n
        }(j);
        function pl() {
            return Qs()(Ms.apply(void 0, arguments))
        }
        var gl = function() {
            function t(e, n) {
                var r = arguments.length > 2 && void 0 !== arguments[2] && arguments[2];
                m(this, t),
                this.accumulator = e,
                this.seed = n,
                this.hasSeed = r
            }
            return _(t, [{
                key: "call",
                value: function(t, e) {
                    return e.subscribe(new yl(t,this.accumulator,this.seed,this.hasSeed))
                }
            }]),
            t
        }()
          , yl = function(t) {
            d(n, t);
            var e = y(n);
            function n(t, r, i, o) {
                var a;
                return m(this, n),
                (a = e.call(this, t)).accumulator = r,
                a._seed = i,
                a.hasSeed = o,
                a.index = 0,
                a
            }
            return _(n, [{
                key: "_next",
                value: function(t) {
                    if (this.hasSeed)
                        return this._tryNext(t);
                    this.seed = t,
                    this.destination.next(t)
                }
            }, {
                key: "_tryNext",
                value: function(t) {
                    var e, n = this.index++;
                    try {
                        e = this.accumulator(this.seed, t, n)
                    } catch (r) {
                        this.destination.error(r)
                    }
                    this.seed = e,
                    this.destination.next(e)
                }
            }, {
                key: "seed",
                get: function() {
                    return this._seed
                },
                set: function(t) {
                    this.hasSeed = !0,
                    this._seed = t
                }
            }]),
            n
        }(j);
        function ml(t) {
            return function(e) {
                var n = new wl(t)
                  , r = e.lift(n);
                return n.caught = r
            }
        }
        var wl = function() {
            function t(e) {
                m(this, t),
                this.selector = e
            }
            return _(t, [{
                key: "call",
                value: function(t, e) {
                    return e.subscribe(new _l(t,this.selector,this.caught))
                }
            }]),
            t
        }()
          , _l = function(t) {
            d(n, t);
            var e = y(n);
            function n(t, r, i) {
                var o;
                return m(this, n),
                (o = e.call(this, t)).selector = r,
                o.caught = i,
                o
            }
            return _(n, [{
                key: "error",
                value: function(t) {
                    if (!this.isStopped) {
                        var e;
                        try {
                            e = this.selector(t, this.caught)
                        } catch (u) {
                            return void i(r(n.prototype), "error", this).call(this, u)
                        }
                        this._unsubscribeAndRecycle();
                        var o = new rt(this);
                        this.add(o);
                        var a = ot(e, o);
                        a !== o && this.add(a)
                    }
                }
            }]),
            n
        }(it);
        function kl(t, e) {
            return at(t, e, 1)
        }
        function bl(t, e) {
            var n = arguments.length >= 2;
            return function(r) {
                return r.pipe(t ? Js((function(e, n) {
                    return t(e, n, r)
                }
                )) : U, fl(1), n ? al(e) : nl((function() {
                    return new Bs
                }
                )))
            }
        }
        function Cl() {}
        function Sl(t, e, n) {
            return function(r) {
                return r.lift(new xl(t,e,n))
            }
        }
        var xl = function() {
            function t(e, n, r) {
                m(this, t),
                this.nextOrObserver = e,
                this.error = n,
                this.complete = r
            }
            return _(t, [{
                key: "call",
                value: function(t, e) {
                    return e.subscribe(new El(t,this.nextOrObserver,this.error,this.complete))
                }
            }]),
            t
        }()
          , El = function(t) {
            d(n, t);
            var e = y(n);
            function n(t, r, i, a) {
                var u;
                return m(this, n),
                (u = e.call(this, t))._tapNext = Cl,
                u._tapError = Cl,
                u._tapComplete = Cl,
                u._tapError = i || Cl,
                u._tapComplete = a || Cl,
                S(r) ? (u._context = o(u),
                u._tapNext = r) : r && (u._context = r,
                u._tapNext = r.next || Cl,
                u._tapError = r.error || Cl,
                u._tapComplete = r.complete || Cl),
                u
            }
            return _(n, [{
                key: "_next",
                value: function(t) {
                    try {
                        this._tapNext.call(this._context, t)
                    } catch (e) {
                        return void this.destination.error(e)
                    }
                    this.destination.next(t)
                }
            }, {
                key: "_error",
                value: function(t) {
                    try {
                        this._tapError.call(this._context, t)
                    } catch (t) {
                        return void this.destination.error(t)
                    }
                    this.destination.error(t)
                }
            }, {
                key: "_complete",
                value: function() {
                    try {
                        this._tapComplete.call(this._context)
                    } catch (t) {
                        return void this.destination.error(t)
                    }
                    return this.destination.complete()
                }
            }]),
            n
        }(j)
          , Tl = function() {
            function t(e) {
                m(this, t),
                this.callback = e
            }
            return _(t, [{
                key: "call",
                value: function(t, e) {
                    return e.subscribe(new Ol(t,this.callback))
                }
            }]),
            t
        }()
          , Ol = function(t) {
            d(n, t);
            var e = y(n);
            function n(t, r) {
                var i;
                return m(this, n),
                (i = e.call(this, t)).add(new E(r)),
                i
            }
            return n
        }(j)
          , Al = function t(e, n) {
            m(this, t),
            this.id = e,
            this.url = n
        }
          , Rl = function(t) {
            d(n, t);
            var e = y(n);
            function n(t, r) {
                var i, o = arguments.length > 2 && void 0 !== arguments[2] ? arguments[2] : "imperative", a = arguments.length > 3 && void 0 !== arguments[3] ? arguments[3] : null;
                return m(this, n),
                (i = e.call(this, t, r)).navigationTrigger = o,
                i.restoredState = a,
                i
            }
            return _(n, [{
                key: "toString",
                value: function() {
                    return "NavigationStart(id: ".concat(this.id, ", url: '").concat(this.url, "')")
                }
            }]),
            n
        }(Al)
          , Pl = function(t) {
            d(n, t);
            var e = y(n);
            function n(t, r, i) {
                var o;
                return m(this, n),
                (o = e.call(this, t, r)).urlAfterRedirects = i,
                o
            }
            return _(n, [{
                key: "toString",
                value: function() {
                    return "NavigationEnd(id: ".concat(this.id, ", url: '").concat(this.url, "', urlAfterRedirects: '").concat(this.urlAfterRedirects, "')")
                }
            }]),
            n
        }(Al)
          , Il = function(t) {
            d(n, t);
            var e = y(n);
            function n(t, r, i) {
                var o;
                return m(this, n),
                (o = e.call(this, t, r)).reason = i,
                o
            }
            return _(n, [{
                key: "toString",
                value: function() {
                    return "NavigationCancel(id: ".concat(this.id, ", url: '").concat(this.url, "')")
                }
            }]),
            n
        }(Al)
          , jl = function(t) {
            d(n, t);
            var e = y(n);
            function n(t, r, i) {
                var o;
                return m(this, n),
                (o = e.call(this, t, r)).error = i,
                o
            }
            return _(n, [{
                key: "toString",
                value: function() {
                    return "NavigationError(id: ".concat(this.id, ", url: '").concat(this.url, "', error: ").concat(this.error, ")")
                }
            }]),
            n
        }(Al)
          , Nl = function(t) {
            d(n, t);
            var e = y(n);
            function n(t, r, i, o) {
                var a;
                return m(this, n),
                (a = e.call(this, t, r)).urlAfterRedirects = i,
                a.state = o,
                a
            }
            return _(n, [{
                key: "toString",
                value: function() {
                    return "RoutesRecognized(id: ".concat(this.id, ", url: '").concat(this.url, "', urlAfterRedirects: '").concat(this.urlAfterRedirects, "', state: ").concat(this.state, ")")
                }
            }]),
            n
        }(Al)
          , Ml = function(t) {
            d(n, t);
            var e = y(n);
            function n(t, r, i, o) {
                var a;
                return m(this, n),
                (a = e.call(this, t, r)).urlAfterRedirects = i,
                a.state = o,
                a
            }
            return _(n, [{
                key: "toString",
                value: function() {
                    return "GuardsCheckStart(id: ".concat(this.id, ", url: '").concat(this.url, "', urlAfterRedirects: '").concat(this.urlAfterRedirects, "', state: ").concat(this.state, ")")
                }
            }]),
            n
        }(Al)
          , Ul = function(t) {
            d(n, t);
            var e = y(n);
            function n(t, r, i, o, a) {
                var u;
                return m(this, n),
                (u = e.call(this, t, r)).urlAfterRedirects = i,
                u.state = o,
                u.shouldActivate = a,
                u
            }
            return _(n, [{
                key: "toString",
                value: function() {
                    return "GuardsCheckEnd(id: ".concat(this.id, ", url: '").concat(this.url, "', urlAfterRedirects: '").concat(this.urlAfterRedirects, "', state: ").concat(this.state, ", shouldActivate: ").concat(this.shouldActivate, ")")
                }
            }]),
            n
        }(Al)
          , Dl = function(t) {
            d(n, t);
            var e = y(n);
            function n(t, r, i, o) {
                var a;
                return m(this, n),
                (a = e.call(this, t, r)).urlAfterRedirects = i,
                a.state = o,
                a
            }
            return _(n, [{
                key: "toString",
                value: function() {
                    return "ResolveStart(id: ".concat(this.id, ", url: '").concat(this.url, "', urlAfterRedirects: '").concat(this.urlAfterRedirects, "', state: ").concat(this.state, ")")
                }
            }]),
            n
        }(Al)
          , Hl = function(t) {
            d(n, t);
            var e = y(n);
            function n(t, r, i, o) {
                var a;
                return m(this, n),
                (a = e.call(this, t, r)).urlAfterRedirects = i,
                a.state = o,
                a
            }
            return _(n, [{
                key: "toString",
                value: function() {
                    return "ResolveEnd(id: ".concat(this.id, ", url: '").concat(this.url, "', urlAfterRedirects: '").concat(this.urlAfterRedirects, "', state: ").concat(this.state, ")")
                }
            }]),
            n
        }(Al)
          , Ll = function() {
            function t(e) {
                m(this, t),
                this.route = e
            }
            return _(t, [{
                key: "toString",
                value: function() {
                    return "RouteConfigLoadStart(path: ".concat(this.route.path, ")")
                }
            }]),
            t
        }()
          , Fl = function() {
            function t(e) {
                m(this, t),
                this.route = e
            }
            return _(t, [{
                key: "toString",
                value: function() {
                    return "RouteConfigLoadEnd(path: ".concat(this.route.path, ")")
                }
            }]),
            t
        }()
          , Vl = function() {
            function t(e) {
                m(this, t),
                this.snapshot = e
            }
            return _(t, [{
                key: "toString",
                value: function() {
                    return "ChildActivationStart(path: '".concat(this.snapshot.routeConfig && this.snapshot.routeConfig.path || "", "')")
                }
            }]),
            t
        }()
          , zl = function() {
            function t(e) {
                m(this, t),
                this.snapshot = e
            }
            return _(t, [{
                key: "toString",
                value: function() {
                    return "ChildActivationEnd(path: '".concat(this.snapshot.routeConfig && this.snapshot.routeConfig.path || "", "')")
                }
            }]),
            t
        }()
          , ql = function() {
            function t(e) {
                m(this, t),
                this.snapshot = e
            }
            return _(t, [{
                key: "toString",
                value: function() {
                    return "ActivationStart(path: '".concat(this.snapshot.routeConfig && this.snapshot.routeConfig.path || "", "')")
                }
            }]),
            t
        }()
          , Bl = function() {
            function t(e) {
                m(this, t),
                this.snapshot = e
            }
            return _(t, [{
                key: "toString",
                value: function() {
                    return "ActivationEnd(path: '".concat(this.snapshot.routeConfig && this.snapshot.routeConfig.path || "", "')")
                }
            }]),
            t
        }()
          , Zl = function() {
            function t(e, n, r) {
                m(this, t),
                this.routerEvent = e,
                this.position = n,
                this.anchor = r
            }
            return _(t, [{
                key: "toString",
                value: function() {
                    var t = this.position ? "".concat(this.position[0], ", ").concat(this.position[1]) : null;
                    return "Scroll(anchor: '".concat(this.anchor, "', position: '").concat(t, "')")
                }
            }]),
            t
        }()
          , Wl = "primary"
          , Gl = function() {
            function t(e) {
                m(this, t),
                this.params = e || {}
            }
            return _(t, [{
                key: "has",
                value: function(t) {
                    return Object.prototype.hasOwnProperty.call(this.params, t)
                }
            }, {
                key: "get",
                value: function(t) {
                    if (this.has(t)) {
                        var e = this.params[t];
                        return Array.isArray(e) ? e[0] : e
                    }
                    return null
                }
            }, {
                key: "getAll",
                value: function(t) {
                    if (this.has(t)) {
                        var e = this.params[t];
                        return Array.isArray(e) ? e : [e]
                    }
                    return []
                }
            }, {
                key: "keys",
                get: function() {
                    return Object.keys(this.params)
                }
            }]),
            t
        }();
        function Ql(t) {
            return new Gl(t)
        }
        function Jl(t) {
            var e = Error("NavigationCancelingError: " + t);
            return e.ngNavigationCancelingError = !0,
            e
        }
        function Kl(t, e, n) {
            var r = n.path.split("/");
            if (r.length > t.length)
                return null;
            if ("full" === n.pathMatch && (e.hasChildren() || r.length < t.length))
                return null;
            for (var i = {}, o = 0; o < r.length; o++) {
                var a = r[o]
                  , u = t[o];
                if (a.startsWith(":"))
                    i[a.substring(1)] = u;
                else if (a !== u.path)
                    return null
            }
            return {
                consumed: t.slice(0, r.length),
                posParams: i
            }
        }
        function Yl(t, e) {
            var n, r = Object.keys(t), i = Object.keys(e);
            if (!r || !i || r.length != i.length)
                return !1;
            for (var o = 0; o < r.length; o++)
                if (!Xl(t[n = r[o]], e[n]))
                    return !1;
            return !0
        }
        function Xl(t, e) {
            if (Array.isArray(t) && Array.isArray(e)) {
                if (t.length !== e.length)
                    return !1;
                var n = l(t).sort()
                  , r = l(e).sort();
                return n.every((function(t, e) {
                    return r[e] === t
                }
                ))
            }
            return t === e
        }
        function $l(t) {
            return Array.prototype.concat.apply([], t)
        }
        function tc(t) {
            return t.length > 0 ? t[t.length - 1] : null
        }
        function ec(t, e) {
            for (var n in t)
                t.hasOwnProperty(n) && e(t[n], n)
        }
        function nc(t) {
            return (e = t) && "function" == typeof e.subscribe ? t : To(t) ? nt(Promise.resolve(t)) : Ms(t);
            var e
        }
        function rc(t, e, n) {
            return n ? function(t, e) {
                return Yl(t, e)
            }(t.queryParams, e.queryParams) && function t(e, n) {
                if (!uc(e.segments, n.segments))
                    return !1;
                if (e.numberOfChildren !== n.numberOfChildren)
                    return !1;
                for (var r in n.children) {
                    if (!e.children[r])
                        return !1;
                    if (!t(e.children[r], n.children[r]))
                        return !1
                }
                return !0
            }(t.root, e.root) : function(t, e) {
                return Object.keys(e).length <= Object.keys(t).length && Object.keys(e).every((function(n) {
                    return Xl(t[n], e[n])
                }
                ))
            }(t.queryParams, e.queryParams) && function t(e, n) {
                return function e(n, r, i) {
                    if (n.segments.length > i.length)
                        return !!uc(n.segments.slice(0, i.length), i) && !r.hasChildren();
                    if (n.segments.length === i.length) {
                        if (!uc(n.segments, i))
                            return !1;
                        for (var o in r.children) {
                            if (!n.children[o])
                                return !1;
                            if (!t(n.children[o], r.children[o]))
                                return !1
                        }
                        return !0
                    }
                    var a = i.slice(0, n.segments.length)
                      , u = i.slice(n.segments.length);
                    return !!uc(n.segments, a) && !!n.children.primary && e(n.children.primary, r, u)
                }(e, n, n.segments)
            }(t.root, e.root)
        }
        var ic = function() {
            function t(e, n, r) {
                m(this, t),
                this.root = e,
                this.queryParams = n,
                this.fragment = r
            }
            return _(t, [{
                key: "toString",
                value: function() {
                    return hc.serialize(this)
                }
            }, {
                key: "queryParamMap",
                get: function() {
                    return this._queryParamMap || (this._queryParamMap = Ql(this.queryParams)),
                    this._queryParamMap
                }
            }]),
            t
        }()
          , oc = function() {
            function t(e, n) {
                var r = this;
                m(this, t),
                this.segments = e,
                this.children = n,
                this.parent = null,
                ec(n, (function(t, e) {
                    return t.parent = r
                }
                ))
            }
            return _(t, [{
                key: "hasChildren",
                value: function() {
                    return this.numberOfChildren > 0
                }
            }, {
                key: "toString",
                value: function() {
                    return fc(this)
                }
            }, {
                key: "numberOfChildren",
                get: function() {
                    return Object.keys(this.children).length
                }
            }]),
            t
        }()
          , ac = function() {
            function t(e, n) {
                m(this, t),
                this.path = e,
                this.parameters = n
            }
            return _(t, [{
                key: "toString",
                value: function() {
                    return mc(this)
                }
            }, {
                key: "parameterMap",
                get: function() {
                    return this._parameterMap || (this._parameterMap = Ql(this.parameters)),
                    this._parameterMap
                }
            }]),
            t
        }();
        function uc(t, e) {
            return t.length === e.length && t.every((function(t, n) {
                return t.path === e[n].path
            }
            ))
        }
        function sc(t, e) {
            var n = [];
            return ec(t.children, (function(t, r) {
                r === Wl && (n = n.concat(e(t, r)))
            }
            )),
            ec(t.children, (function(t, r) {
                r !== Wl && (n = n.concat(e(t, r)))
            }
            )),
            n
        }
        var lc = function t() {
            m(this, t)
        }
          , cc = function() {
            function t() {
                m(this, t)
            }
            return _(t, [{
                key: "parse",
                value: function(t) {
                    var e = new Cc(t);
                    return new ic(e.parseRootSegment(),e.parseQueryParams(),e.parseFragment())
                }
            }, {
                key: "serialize",
                value: function(t) {
                    var e, n, r = "/".concat(function t(e, n) {
                        if (!e.hasChildren())
                            return fc(e);
                        if (n) {
                            var r = e.children.primary ? t(e.children.primary, !1) : ""
                              , i = [];
                            return ec(e.children, (function(e, n) {
                                n !== Wl && i.push("".concat(n, ":").concat(t(e, !1)))
                            }
                            )),
                            i.length > 0 ? "".concat(r, "(").concat(i.join("//"), ")") : r
                        }
                        var o = sc(e, (function(n, r) {
                            return r === Wl ? [t(e.children.primary, !1)] : ["".concat(r, ":").concat(t(n, !1))]
                        }
                        ));
                        return 1 === Object.keys(e.children).length && null != e.children.primary ? "".concat(fc(e), "/").concat(o[0]) : "".concat(fc(e), "/(").concat(o.join("//"), ")")
                    }(t.root, !0)), i = (e = t.queryParams,
                    (n = Object.keys(e).map((function(t) {
                        var n = e[t];
                        return Array.isArray(n) ? n.map((function(e) {
                            return "".concat(vc(t), "=").concat(vc(e))
                        }
                        )).join("&") : "".concat(vc(t), "=").concat(vc(n))
                    }
                    ))).length ? "?".concat(n.join("&")) : ""), o = "string" == typeof t.fragment ? "#".concat(encodeURI(t.fragment)) : "";
                    return "".concat(r).concat(i).concat(o)
                }
            }]),
            t
        }()
          , hc = new cc;
        function fc(t) {
            return t.segments.map((function(t) {
                return mc(t)
            }
            )).join("/")
        }
        function dc(t) {
            return encodeURIComponent(t).replace(/%40/g, "@").replace(/%3A/gi, ":").replace(/%24/g, "$").replace(/%2C/gi, ",")
        }
        function vc(t) {
            return dc(t).replace(/%3B/gi, ";")
        }
        function pc(t) {
            return dc(t).replace(/\(/g, "%28").replace(/\)/g, "%29").replace(/%26/gi, "&")
        }
        function gc(t) {
            return decodeURIComponent(t)
        }
        function yc(t) {
            return gc(t.replace(/\+/g, "%20"))
        }
        function mc(t) {
            return "".concat(pc(t.path)).concat((e = t.parameters,
            Object.keys(e).map((function(t) {
                return ";".concat(pc(t), "=").concat(pc(e[t]))
            }
            )).join("")));
            var e
        }
        var wc = /^[^\/()?;=#]+/;
        function _c(t) {
            var e = t.match(wc);
            return e ? e[0] : ""
        }
        var kc = /^[^=?&#]+/
          , bc = /^[^?&#]+/
          , Cc = function() {
            function t(e) {
                m(this, t),
                this.url = e,
                this.remaining = e
            }
            return _(t, [{
                key: "parseRootSegment",
                value: function() {
                    return this.consumeOptional("/"),
                    "" === this.remaining || this.peekStartsWith("?") || this.peekStartsWith("#") ? new oc([],{}) : new oc([],this.parseChildren())
                }
            }, {
                key: "parseQueryParams",
                value: function() {
                    var t = {};
                    if (this.consumeOptional("?"))
                        do {
                            this.parseQueryParam(t)
                        } while (this.consumeOptional("&"));
                    return t
                }
            }, {
                key: "parseFragment",
                value: function() {
                    return this.consumeOptional("#") ? decodeURIComponent(this.remaining) : null
                }
            }, {
                key: "parseChildren",
                value: function() {
                    if ("" === this.remaining)
                        return {};
                    this.consumeOptional("/");
                    var t = [];
                    for (this.peekStartsWith("(") || t.push(this.parseSegment()); this.peekStartsWith("/") && !this.peekStartsWith("//") && !this.peekStartsWith("/("); )
                        this.capture("/"),
                        t.push(this.parseSegment());
                    var e = {};
                    this.peekStartsWith("/(") && (this.capture("/"),
                    e = this.parseParens(!0));
                    var n = {};
                    return this.peekStartsWith("(") && (n = this.parseParens(!1)),
                    (t.length > 0 || Object.keys(e).length > 0) && (n.primary = new oc(t,e)),
                    n
                }
            }, {
                key: "parseSegment",
                value: function() {
                    var t = _c(this.remaining);
                    if ("" === t && this.peekStartsWith(";"))
                        throw new Error("Empty path url segment cannot have parameters: '".concat(this.remaining, "'."));
                    return this.capture(t),
                    new ac(gc(t),this.parseMatrixParams())
                }
            }, {
                key: "parseMatrixParams",
                value: function() {
                    for (var t = {}; this.consumeOptional(";"); )
                        this.parseParam(t);
                    return t
                }
            }, {
                key: "parseParam",
                value: function(t) {
                    var e = _c(this.remaining);
                    if (e) {
                        this.capture(e);
                        var n = "";
                        if (this.consumeOptional("=")) {
                            var r = _c(this.remaining);
                            r && this.capture(n = r)
                        }
                        t[gc(e)] = gc(n)
                    }
                }
            }, {
                key: "parseQueryParam",
                value: function(t) {
                    var e, n = (e = this.remaining.match(kc)) ? e[0] : "";
                    if (n) {
                        this.capture(n);
                        var r = "";
                        if (this.consumeOptional("=")) {
                            var i = function(t) {
                                var e = t.match(bc);
                                return e ? e[0] : ""
                            }(this.remaining);
                            i && this.capture(r = i)
                        }
                        var o = yc(n)
                          , a = yc(r);
                        if (t.hasOwnProperty(o)) {
                            var u = t[o];
                            Array.isArray(u) || (t[o] = u = [u]),
                            u.push(a)
                        } else
                            t[o] = a
                    }
                }
            }, {
                key: "parseParens",
                value: function(t) {
                    var e = {};
                    for (this.capture("("); !this.consumeOptional(")") && this.remaining.length > 0; ) {
                        var n = _c(this.remaining)
                          , r = this.remaining[n.length];
                        if ("/" !== r && ")" !== r && ";" !== r)
                            throw new Error("Cannot parse url '".concat(this.url, "'"));
                        var i = void 0;
                        n.indexOf(":") > -1 ? (i = n.substr(0, n.indexOf(":")),
                        this.capture(i),
                        this.capture(":")) : t && (i = Wl);
                        var o = this.parseChildren();
                        e[i] = 1 === Object.keys(o).length ? o.primary : new oc([],o),
                        this.consumeOptional("//")
                    }
                    return e
                }
            }, {
                key: "peekStartsWith",
                value: function(t) {
                    return this.remaining.startsWith(t)
                }
            }, {
                key: "consumeOptional",
                value: function(t) {
                    return !!this.peekStartsWith(t) && (this.remaining = this.remaining.substring(t.length),
                    !0)
                }
            }, {
                key: "capture",
                value: function(t) {
                    if (!this.consumeOptional(t))
                        throw new Error('Expected "'.concat(t, '".'))
                }
            }]),
            t
        }()
          , Sc = function() {
            function t(e) {
                m(this, t),
                this._root = e
            }
            return _(t, [{
                key: "parent",
                value: function(t) {
                    var e = this.pathFromRoot(t);
                    return e.length > 1 ? e[e.length - 2] : null
                }
            }, {
                key: "children",
                value: function(t) {
                    var e = xc(t, this._root);
                    return e ? e.children.map((function(t) {
                        return t.value
                    }
                    )) : []
                }
            }, {
                key: "firstChild",
                value: function(t) {
                    var e = xc(t, this._root);
                    return e && e.children.length > 0 ? e.children[0].value : null
                }
            }, {
                key: "siblings",
                value: function(t) {
                    var e = Ec(t, this._root);
                    return e.length < 2 ? [] : e[e.length - 2].children.map((function(t) {
                        return t.value
                    }
                    )).filter((function(e) {
                        return e !== t
                    }
                    ))
                }
            }, {
                key: "pathFromRoot",
                value: function(t) {
                    return Ec(t, this._root).map((function(t) {
                        return t.value
                    }
                    ))
                }
            }, {
                key: "root",
                get: function() {
                    return this._root.value
                }
            }]),
            t
        }();
        function xc(t, e) {
            if (t === e.value)
                return e;
            var n, r = h(e.children);
            try {
                for (r.s(); !(n = r.n()).done; ) {
                    var i = xc(t, n.value);
                    if (i)
                        return i
                }
            } catch (o) {
                r.e(o)
            } finally {
                r.f()
            }
            return null
        }
        function Ec(t, e) {
            if (t === e.value)
                return [e];
            var n, r = h(e.children);
            try {
                for (r.s(); !(n = r.n()).done; ) {
                    var i = Ec(t, n.value);
                    if (i.length)
                        return i.unshift(e),
                        i
                }
            } catch (o) {
                r.e(o)
            } finally {
                r.f()
            }
            return []
        }
        var Tc = function() {
            function t(e, n) {
                m(this, t),
                this.value = e,
                this.children = n
            }
            return _(t, [{
                key: "toString",
                value: function() {
                    return "TreeNode(".concat(this.value, ")")
                }
            }]),
            t
        }();
        function Oc(t) {
            var e = {};
            return t && t.children.forEach((function(t) {
                return e[t.value.outlet] = t
            }
            )),
            e
        }
        var Ac = function(t) {
            d(n, t);
            var e = y(n);
            function n(t, r) {
                var i;
                return m(this, n),
                (i = e.call(this, t)).snapshot = r,
                Uc(o(i), t),
                i
            }
            return _(n, [{
                key: "toString",
                value: function() {
                    return this.snapshot.toString()
                }
            }]),
            n
        }(Sc);
        function Rc(t, e) {
            var n = function(t, e) {
                var n = new Nc([],{},{},"",{},Wl,e,null,t.root,-1,{});
                return new Mc("",new Tc(n,[]))
            }(t, e)
              , r = new Us([new ac("",{})])
              , i = new Us({})
              , o = new Us({})
              , a = new Us({})
              , u = new Us("")
              , s = new Pc(r,i,a,u,o,Wl,e,n.root);
            return s.snapshot = n.root,
            new Ac(new Tc(s,[]),n)
        }
        var Pc = function() {
            function t(e, n, r, i, o, a, u, s) {
                m(this, t),
                this.url = e,
                this.params = n,
                this.queryParams = r,
                this.fragment = i,
                this.data = o,
                this.outlet = a,
                this.component = u,
                this._futureSnapshot = s
            }
            return _(t, [{
                key: "toString",
                value: function() {
                    return this.snapshot ? this.snapshot.toString() : "Future(".concat(this._futureSnapshot, ")")
                }
            }, {
                key: "routeConfig",
                get: function() {
                    return this._futureSnapshot.routeConfig
                }
            }, {
                key: "root",
                get: function() {
                    return this._routerState.root
                }
            }, {
                key: "parent",
                get: function() {
                    return this._routerState.parent(this)
                }
            }, {
                key: "firstChild",
                get: function() {
                    return this._routerState.firstChild(this)
                }
            }, {
                key: "children",
                get: function() {
                    return this._routerState.children(this)
                }
            }, {
                key: "pathFromRoot",
                get: function() {
                    return this._routerState.pathFromRoot(this)
                }
            }, {
                key: "paramMap",
                get: function() {
                    return this._paramMap || (this._paramMap = this.params.pipe(W((function(t) {
                        return Ql(t)
                    }
                    )))),
                    this._paramMap
                }
            }, {
                key: "queryParamMap",
                get: function() {
                    return this._queryParamMap || (this._queryParamMap = this.queryParams.pipe(W((function(t) {
                        return Ql(t)
                    }
                    )))),
                    this._queryParamMap
                }
            }]),
            t
        }();
        function Ic(t) {
            var e = arguments.length > 1 && void 0 !== arguments[1] ? arguments[1] : "emptyOnly"
              , n = t.pathFromRoot
              , r = 0;
            if ("always" !== e)
                for (r = n.length - 1; r >= 1; ) {
                    var i = n[r]
                      , o = n[r - 1];
                    if (i.routeConfig && "" === i.routeConfig.path)
                        r--;
                    else {
                        if (o.component)
                            break;
                        r--
                    }
                }
            return jc(n.slice(r))
        }
        function jc(t) {
            return t.reduce((function(t, e) {
                return {
                    params: Object.assign(Object.assign({}, t.params), e.params),
                    data: Object.assign(Object.assign({}, t.data), e.data),
                    resolve: Object.assign(Object.assign({}, t.resolve), e._resolvedData)
                }
            }
            ), {
                params: {},
                data: {},
                resolve: {}
            })
        }
        var Nc = function() {
            function t(e, n, r, i, o, a, u, s, l, c, h) {
                m(this, t),
                this.url = e,
                this.params = n,
                this.queryParams = r,
                this.fragment = i,
                this.data = o,
                this.outlet = a,
                this.component = u,
                this.routeConfig = s,
                this._urlSegment = l,
                this._lastPathIndex = c,
                this._resolve = h
            }
            return _(t, [{
                key: "toString",
                value: function() {
                    var t = this.url.map((function(t) {
                        return t.toString()
                    }
                    )).join("/")
                      , e = this.routeConfig ? this.routeConfig.path : "";
                    return "Route(url:'".concat(t, "', path:'").concat(e, "')")
                }
            }, {
                key: "root",
                get: function() {
                    return this._routerState.root
                }
            }, {
                key: "parent",
                get: function() {
                    return this._routerState.parent(this)
                }
            }, {
                key: "firstChild",
                get: function() {
                    return this._routerState.firstChild(this)
                }
            }, {
                key: "children",
                get: function() {
                    return this._routerState.children(this)
                }
            }, {
                key: "pathFromRoot",
                get: function() {
                    return this._routerState.pathFromRoot(this)
                }
            }, {
                key: "paramMap",
                get: function() {
                    return this._paramMap || (this._paramMap = Ql(this.params)),
                    this._paramMap
                }
            }, {
                key: "queryParamMap",
                get: function() {
                    return this._queryParamMap || (this._queryParamMap = Ql(this.queryParams)),
                    this._queryParamMap
                }
            }]),
            t
        }()
          , Mc = function(t) {
            d(n, t);
            var e = y(n);
            function n(t, r) {
                var i;
                return m(this, n),
                (i = e.call(this, r)).url = t,
                Uc(o(i), r),
                i
            }
            return _(n, [{
                key: "toString",
                value: function() {
                    return Dc(this._root)
                }
            }]),
            n
        }(Sc);
        function Uc(t, e) {
            e.value._routerState = t,
            e.children.forEach((function(e) {
                return Uc(t, e)
            }
            ))
        }
        function Dc(t) {
            var e = t.children.length > 0 ? " { ".concat(t.children.map(Dc).join(", "), " } ") : "";
            return "".concat(t.value).concat(e)
        }
        function Hc(t) {
            if (t.snapshot) {
                var e = t.snapshot
                  , n = t._futureSnapshot;
                t.snapshot = n,
                Yl(e.queryParams, n.queryParams) || t.queryParams.next(n.queryParams),
                e.fragment !== n.fragment && t.fragment.next(n.fragment),
                Yl(e.params, n.params) || t.params.next(n.params),
                function(t, e) {
                    if (t.length !== e.length)
                        return !1;
                    for (var n = 0; n < t.length; ++n)
                        if (!Yl(t[n], e[n]))
                            return !1;
                    return !0
                }(e.url, n.url) || t.url.next(n.url),
                Yl(e.data, n.data) || t.data.next(n.data)
            } else
                t.snapshot = t._futureSnapshot,
                t.data.next(t._futureSnapshot.data)
        }
        function Lc(t, e) {
            var n, r;
            return Yl(t.params, e.params) && uc(n = t.url, r = e.url) && n.every((function(t, e) {
                return Yl(t.parameters, r[e].parameters)
            }
            )) && !(!t.parent != !e.parent) && (!t.parent || Lc(t.parent, e.parent))
        }
        function Fc(t, e, n, r, i) {
            if (0 === n.length)
                return zc(e.root, e.root, e, r, i);
            var o = function(t) {
                if ("string" == typeof t[0] && 1 === t.length && "/" === t[0])
                    return new qc(!0,0,t);
                var e = 0
                  , n = !1
                  , r = t.reduce((function(t, r, i) {
                    if ("object" == typeof r && null != r) {
                        if (r.outlets) {
                            var o = {};
                            return ec(r.outlets, (function(t, e) {
                                o[e] = "string" == typeof t ? t.split("/") : t
                            }
                            )),
                            [].concat(l(t), [{
                                outlets: o
                            }])
                        }
                        if (r.segmentPath)
                            return [].concat(l(t), [r.segmentPath])
                    }
                    return "string" != typeof r ? [].concat(l(t), [r]) : 0 === i ? (r.split("/").forEach((function(r, i) {
                        0 == i && "." === r || (0 == i && "" === r ? n = !0 : ".." === r ? e++ : "" != r && t.push(r))
                    }
                    )),
                    t) : [].concat(l(t), [r])
                }
                ), []);
                return new qc(n,e,r)
            }(n);
            if (o.toRoot())
                return zc(e.root, new oc([],{}), e, r, i);
            var a = function(t, e, n) {
                if (t.isAbsolute)
                    return new Bc(e.root,!0,0);
                if (-1 === n.snapshot._lastPathIndex) {
                    var r = n.snapshot._urlSegment;
                    return new Bc(r,r === e.root,0)
                }
                var i = Vc(t.commands[0]) ? 0 : 1;
                return function(t, e, n) {
                    for (var r = t, i = e, o = n; o > i; ) {
                        if (o -= i,
                        !(r = r.parent))
                            throw new Error("Invalid number of '../'");
                        i = r.segments.length
                    }
                    return new Bc(r,!1,i - o)
                }(n.snapshot._urlSegment, n.snapshot._lastPathIndex + i, t.numberOfDoubleDots)
            }(o, e, t)
              , u = a.processChildren ? Gc(a.segmentGroup, a.index, o.commands) : Wc(a.segmentGroup, a.index, o.commands);
            return zc(a.segmentGroup, u, e, r, i)
        }
        function Vc(t) {
            return "object" == typeof t && null != t && !t.outlets && !t.segmentPath
        }
        function zc(t, e, n, r, i) {
            var o = {};
            return r && ec(r, (function(t, e) {
                o[e] = Array.isArray(t) ? t.map((function(t) {
                    return "".concat(t)
                }
                )) : "".concat(t)
            }
            )),
            new ic(n.root === t ? e : function t(e, n, r) {
                var i = {};
                return ec(e.children, (function(e, o) {
                    i[o] = e === n ? r : t(e, n, r)
                }
                )),
                new oc(e.segments,i)
            }(n.root, t, e),o,i)
        }
        var qc = function() {
            function t(e, n, r) {
                if (m(this, t),
                this.isAbsolute = e,
                this.numberOfDoubleDots = n,
                this.commands = r,
                e && r.length > 0 && Vc(r[0]))
                    throw new Error("Root segment cannot have matrix parameters");
                var i = r.find((function(t) {
                    return "object" == typeof t && null != t && t.outlets
                }
                ));
                if (i && i !== tc(r))
                    throw new Error("{outlets:{}} has to be the last command")
            }
            return _(t, [{
                key: "toRoot",
                value: function() {
                    return this.isAbsolute && 1 === this.commands.length && "/" == this.commands[0]
                }
            }]),
            t
        }()
          , Bc = function t(e, n, r) {
            m(this, t),
            this.segmentGroup = e,
            this.processChildren = n,
            this.index = r
        };
        function Zc(t) {
            return "object" == typeof t && null != t && t.outlets ? t.outlets.primary : "".concat(t)
        }
        function Wc(t, e, n) {
            if (t || (t = new oc([],{})),
            0 === t.segments.length && t.hasChildren())
                return Gc(t, e, n);
            var r = function(t, e, n) {
                for (var r = 0, i = e, o = {
                    match: !1,
                    pathIndex: 0,
                    commandIndex: 0
                }; i < t.segments.length; ) {
                    if (r >= n.length)
                        return o;
                    var a = t.segments[i]
                      , u = Zc(n[r])
                      , s = r < n.length - 1 ? n[r + 1] : null;
                    if (i > 0 && void 0 === u)
                        break;
                    if (u && s && "object" == typeof s && void 0 === s.outlets) {
                        if (!Yc(u, s, a))
                            return o;
                        r += 2
                    } else {
                        if (!Yc(u, {}, a))
                            return o;
                        r++
                    }
                    i++
                }
                return {
                    match: !0,
                    pathIndex: i,
                    commandIndex: r
                }
            }(t, e, n)
              , i = n.slice(r.commandIndex);
            if (r.match && r.pathIndex < t.segments.length) {
                var o = new oc(t.segments.slice(0, r.pathIndex),{});
                return o.children.primary = new oc(t.segments.slice(r.pathIndex),t.children),
                Gc(o, 0, i)
            }
            return r.match && 0 === i.length ? new oc(t.segments,{}) : r.match && !t.hasChildren() ? Qc(t, e, n) : r.match ? Gc(t, 0, i) : Qc(t, e, n)
        }
        function Gc(t, e, n) {
            if (0 === n.length)
                return new oc(t.segments,{});
            var r = function(t) {
                return "object" == typeof t[0] && null !== t[0] && t[0].outlets ? t[0].outlets : c({}, Wl, t)
            }(n)
              , i = {};
            return ec(r, (function(n, r) {
                null !== n && (i[r] = Wc(t.children[r], e, n))
            }
            )),
            ec(t.children, (function(t, e) {
                void 0 === r[e] && (i[e] = t)
            }
            )),
            new oc(t.segments,i)
        }
        function Qc(t, e, n) {
            for (var r = t.segments.slice(0, e), i = 0; i < n.length; ) {
                if ("object" == typeof n[i] && null !== n[i] && void 0 !== n[i].outlets) {
                    var o = Jc(n[i].outlets);
                    return new oc(r,o)
                }
                if (0 === i && Vc(n[0]))
                    r.push(new ac(t.segments[e].path,n[0])),
                    i++;
                else {
                    var a = Zc(n[i])
                      , u = i < n.length - 1 ? n[i + 1] : null;
                    a && u && Vc(u) ? (r.push(new ac(a,Kc(u))),
                    i += 2) : (r.push(new ac(a,{})),
                    i++)
                }
            }
            return new oc(r,{})
        }
        function Jc(t) {
            var e = {};
            return ec(t, (function(t, n) {
                null !== t && (e[n] = Qc(new oc([],{}), 0, t))
            }
            )),
            e
        }
        function Kc(t) {
            var e = {};
            return ec(t, (function(t, n) {
                return e[n] = "".concat(t)
            }
            )),
            e
        }
        function Yc(t, e, n) {
            return t == n.path && Yl(e, n.parameters)
        }
        var Xc = function() {
            function t(e, n, r, i) {
                m(this, t),
                this.routeReuseStrategy = e,
                this.futureState = n,
                this.currState = r,
                this.forwardEvent = i
            }
            return _(t, [{
                key: "activate",
                value: function(t) {
                    var e = this.futureState._root
                      , n = this.currState ? this.currState._root : null;
                    this.deactivateChildRoutes(e, n, t),
                    Hc(this.futureState.root),
                    this.activateChildRoutes(e, n, t)
                }
            }, {
                key: "deactivateChildRoutes",
                value: function(t, e, n) {
                    var r = this
                      , i = Oc(e);
                    t.children.forEach((function(t) {
                        var e = t.value.outlet;
                        r.deactivateRoutes(t, i[e], n),
                        delete i[e]
                    }
                    )),
                    ec(i, (function(t, e) {
                        r.deactivateRouteAndItsChildren(t, n)
                    }
                    ))
                }
            }, {
                key: "deactivateRoutes",
                value: function(t, e, n) {
                    var r = t.value
                      , i = e ? e.value : null;
                    if (r === i)
                        if (r.component) {
                            var o = n.getContext(r.outlet);
                            o && this.deactivateChildRoutes(t, e, o.children)
                        } else
                            this.deactivateChildRoutes(t, e, n);
                    else
                        i && this.deactivateRouteAndItsChildren(e, n)
                }
            }, {
                key: "deactivateRouteAndItsChildren",
                value: function(t, e) {
                    this.routeReuseStrategy.shouldDetach(t.value.snapshot) ? this.detachAndStoreRouteSubtree(t, e) : this.deactivateRouteAndOutlet(t, e)
                }
            }, {
                key: "detachAndStoreRouteSubtree",
                value: function(t, e) {
                    var n = e.getContext(t.value.outlet);
                    if (n && n.outlet) {
                        var r = n.outlet.detach()
                          , i = n.children.onOutletDeactivated();
                        this.routeReuseStrategy.store(t.value.snapshot, {
                            componentRef: r,
                            route: t,
                            contexts: i
                        })
                    }
                }
            }, {
                key: "deactivateRouteAndOutlet",
                value: function(t, e) {
                    var n = this
                      , r = e.getContext(t.value.outlet);
                    if (r) {
                        var i = Oc(t)
                          , o = t.value.component ? r.children : e;
                        ec(i, (function(t, e) {
                            return n.deactivateRouteAndItsChildren(t, o)
                        }
                        )),
                        r.outlet && (r.outlet.deactivate(),
                        r.children.onOutletDeactivated())
                    }
                }
            }, {
                key: "activateChildRoutes",
                value: function(t, e, n) {
                    var r = this
                      , i = Oc(e);
                    t.children.forEach((function(t) {
                        r.activateRoutes(t, i[t.value.outlet], n),
                        r.forwardEvent(new Bl(t.value.snapshot))
                    }
                    )),
                    t.children.length && this.forwardEvent(new zl(t.value.snapshot))
                }
            }, {
                key: "activateRoutes",
                value: function(t, e, n) {
                    var r = t.value
                      , i = e ? e.value : null;
                    if (Hc(r),
                    r === i)
                        if (r.component) {
                            var o = n.getOrCreateContext(r.outlet);
                            this.activateChildRoutes(t, e, o.children)
                        } else
                            this.activateChildRoutes(t, e, n);
                    else if (r.component) {
                        var a = n.getOrCreateContext(r.outlet);
                        if (this.routeReuseStrategy.shouldAttach(r.snapshot)) {
                            var u = this.routeReuseStrategy.retrieve(r.snapshot);
                            this.routeReuseStrategy.store(r.snapshot, null),
                            a.children.onOutletReAttached(u.contexts),
                            a.attachRef = u.componentRef,
                            a.route = u.route.value,
                            a.outlet && a.outlet.attach(u.componentRef, u.route.value),
                            $c(u.route)
                        } else {
                            var s = function(t) {
                                for (var e = t.parent; e; e = e.parent) {
                                    var n = e.routeConfig;
                                    if (n && n._loadedConfig)
                                        return n._loadedConfig;
                                    if (n && n.component)
                                        return null
                                }
                                return null
                            }(r.snapshot)
                              , l = s ? s.module.componentFactoryResolver : null;
                            a.attachRef = null,
                            a.route = r,
                            a.resolver = l,
                            a.outlet && a.outlet.activateWith(r, l),
                            this.activateChildRoutes(t, null, a.children)
                        }
                    } else
                        this.activateChildRoutes(t, null, n)
                }
            }]),
            t
        }();
        function $c(t) {
            Hc(t.value),
            t.children.forEach($c)
        }
        var th = function t(e, n) {
            m(this, t),
            this.routes = e,
            this.module = n
        };
        function eh(t) {
            return "function" == typeof t
        }
        function nh(t) {
            return t instanceof ic
        }
        var rh = Symbol("INITIAL_VALUE");
        function ih() {
            return ll((function(t) {
                return Vs.apply(void 0, l(t.map((function(t) {
                    return t.pipe(fl(1), function() {
                        for (var t = arguments.length, e = new Array(t), n = 0; n < t; n++)
                            e[n] = arguments[n];
                        var r = e[e.length - 1];
                        return Z(r) ? (e.pop(),
                        function(t) {
                            return pl(e, t, r)
                        }
                        ) : function(t) {
                            return pl(e, t)
                        }
                    }(rh))
                }
                )))).pipe(function(t, e) {
                    var n = !1;
                    return arguments.length >= 2 && (n = !0),
                    function(r) {
                        return r.lift(new gl(t,e,n))
                    }
                }((function(t, e) {
                    var n = !1;
                    return e.reduce((function(t, r, i) {
                        if (t !== rh)
                            return t;
                        if (r === rh && (n = !0),
                        !n) {
                            if (!1 === r)
                                return r;
                            if (i === e.length - 1 || nh(r))
                                return r
                        }
                        return t
                    }
                    ), t)
                }
                ), rh), Js((function(t) {
                    return t !== rh
                }
                )), W((function(t) {
                    return nh(t) ? t : !0 === t
                }
                )), fl(1))
            }
            ))
        }
        var oh = function t(e) {
            m(this, t),
            this.segmentGroup = e || null
        }
          , ah = function t(e) {
            m(this, t),
            this.urlTree = e
        };
        function uh(t) {
            return new H((function(e) {
                return e.error(new oh(t))
            }
            ))
        }
        function sh(t) {
            return new H((function(e) {
                return e.error(new ah(t))
            }
            ))
        }
        function lh(t) {
            return new H((function(e) {
                return e.error(new Error("Only absolute redirects can have named outlets. redirectTo: '".concat(t, "'")))
            }
            ))
        }
        var ch = function() {
            function t(e, n, r, i, o) {
                m(this, t),
                this.configLoader = n,
                this.urlSerializer = r,
                this.urlTree = i,
                this.config = o,
                this.allowRedirects = !0,
                this.ngModule = e.get(ge)
            }
            return _(t, [{
                key: "apply",
                value: function() {
                    var t = this;
                    return this.expandSegmentGroup(this.ngModule, this.config, this.urlTree.root, Wl).pipe(W((function(e) {
                        return t.createUrlTree(e, t.urlTree.queryParams, t.urlTree.fragment)
                    }
                    ))).pipe(ml((function(e) {
                        if (e instanceof ah)
                            return t.allowRedirects = !1,
                            t.match(e.urlTree);
                        if (e instanceof oh)
                            throw t.noMatchError(e);
                        throw e
                    }
                    )))
                }
            }, {
                key: "match",
                value: function(t) {
                    var e = this;
                    return this.expandSegmentGroup(this.ngModule, this.config, t.root, Wl).pipe(W((function(n) {
                        return e.createUrlTree(n, t.queryParams, t.fragment)
                    }
                    ))).pipe(ml((function(t) {
                        if (t instanceof oh)
                            throw e.noMatchError(t);
                        throw t
                    }
                    )))
                }
            }, {
                key: "noMatchError",
                value: function(t) {
                    return new Error("Cannot match any routes. URL Segment: '".concat(t.segmentGroup, "'"))
                }
            }, {
                key: "createUrlTree",
                value: function(t, e, n) {
                    var r = t.segments.length > 0 ? new oc([],c({}, Wl, t)) : t;
                    return new ic(r,e,n)
                }
            }, {
                key: "expandSegmentGroup",
                value: function(t, e, n, r) {
                    return 0 === n.segments.length && n.hasChildren() ? this.expandChildren(t, e, n).pipe(W((function(t) {
                        return new oc([],t)
                    }
                    ))) : this.expandSegment(t, n, e, n.segments, r, !0)
                }
            }, {
                key: "expandChildren",
                value: function(t, e, n) {
                    var r = this;
                    return function(n, i) {
                        if (0 === Object.keys(n).length)
                            return Ms({});
                        var o = []
                          , a = []
                          , u = {};
                        return ec(n, (function(n, i) {
                            var s, l, c = (s = i,
                            l = n,
                            r.expandSegmentGroup(t, e, l, s)).pipe(W((function(t) {
                                return u[i] = t
                            }
                            )));
                            i === Wl ? o.push(c) : a.push(c)
                        }
                        )),
                        Ms.apply(null, o.concat(a)).pipe(Qs(), function(t, e) {
                            var n = arguments.length >= 2;
                            return function(r) {
                                return r.pipe(t ? Js((function(e, n) {
                                    return t(e, n, r)
                                }
                                )) : U, $s(1), n ? al(e) : nl((function() {
                                    return new Bs
                                }
                                )))
                            }
                        }(), W((function() {
                            return u
                        }
                        )))
                    }(n.children)
                }
            }, {
                key: "expandSegment",
                value: function(t, e, n, r, i, o) {
                    var a = this;
                    return Ms.apply(void 0, l(n)).pipe(kl((function(u) {
                        return a.expandSegmentAgainstRoute(t, e, n, u, r, i, o).pipe(ml((function(t) {
                            if (t instanceof oh)
                                return Ms(null);
                            throw t
                        }
                        )))
                    }
                    )), bl((function(t) {
                        return !!t
                    }
                    )), ml((function(t, n) {
                        if (t instanceof Bs || "EmptyError" === t.name) {
                            if (a.noLeftoversInUrl(e, r, i))
                                return Ms(new oc([],{}));
                            throw new oh(e)
                        }
                        throw t
                    }
                    )))
                }
            }, {
                key: "noLeftoversInUrl",
                value: function(t, e, n) {
                    return 0 === e.length && !t.children[n]
                }
            }, {
                key: "expandSegmentAgainstRoute",
                value: function(t, e, n, r, i, o, a) {
                    return vh(r) !== o ? uh(e) : void 0 === r.redirectTo ? this.matchSegmentAgainstRoute(t, e, r, i) : a && this.allowRedirects ? this.expandSegmentAgainstRouteUsingRedirect(t, e, n, r, i, o) : uh(e)
                }
            }, {
                key: "expandSegmentAgainstRouteUsingRedirect",
                value: function(t, e, n, r, i, o) {
                    return "**" === r.path ? this.expandWildCardWithParamsAgainstRouteUsingRedirect(t, n, r, o) : this.expandRegularSegmentAgainstRouteUsingRedirect(t, e, n, r, i, o)
                }
            }, {
                key: "expandWildCardWithParamsAgainstRouteUsingRedirect",
                value: function(t, e, n, r) {
                    var i = this
                      , o = this.applyRedirectCommands([], n.redirectTo, {});
                    return n.redirectTo.startsWith("/") ? sh(o) : this.lineralizeSegments(n, o).pipe(at((function(n) {
                        var o = new oc(n,{});
                        return i.expandSegment(t, o, e, n, r, !1)
                    }
                    )))
                }
            }, {
                key: "expandRegularSegmentAgainstRouteUsingRedirect",
                value: function(t, e, n, r, i, o) {
                    var a = this
                      , u = hh(e, r, i)
                      , s = u.consumedSegments
                      , l = u.lastChild
                      , c = u.positionalParamSegments;
                    if (!u.matched)
                        return uh(e);
                    var h = this.applyRedirectCommands(s, r.redirectTo, c);
                    return r.redirectTo.startsWith("/") ? sh(h) : this.lineralizeSegments(r, h).pipe(at((function(r) {
                        return a.expandSegment(t, e, n, r.concat(i.slice(l)), o, !1)
                    }
                    )))
                }
            }, {
                key: "matchSegmentAgainstRoute",
                value: function(t, e, n, r) {
                    var i = this;
                    if ("**" === n.path)
                        return n.loadChildren ? this.configLoader.load(t.injector, n).pipe(W((function(t) {
                            return n._loadedConfig = t,
                            new oc(r,{})
                        }
                        ))) : Ms(new oc(r,{}));
                    var o = hh(e, n, r)
                      , a = o.consumedSegments
                      , u = o.lastChild;
                    if (!o.matched)
                        return uh(e);
                    var s = r.slice(u);
                    return this.getChildConfig(t, n, r).pipe(at((function(t) {
                        var n = t.module
                          , r = t.routes
                          , o = function(t, e, n, r) {
                            return n.length > 0 && function(t, e, n) {
                                return n.some((function(n) {
                                    return dh(t, e, n) && vh(n) !== Wl
                                }
                                ))
                            }(t, n, r) ? {
                                segmentGroup: fh(new oc(e,function(t, e) {
                                    var n = {};
                                    n.primary = e;
                                    var r, i = h(t);
                                    try {
                                        for (i.s(); !(r = i.n()).done; ) {
                                            var o = r.value;
                                            "" === o.path && vh(o) !== Wl && (n[vh(o)] = new oc([],{}))
                                        }
                                    } catch (a) {
                                        i.e(a)
                                    } finally {
                                        i.f()
                                    }
                                    return n
                                }(r, new oc(n,t.children)))),
                                slicedSegments: []
                            } : 0 === n.length && function(t, e, n) {
                                return n.some((function(n) {
                                    return dh(t, e, n)
                                }
                                ))
                            }(t, n, r) ? {
                                segmentGroup: fh(new oc(t.segments,function(t, e, n, r) {
                                    var i, o = {}, a = h(n);
                                    try {
                                        for (a.s(); !(i = a.n()).done; ) {
                                            var u = i.value;
                                            dh(t, e, u) && !r[vh(u)] && (o[vh(u)] = new oc([],{}))
                                        }
                                    } catch (s) {
                                        a.e(s)
                                    } finally {
                                        a.f()
                                    }
                                    return Object.assign(Object.assign({}, r), o)
                                }(t, n, r, t.children))),
                                slicedSegments: n
                            } : {
                                segmentGroup: t,
                                slicedSegments: n
                            }
                        }(e, a, s, r)
                          , u = o.segmentGroup
                          , l = o.slicedSegments;
                        return 0 === l.length && u.hasChildren() ? i.expandChildren(n, r, u).pipe(W((function(t) {
                            return new oc(a,t)
                        }
                        ))) : 0 === r.length && 0 === l.length ? Ms(new oc(a,{})) : i.expandSegment(n, u, r, l, Wl, !0).pipe(W((function(t) {
                            return new oc(a.concat(t.segments),t.children)
                        }
                        )))
                    }
                    )))
                }
            }, {
                key: "getChildConfig",
                value: function(t, e, n) {
                    var r = this;
                    return e.children ? Ms(new th(e.children,t)) : e.loadChildren ? void 0 !== e._loadedConfig ? Ms(e._loadedConfig) : this.runCanLoadGuards(t.injector, e, n).pipe(at((function(n) {
                        return n ? r.configLoader.load(t.injector, e).pipe(W((function(t) {
                            return e._loadedConfig = t,
                            t
                        }
                        ))) : function(t) {
                            return new H((function(e) {
                                return e.error(Jl("Cannot load children because the guard of the route \"path: '".concat(t.path, "'\" returned false")))
                            }
                            ))
                        }(e)
                    }
                    ))) : Ms(new th([],t))
                }
            }, {
                key: "runCanLoadGuards",
                value: function(t, e, n) {
                    var r = this
                      , i = e.canLoad;
                    return i && 0 !== i.length ? Ms(i.map((function(r) {
                        var i, o = t.get(r);
                        if (function(t) {
                            return t && eh(t.canLoad)
                        }(o))
                            i = o.canLoad(e, n);
                        else {
                            if (!eh(o))
                                throw new Error("Invalid CanLoad guard");
                            i = o(e, n)
                        }
                        return nc(i)
                    }
                    ))).pipe(ih(), Sl((function(t) {
                        if (nh(t)) {
                            var e = Jl('Redirecting to "'.concat(r.urlSerializer.serialize(t), '"'));
                            throw e.url = t,
                            e
                        }
                    }
                    )), W((function(t) {
                        return !0 === t
                    }
                    ))) : Ms(!0)
                }
            }, {
                key: "lineralizeSegments",
                value: function(t, e) {
                    for (var n = [], r = e.root; ; ) {
                        if (n = n.concat(r.segments),
                        0 === r.numberOfChildren)
                            return Ms(n);
                        if (r.numberOfChildren > 1 || !r.children.primary)
                            return lh(t.redirectTo);
                        r = r.children.primary
                    }
                }
            }, {
                key: "applyRedirectCommands",
                value: function(t, e, n) {
                    return this.applyRedirectCreatreUrlTree(e, this.urlSerializer.parse(e), t, n)
                }
            }, {
                key: "applyRedirectCreatreUrlTree",
                value: function(t, e, n, r) {
                    var i = this.createSegmentGroup(t, e.root, n, r);
                    return new ic(i,this.createQueryParams(e.queryParams, this.urlTree.queryParams),e.fragment)
                }
            }, {
                key: "createQueryParams",
                value: function(t, e) {
                    var n = {};
                    return ec(t, (function(t, r) {
                        if ("string" == typeof t && t.startsWith(":")) {
                            var i = t.substring(1);
                            n[r] = e[i]
                        } else
                            n[r] = t
                    }
                    )),
                    n
                }
            }, {
                key: "createSegmentGroup",
                value: function(t, e, n, r) {
                    var i = this
                      , o = this.createSegments(t, e.segments, n, r)
                      , a = {};
                    return ec(e.children, (function(e, o) {
                        a[o] = i.createSegmentGroup(t, e, n, r)
                    }
                    )),
                    new oc(o,a)
                }
            }, {
                key: "createSegments",
                value: function(t, e, n, r) {
                    var i = this;
                    return e.map((function(e) {
                        return e.path.startsWith(":") ? i.findPosParam(t, e, r) : i.findOrReturn(e, n)
                    }
                    ))
                }
            }, {
                key: "findPosParam",
                value: function(t, e, n) {
                    var r = n[e.path.substring(1)];
                    if (!r)
                        throw new Error("Cannot redirect to '".concat(t, "'. Cannot find '").concat(e.path, "'."));
                    return r
                }
            }, {
                key: "findOrReturn",
                value: function(t, e) {
                    var n, r = 0, i = h(e);
                    try {
                        for (i.s(); !(n = i.n()).done; ) {
                            var o = n.value;
                            if (o.path === t.path)
                                return e.splice(r),
                                o;
                            r++
                        }
                    } catch (a) {
                        i.e(a)
                    } finally {
                        i.f()
                    }
                    return t
                }
            }]),
            t
        }();
        function hh(t, e, n) {
            if ("" === e.path)
                return "full" === e.pathMatch && (t.hasChildren() || n.length > 0) ? {
                    matched: !1,
                    consumedSegments: [],
                    lastChild: 0,
                    positionalParamSegments: {}
                } : {
                    matched: !0,
                    consumedSegments: [],
                    lastChild: 0,
                    positionalParamSegments: {}
                };
            var r = (e.matcher || Kl)(n, t, e);
            return r ? {
                matched: !0,
                consumedSegments: r.consumed,
                lastChild: r.consumed.length,
                positionalParamSegments: r.posParams
            } : {
                matched: !1,
                consumedSegments: [],
                lastChild: 0,
                positionalParamSegments: {}
            }
        }
        function fh(t) {
            if (1 === t.numberOfChildren && t.children.primary) {
                var e = t.children.primary;
                return new oc(t.segments.concat(e.segments),e.children)
            }
            return t
        }
        function dh(t, e, n) {
            return (!(t.hasChildren() || e.length > 0) || "full" !== n.pathMatch) && "" === n.path && void 0 !== n.redirectTo
        }
        function vh(t) {
            return t.outlet || Wl
        }
        var ph = function t(e) {
            m(this, t),
            this.path = e,
            this.route = this.path[this.path.length - 1]
        }
          , gh = function t(e, n) {
            m(this, t),
            this.component = e,
            this.route = n
        };
        function yh(t, e, n) {
            var r = function(t) {
                if (!t)
                    return null;
                for (var e = t.parent; e; e = e.parent) {
                    var n = e.routeConfig;
                    if (n && n._loadedConfig)
                        return n._loadedConfig
                }
                return null
            }(e);
            return (r ? r.module.injector : n).get(t)
        }
        function mh(t, e, n, r) {
            var i = arguments.length > 4 && void 0 !== arguments[4] ? arguments[4] : {
                canDeactivateChecks: [],
                canActivateChecks: []
            }
              , o = Oc(e);
            return t.children.forEach((function(t) {
                wh(t, o[t.value.outlet], n, r.concat([t.value]), i),
                delete o[t.value.outlet]
            }
            )),
            ec(o, (function(t, e) {
                return kh(t, n.getContext(e), i)
            }
            )),
            i
        }
        function wh(t, e, n, r) {
            var i = arguments.length > 4 && void 0 !== arguments[4] ? arguments[4] : {
                canDeactivateChecks: [],
                canActivateChecks: []
            }
              , o = t.value
              , a = e ? e.value : null
              , u = n ? n.getContext(t.value.outlet) : null;
            if (a && o.routeConfig === a.routeConfig) {
                var s = _h(a, o, o.routeConfig.runGuardsAndResolvers);
                s ? i.canActivateChecks.push(new ph(r)) : (o.data = a.data,
                o._resolvedData = a._resolvedData),
                mh(t, e, o.component ? u ? u.children : null : n, r, i),
                s && u && u.outlet && u.outlet.isActivated && i.canDeactivateChecks.push(new gh(u.outlet.component,a))
            } else
                a && kh(e, u, i),
                i.canActivateChecks.push(new ph(r)),
                mh(t, null, o.component ? u ? u.children : null : n, r, i);
            return i
        }
        function _h(t, e, n) {
            if ("function" == typeof n)
                return n(t, e);
            switch (n) {
            case "pathParamsChange":
                return !uc(t.url, e.url);
            case "pathParamsOrQueryParamsChange":
                return !uc(t.url, e.url) || !Yl(t.queryParams, e.queryParams);
            case "always":
                return !0;
            case "paramsOrQueryParamsChange":
                return !Lc(t, e) || !Yl(t.queryParams, e.queryParams);
            case "paramsChange":
            default:
                return !Lc(t, e)
            }
        }
        function kh(t, e, n) {
            var r = Oc(t)
              , i = t.value;
            ec(r, (function(t, r) {
                kh(t, i.component ? e ? e.children.getContext(r) : null : e, n)
            }
            )),
            n.canDeactivateChecks.push(new gh(i.component && e && e.outlet && e.outlet.isActivated ? e.outlet.component : null,i))
        }
        function bh(t, e) {
            return null !== t && e && e(new ql(t)),
            Ms(!0)
        }
        function Ch(t, e) {
            return null !== t && e && e(new Vl(t)),
            Ms(!0)
        }
        function Sh(t, e, n) {
            var r = e.routeConfig ? e.routeConfig.canActivate : null;
            return r && 0 !== r.length ? Ms(r.map((function(r) {
                return Gs((function() {
                    var i, o = yh(r, e, n);
                    if (function(t) {
                        return t && eh(t.canActivate)
                    }(o))
                        i = nc(o.canActivate(e, t));
                    else {
                        if (!eh(o))
                            throw new Error("Invalid CanActivate guard");
                        i = nc(o(e, t))
                    }
                    return i.pipe(bl())
                }
                ))
            }
            ))).pipe(ih()) : Ms(!0)
        }
        function xh(t, e, n) {
            var r = e[e.length - 1]
              , i = e.slice(0, e.length - 1).reverse().map((function(t) {
                return function(t) {
                    var e = t.routeConfig ? t.routeConfig.canActivateChild : null;
                    return e && 0 !== e.length ? {
                        node: t,
                        guards: e
                    } : null
                }(t)
            }
            )).filter((function(t) {
                return null !== t
            }
            )).map((function(e) {
                return Gs((function() {
                    return Ms(e.guards.map((function(i) {
                        var o, a = yh(i, e.node, n);
                        if (function(t) {
                            return t && eh(t.canActivateChild)
                        }(a))
                            o = nc(a.canActivateChild(r, t));
                        else {
                            if (!eh(a))
                                throw new Error("Invalid CanActivateChild guard");
                            o = nc(a(r, t))
                        }
                        return o.pipe(bl())
                    }
                    ))).pipe(ih())
                }
                ))
            }
            ));
            return Ms(i).pipe(ih())
        }
        var Eh = function t() {
            m(this, t)
        }
          , Th = function() {
            function t(e, n, r, i, o, a) {
                m(this, t),
                this.rootComponentType = e,
                this.config = n,
                this.urlTree = r,
                this.url = i,
                this.paramsInheritanceStrategy = o,
                this.relativeLinkResolution = a
            }
            return _(t, [{
                key: "recognize",
                value: function() {
                    try {
                        var t = Rh(this.urlTree.root, [], [], this.config, this.relativeLinkResolution).segmentGroup
                          , e = this.processSegmentGroup(this.config, t, Wl)
                          , n = new Nc([],Object.freeze({}),Object.freeze(Object.assign({}, this.urlTree.queryParams)),this.urlTree.fragment,{},Wl,this.rootComponentType,null,this.urlTree.root,-1,{})
                          , r = new Tc(n,e)
                          , i = new Mc(this.url,r);
                        return this.inheritParamsAndData(i._root),
                        Ms(i)
                    } catch (o) {
                        return new H((function(t) {
                            return t.error(o)
                        }
                        ))
                    }
                }
            }, {
                key: "inheritParamsAndData",
                value: function(t) {
                    var e = this
                      , n = t.value
                      , r = Ic(n, this.paramsInheritanceStrategy);
                    n.params = Object.freeze(r.params),
                    n.data = Object.freeze(r.data),
                    t.children.forEach((function(t) {
                        return e.inheritParamsAndData(t)
                    }
                    ))
                }
            }, {
                key: "processSegmentGroup",
                value: function(t, e, n) {
                    return 0 === e.segments.length && e.hasChildren() ? this.processChildren(t, e) : this.processSegment(t, e, e.segments, n)
                }
            }, {
                key: "processChildren",
                value: function(t, e) {
                    var n, r = this, i = sc(e, (function(e, n) {
                        return r.processSegmentGroup(t, e, n)
                    }
                    ));
                    return n = {},
                    i.forEach((function(t) {
                        var e = n[t.value.outlet];
                        if (e) {
                            var r = e.url.map((function(t) {
                                return t.toString()
                            }
                            )).join("/")
                              , i = t.value.url.map((function(t) {
                                return t.toString()
                            }
                            )).join("/");
                            throw new Error("Two segments cannot have the same outlet name: '".concat(r, "' and '").concat(i, "'."))
                        }
                        n[t.value.outlet] = t.value
                    }
                    )),
                    i.sort((function(t, e) {
                        return t.value.outlet === Wl ? -1 : e.value.outlet === Wl ? 1 : t.value.outlet.localeCompare(e.value.outlet)
                    }
                    )),
                    i
                }
            }, {
                key: "processSegment",
                value: function(t, e, n, r) {
                    var i, o = h(t);
                    try {
                        for (o.s(); !(i = o.n()).done; ) {
                            var a = i.value;
                            try {
                                return this.processSegmentAgainstRoute(a, e, n, r)
                            } catch (u) {
                                if (!(u instanceof Eh))
                                    throw u
                            }
                        }
                    } catch (s) {
                        o.e(s)
                    } finally {
                        o.f()
                    }
                    if (this.noLeftoversInUrl(e, n, r))
                        return [];
                    throw new Eh
                }
            }, {
                key: "noLeftoversInUrl",
                value: function(t, e, n) {
                    return 0 === e.length && !t.children[n]
                }
            }, {
                key: "processSegmentAgainstRoute",
                value: function(t, e, n, r) {
                    if (t.redirectTo)
                        throw new Eh;
                    if ((t.outlet || Wl) !== r)
                        throw new Eh;
                    var i, o = [], a = [];
                    if ("**" === t.path) {
                        var u = n.length > 0 ? tc(n).parameters : {};
                        i = new Nc(n,u,Object.freeze(Object.assign({}, this.urlTree.queryParams)),this.urlTree.fragment,jh(t),r,t.component,t,Oh(e),Ah(e) + n.length,Nh(t))
                    } else {
                        var s = function(t, e, n) {
                            if ("" === e.path) {
                                if ("full" === e.pathMatch && (t.hasChildren() || n.length > 0))
                                    throw new Eh;
                                return {
                                    consumedSegments: [],
                                    lastChild: 0,
                                    parameters: {}
                                }
                            }
                            var r = (e.matcher || Kl)(n, t, e);
                            if (!r)
                                throw new Eh;
                            var i = {};
                            ec(r.posParams, (function(t, e) {
                                i[e] = t.path
                            }
                            ));
                            var o = r.consumed.length > 0 ? Object.assign(Object.assign({}, i), r.consumed[r.consumed.length - 1].parameters) : i;
                            return {
                                consumedSegments: r.consumed,
                                lastChild: r.consumed.length,
                                parameters: o
                            }
                        }(e, t, n);
                        o = s.consumedSegments,
                        a = n.slice(s.lastChild),
                        i = new Nc(o,s.parameters,Object.freeze(Object.assign({}, this.urlTree.queryParams)),this.urlTree.fragment,jh(t),r,t.component,t,Oh(e),Ah(e) + o.length,Nh(t))
                    }
                    var l = function(t) {
                        return t.children ? t.children : t.loadChildren ? t._loadedConfig.routes : []
                    }(t)
                      , c = Rh(e, o, a, l, this.relativeLinkResolution)
                      , h = c.segmentGroup
                      , f = c.slicedSegments;
                    if (0 === f.length && h.hasChildren()) {
                        var d = this.processChildren(l, h);
                        return [new Tc(i,d)]
                    }
                    if (0 === l.length && 0 === f.length)
                        return [new Tc(i,[])];
                    var v = this.processSegment(l, h, f, Wl);
                    return [new Tc(i,v)]
                }
            }]),
            t
        }();
        function Oh(t) {
            for (var e = t; e._sourceSegment; )
                e = e._sourceSegment;
            return e
        }
        function Ah(t) {
            for (var e = t, n = e._segmentIndexShift ? e._segmentIndexShift : 0; e._sourceSegment; )
                n += (e = e._sourceSegment)._segmentIndexShift ? e._segmentIndexShift : 0;
            return n - 1
        }
        function Rh(t, e, n, r, i) {
            if (n.length > 0 && function(t, e, n) {
                return n.some((function(n) {
                    return Ph(t, e, n) && Ih(n) !== Wl
                }
                ))
            }(t, n, r)) {
                var o = new oc(e,function(t, e, n, r) {
                    var i = {};
                    i.primary = r,
                    r._sourceSegment = t,
                    r._segmentIndexShift = e.length;
                    var o, a = h(n);
                    try {
                        for (a.s(); !(o = a.n()).done; ) {
                            var u = o.value;
                            if ("" === u.path && Ih(u) !== Wl) {
                                var s = new oc([],{});
                                s._sourceSegment = t,
                                s._segmentIndexShift = e.length,
                                i[Ih(u)] = s
                            }
                        }
                    } catch (l) {
                        a.e(l)
                    } finally {
                        a.f()
                    }
                    return i
                }(t, e, r, new oc(n,t.children)));
                return o._sourceSegment = t,
                o._segmentIndexShift = e.length,
                {
                    segmentGroup: o,
                    slicedSegments: []
                }
            }
            if (0 === n.length && function(t, e, n) {
                return n.some((function(n) {
                    return Ph(t, e, n)
                }
                ))
            }(t, n, r)) {
                var a = new oc(t.segments,function(t, e, n, r, i, o) {
                    var a, u = {}, s = h(r);
                    try {
                        for (s.s(); !(a = s.n()).done; ) {
                            var l = a.value;
                            if (Ph(t, n, l) && !i[Ih(l)]) {
                                var c = new oc([],{});
                                c._sourceSegment = t,
                                c._segmentIndexShift = "legacy" === o ? t.segments.length : e.length,
                                u[Ih(l)] = c
                            }
                        }
                    } catch (f) {
                        s.e(f)
                    } finally {
                        s.f()
                    }
                    return Object.assign(Object.assign({}, i), u)
                }(t, e, n, r, t.children, i));
                return a._sourceSegment = t,
                a._segmentIndexShift = e.length,
                {
                    segmentGroup: a,
                    slicedSegments: n
                }
            }
            var u = new oc(t.segments,t.children);
            return u._sourceSegment = t,
            u._segmentIndexShift = e.length,
            {
                segmentGroup: u,
                slicedSegments: n
            }
        }
        function Ph(t, e, n) {
            return (!(t.hasChildren() || e.length > 0) || "full" !== n.pathMatch) && "" === n.path && void 0 === n.redirectTo
        }
        function Ih(t) {
            return t.outlet || Wl
        }
        function jh(t) {
            return t.data || {}
        }
        function Nh(t) {
            return t.resolve || {}
        }
        function Mh(t) {
            return function(e) {
                return e.pipe(ll((function(e) {
                    var n = t(e);
                    return n ? nt(n).pipe(W((function() {
                        return e
                    }
                    ))) : nt([e])
                }
                )))
            }
        }
        var Uh = function(t) {
            d(n, t);
            var e = y(n);
            function n() {
                return m(this, n),
                e.apply(this, arguments)
            }
            return n
        }(function() {
            function t() {
                m(this, t)
            }
            return _(t, [{
                key: "shouldDetach",
                value: function(t) {
                    return !1
                }
            }, {
                key: "store",
                value: function(t, e) {}
            }, {
                key: "shouldAttach",
                value: function(t) {
                    return !1
                }
            }, {
                key: "retrieve",
                value: function(t) {
                    return null
                }
            }, {
                key: "shouldReuseRoute",
                value: function(t, e) {
                    return t.routeConfig === e.routeConfig
                }
            }]),
            t
        }())
          , Dh = function() {
            var t = function t() {
                m(this, t)
            };
            return t.\u0275fac = function(e) {
                return new (e || t)
            }
            ,
            t.\u0275cmp = Ee({
                type: t,
                selectors: [["ng-component"]],
                decls: 1,
                vars: 0,
                template: function(t, e) {
                    1 & t && Eo(0, "router-outlet")
                },
                directives: function() {
                    return [Xh]
                },
                encapsulation: 2
            }),
            t
        }();
        function Hh(t) {
            for (var e = arguments.length > 1 && void 0 !== arguments[1] ? arguments[1] : "", n = 0; n < t.length; n++) {
                var r = t[n]
                  , i = Fh(e, r);
                Lh(r, i)
            }
        }
        function Lh(t, e) {
            if (!t)
                throw new Error("\n      Invalid configuration of route '".concat(e, "': Encountered undefined route.\n      The reason might be an extra comma.\n\n      Example:\n      const routes: Routes = [\n        { path: '', redirectTo: '/dashboard', pathMatch: 'full' },\n        { path: 'dashboard',  component: DashboardComponent },, << two commas\n        { path: 'detail/:id', component: HeroDetailComponent }\n      ];\n    "));
            if (Array.isArray(t))
                throw new Error("Invalid configuration of route '".concat(e, "': Array cannot be specified"));
            if (!t.component && !t.children && !t.loadChildren && t.outlet && t.outlet !== Wl)
                throw new Error("Invalid configuration of route '".concat(e, "': a componentless route without children or loadChildren cannot have a named outlet set"));
            if (t.redirectTo && t.children)
                throw new Error("Invalid configuration of route '".concat(e, "': redirectTo and children cannot be used together"));
            if (t.redirectTo && t.loadChildren)
                throw new Error("Invalid configuration of route '".concat(e, "': redirectTo and loadChildren cannot be used together"));
            if (t.children && t.loadChildren)
                throw new Error("Invalid configuration of route '".concat(e, "': children and loadChildren cannot be used together"));
            if (t.redirectTo && t.component)
                throw new Error("Invalid configuration of route '".concat(e, "': redirectTo and component cannot be used together"));
            if (t.path && t.matcher)
                throw new Error("Invalid configuration of route '".concat(e, "': path and matcher cannot be used together"));
            if (void 0 === t.redirectTo && !t.component && !t.children && !t.loadChildren)
                throw new Error("Invalid configuration of route '".concat(e, "'. One of the following must be provided: component, redirectTo, children or loadChildren"));
            if (void 0 === t.path && void 0 === t.matcher)
                throw new Error("Invalid configuration of route '".concat(e, "': routes must have either a path or a matcher specified"));
            if ("string" == typeof t.path && "/" === t.path.charAt(0))
                throw new Error("Invalid configuration of route '".concat(e, "': path cannot start with a slash"));
            if ("" === t.path && void 0 !== t.redirectTo && void 0 === t.pathMatch)
                throw new Error("Invalid configuration of route '{path: \"".concat(e, '", redirectTo: "').concat(t.redirectTo, "\"}': please provide 'pathMatch'. ").concat("The default value of 'pathMatch' is 'prefix', but often the intent is to use 'full'."));
            if (void 0 !== t.pathMatch && "full" !== t.pathMatch && "prefix" !== t.pathMatch)
                throw new Error("Invalid configuration of route '".concat(e, "': pathMatch can only be set to 'prefix' or 'full'"));
            t.children && Hh(t.children, e)
        }
        function Fh(t, e) {
            return e ? t || e.path ? t && !e.path ? "".concat(t, "/") : !t && e.path ? e.path : "".concat(t, "/").concat(e.path) : "" : t
        }
        function Vh(t) {
            var e = t.children && t.children.map(Vh)
              , n = e ? Object.assign(Object.assign({}, t), {
                children: e
            }) : Object.assign({}, t);
            return !n.component && (e || n.loadChildren) && n.outlet && n.outlet !== Wl && (n.component = Dh),
            n
        }
        var zh = new ee("ROUTES")
          , qh = function() {
            function t(e, n, r, i) {
                m(this, t),
                this.loader = e,
                this.compiler = n,
                this.onLoadStartListener = r,
                this.onLoadEndListener = i
            }
            return _(t, [{
                key: "load",
                value: function(t, e) {
                    var n = this;
                    return this.onLoadStartListener && this.onLoadStartListener(e),
                    this.loadModuleFactory(e.loadChildren).pipe(W((function(r) {
                        n.onLoadEndListener && n.onLoadEndListener(e);
                        var i = r.create(t);
                        return new th($l(i.injector.get(zh)).map(Vh),i)
                    }
                    )))
                }
            }, {
                key: "loadModuleFactory",
                value: function(t) {
                    var e = this;
                    return "string" == typeof t ? nt(this.loader.load(t)) : nc(t()).pipe(at((function(t) {
                        return t instanceof ye ? Ms(t) : nt(e.compiler.compileModuleAsync(t))
                    }
                    )))
                }
            }]),
            t
        }()
          , Bh = function t() {
            m(this, t),
            this.outlet = null,
            this.route = null,
            this.resolver = null,
            this.children = new Zh,
            this.attachRef = null
        }
          , Zh = function() {
            function t() {
                m(this, t),
                this.contexts = new Map
            }
            return _(t, [{
                key: "onChildOutletCreated",
                value: function(t, e) {
                    var n = this.getOrCreateContext(t);
                    n.outlet = e,
                    this.contexts.set(t, n)
                }
            }, {
                key: "onChildOutletDestroyed",
                value: function(t) {
                    var e = this.getContext(t);
                    e && (e.outlet = null)
                }
            }, {
                key: "onOutletDeactivated",
                value: function() {
                    var t = this.contexts;
                    return this.contexts = new Map,
                    t
                }
            }, {
                key: "onOutletReAttached",
                value: function(t) {
                    this.contexts = t
                }
            }, {
                key: "getOrCreateContext",
                value: function(t) {
                    var e = this.getContext(t);
                    return e || (e = new Bh,
                    this.contexts.set(t, e)),
                    e
                }
            }, {
                key: "getContext",
                value: function(t) {
                    return this.contexts.get(t) || null
                }
            }]),
            t
        }()
          , Wh = function() {
            function t() {
                m(this, t)
            }
            return _(t, [{
                key: "shouldProcessUrl",
                value: function(t) {
                    return !0
                }
            }, {
                key: "extract",
                value: function(t) {
                    return t
                }
            }, {
                key: "merge",
                value: function(t, e) {
                    return t
                }
            }]),
            t
        }();
        function Gh(t) {
            throw t
        }
        function Qh(t, e, n) {
            return e.parse("/")
        }
        function Jh(t, e) {
            return Ms(null)
        }
        var Kh = function() {
            var t = function() {
                function t(e, n, r, i, o, a, u, s) {
                    var l = this;
                    m(this, t),
                    this.rootComponentType = e,
                    this.urlSerializer = n,
                    this.rootContexts = r,
                    this.location = i,
                    this.config = s,
                    this.lastSuccessfulNavigation = null,
                    this.currentNavigation = null,
                    this.lastLocationChangeInfo = null,
                    this.navigationId = 0,
                    this.isNgZoneEnabled = !1,
                    this.events = new q,
                    this.errorHandler = Gh,
                    this.malformedUriErrorHandler = Qh,
                    this.navigated = !1,
                    this.lastSuccessfulId = -1,
                    this.hooks = {
                        beforePreactivation: Jh,
                        afterPreactivation: Jh
                    },
                    this.urlHandlingStrategy = new Wh,
                    this.routeReuseStrategy = new Uh,
                    this.onSameUrlNavigation = "ignore",
                    this.paramsInheritanceStrategy = "emptyOnly",
                    this.urlUpdateStrategy = "deferred",
                    this.relativeLinkResolution = "legacy",
                    this.ngModule = o.get(ge),
                    this.console = o.get(Ma);
                    var c = o.get(Qa);
                    this.isNgZoneEnabled = c instanceof Qa,
                    this.resetConfig(s),
                    this.currentUrlTree = new ic(new oc([],{}),{},null),
                    this.rawUrlTree = this.currentUrlTree,
                    this.browserUrlTree = this.currentUrlTree,
                    this.configLoader = new qh(a,u,(function(t) {
                        return l.triggerEvent(new Ll(t))
                    }
                    ),(function(t) {
                        return l.triggerEvent(new Fl(t))
                    }
                    )),
                    this.routerState = Rc(this.currentUrlTree, this.rootComponentType),
                    this.transitions = new Us({
                        id: 0,
                        currentUrlTree: this.currentUrlTree,
                        currentRawUrl: this.currentUrlTree,
                        extractedUrl: this.urlHandlingStrategy.extract(this.currentUrlTree),
                        urlAfterRedirects: this.urlHandlingStrategy.extract(this.currentUrlTree),
                        rawUrl: this.currentUrlTree,
                        extras: {},
                        resolve: null,
                        reject: null,
                        promise: Promise.resolve(!0),
                        source: "imperative",
                        restoredState: null,
                        currentSnapshot: this.routerState.snapshot,
                        targetSnapshot: null,
                        currentRouterState: this.routerState,
                        targetRouterState: null,
                        guards: {
                            canActivateChecks: [],
                            canDeactivateChecks: []
                        },
                        guardsResult: null
                    }),
                    this.navigations = this.setupNavigations(this.transitions),
                    this.processNavigations()
                }
                return _(t, [{
                    key: "setupNavigations",
                    value: function(t) {
                        var e = this
                          , n = this.events;
                        return t.pipe(Js((function(t) {
                            return 0 !== t.id
                        }
                        )), W((function(t) {
                            return Object.assign(Object.assign({}, t), {
                                extractedUrl: e.urlHandlingStrategy.extract(t.rawUrl)
                            })
                        }
                        )), ll((function(t) {
                            var r, i, o, a, u = !1, s = !1;
                            return Ms(t).pipe(Sl((function(t) {
                                e.currentNavigation = {
                                    id: t.id,
                                    initialUrl: t.currentRawUrl,
                                    extractedUrl: t.extractedUrl,
                                    trigger: t.source,
                                    extras: t.extras,
                                    previousNavigation: e.lastSuccessfulNavigation ? Object.assign(Object.assign({}, e.lastSuccessfulNavigation), {
                                        previousNavigation: null
                                    }) : null
                                }
                            }
                            )), ll((function(t) {
                                var r, i, o, a, u = !e.navigated || t.extractedUrl.toString() !== e.browserUrlTree.toString();
                                if (("reload" === e.onSameUrlNavigation || u) && e.urlHandlingStrategy.shouldProcessUrl(t.rawUrl))
                                    return Ms(t).pipe(ll((function(t) {
                                        var r = e.transitions.getValue();
                                        return n.next(new Rl(t.id,e.serializeUrl(t.extractedUrl),t.source,t.restoredState)),
                                        r !== e.transitions.getValue() ? Zs : [t]
                                    }
                                    )), ll((function(t) {
                                        return Promise.resolve(t)
                                    }
                                    )), (r = e.ngModule.injector,
                                    i = e.configLoader,
                                    o = e.urlSerializer,
                                    a = e.config,
                                    function(t) {
                                        return t.pipe(ll((function(t) {
                                            return function(t, e, n, r, i) {
                                                return new ch(t,e,n,r,i).apply()
                                            }(r, i, o, t.extractedUrl, a).pipe(W((function(e) {
                                                return Object.assign(Object.assign({}, t), {
                                                    urlAfterRedirects: e
                                                })
                                            }
                                            )))
                                        }
                                        )))
                                    }
                                    ), Sl((function(t) {
                                        e.currentNavigation = Object.assign(Object.assign({}, e.currentNavigation), {
                                            finalUrl: t.urlAfterRedirects
                                        })
                                    }
                                    )), function(t, n, r, i, o) {
                                        return function(r) {
                                            return r.pipe(at((function(r) {
                                                return function(t, e, n, r) {
                                                    return new Th(t,e,n,r,arguments.length > 4 && void 0 !== arguments[4] ? arguments[4] : "emptyOnly",arguments.length > 5 && void 0 !== arguments[5] ? arguments[5] : "legacy").recognize()
                                                }(t, n, r.urlAfterRedirects, (a = r.urlAfterRedirects,
                                                e.serializeUrl(a)), i, o).pipe(W((function(t) {
                                                    return Object.assign(Object.assign({}, r), {
                                                        targetSnapshot: t
                                                    })
                                                }
                                                )));
                                                var a
                                            }
                                            )))
                                        }
                                    }(e.rootComponentType, e.config, 0, e.paramsInheritanceStrategy, e.relativeLinkResolution), Sl((function(t) {
                                        "eager" === e.urlUpdateStrategy && (t.extras.skipLocationChange || e.setBrowserUrl(t.urlAfterRedirects, !!t.extras.replaceUrl, t.id, t.extras.state),
                                        e.browserUrlTree = t.urlAfterRedirects)
                                    }
                                    )), Sl((function(t) {
                                        var r = new Nl(t.id,e.serializeUrl(t.extractedUrl),e.serializeUrl(t.urlAfterRedirects),t.targetSnapshot);
                                        n.next(r)
                                    }
                                    )));
                                if (u && e.rawUrlTree && e.urlHandlingStrategy.shouldProcessUrl(e.rawUrlTree)) {
                                    var s = t.extractedUrl
                                      , l = t.source
                                      , c = t.restoredState
                                      , h = t.extras
                                      , f = new Rl(t.id,e.serializeUrl(s),l,c);
                                    n.next(f);
                                    var d = Rc(s, e.rootComponentType).snapshot;
                                    return Ms(Object.assign(Object.assign({}, t), {
                                        targetSnapshot: d,
                                        urlAfterRedirects: s,
                                        extras: Object.assign(Object.assign({}, h), {
                                            skipLocationChange: !1,
                                            replaceUrl: !1
                                        })
                                    }))
                                }
                                return e.rawUrlTree = t.rawUrl,
                                e.browserUrlTree = t.urlAfterRedirects,
                                t.resolve(null),
                                Zs
                            }
                            )), Mh((function(t) {
                                var n = t.extras;
                                return e.hooks.beforePreactivation(t.targetSnapshot, {
                                    navigationId: t.id,
                                    appliedUrlTree: t.extractedUrl,
                                    rawUrlTree: t.rawUrl,
                                    skipLocationChange: !!n.skipLocationChange,
                                    replaceUrl: !!n.replaceUrl
                                })
                            }
                            )), Sl((function(t) {
                                var n = new Ml(t.id,e.serializeUrl(t.extractedUrl),e.serializeUrl(t.urlAfterRedirects),t.targetSnapshot);
                                e.triggerEvent(n)
                            }
                            )), W((function(t) {
                                return Object.assign(Object.assign({}, t), {
                                    guards: (n = t.targetSnapshot,
                                    r = t.currentSnapshot,
                                    i = e.rootContexts,
                                    o = n._root,
                                    mh(o, r ? r._root : null, i, [o.value]))
                                });
                                var n, r, i, o
                            }
                            )), function(t, e) {
                                return function(n) {
                                    return n.pipe(at((function(n) {
                                        var r = n.targetSnapshot
                                          , i = n.currentSnapshot
                                          , o = n.guards
                                          , a = o.canActivateChecks
                                          , u = o.canDeactivateChecks;
                                        return 0 === u.length && 0 === a.length ? Ms(Object.assign(Object.assign({}, n), {
                                            guardsResult: !0
                                        })) : function(t, e, n, r) {
                                            return nt(t).pipe(at((function(t) {
                                                return function(t, e, n, r, i) {
                                                    var o = e && e.routeConfig ? e.routeConfig.canDeactivate : null;
                                                    return o && 0 !== o.length ? Ms(o.map((function(o) {
                                                        var a, u = yh(o, e, i);
                                                        if (function(t) {
                                                            return t && eh(t.canDeactivate)
                                                        }(u))
                                                            a = nc(u.canDeactivate(t, e, n, r));
                                                        else {
                                                            if (!eh(u))
                                                                throw new Error("Invalid CanDeactivate guard");
                                                            a = nc(u(t, e, n, r))
                                                        }
                                                        return a.pipe(bl())
                                                    }
                                                    ))).pipe(ih()) : Ms(!0)
                                                }(t.component, t.route, n, e, r)
                                            }
                                            )), bl((function(t) {
                                                return !0 !== t
                                            }
                                            ), !0))
                                        }(u, r, i, t).pipe(at((function(n) {
                                            return n && "boolean" == typeof n ? function(t, e, n, r) {
                                                return nt(e).pipe(kl((function(e) {
                                                    return nt([Ch(e.route.parent, r), bh(e.route, r), xh(t, e.path, n), Sh(t, e.route, n)]).pipe(Qs(), bl((function(t) {
                                                        return !0 !== t
                                                    }
                                                    ), !0))
                                                }
                                                )), bl((function(t) {
                                                    return !0 !== t
                                                }
                                                ), !0))
                                            }(r, a, t, e) : Ms(n)
                                        }
                                        )), W((function(t) {
                                            return Object.assign(Object.assign({}, n), {
                                                guardsResult: t
                                            })
                                        }
                                        )))
                                    }
                                    )))
                                }
                            }(e.ngModule.injector, (function(t) {
                                return e.triggerEvent(t)
                            }
                            )), Sl((function(t) {
                                if (nh(t.guardsResult)) {
                                    var n = Jl('Redirecting to "'.concat(e.serializeUrl(t.guardsResult), '"'));
                                    throw n.url = t.guardsResult,
                                    n
                                }
                            }
                            )), Sl((function(t) {
                                var n = new Ul(t.id,e.serializeUrl(t.extractedUrl),e.serializeUrl(t.urlAfterRedirects),t.targetSnapshot,!!t.guardsResult);
                                e.triggerEvent(n)
                            }
                            )), Js((function(t) {
                                if (!t.guardsResult) {
                                    e.resetUrlToCurrentUrlTree();
                                    var r = new Il(t.id,e.serializeUrl(t.extractedUrl),"");
                                    return n.next(r),
                                    t.resolve(!1),
                                    !1
                                }
                                return !0
                            }
                            )), Mh((function(t) {
                                if (t.guards.canActivateChecks.length)
                                    return Ms(t).pipe(Sl((function(t) {
                                        var n = new Dl(t.id,e.serializeUrl(t.extractedUrl),e.serializeUrl(t.urlAfterRedirects),t.targetSnapshot);
                                        e.triggerEvent(n)
                                    }
                                    )), ll((function(t) {
                                        var r, i, o = !1;
                                        return Ms(t).pipe((r = e.paramsInheritanceStrategy,
                                        i = e.ngModule.injector,
                                        function(t) {
                                            return t.pipe(at((function(t) {
                                                var e = t.targetSnapshot
                                                  , n = t.guards.canActivateChecks;
                                                if (!n.length)
                                                    return Ms(t);
                                                var o = 0;
                                                return nt(n).pipe(kl((function(t) {
                                                    return function(t, e, n, r) {
                                                        return function(t, e, n, r) {
                                                            var i = Object.keys(t);
                                                            if (0 === i.length)
                                                                return Ms({});
                                                            var o = {};
                                                            return nt(i).pipe(at((function(i) {
                                                                return function(t, e, n, r) {
                                                                    var i = yh(t, e, r);
                                                                    return nc(i.resolve ? i.resolve(e, n) : i(e, n))
                                                                }(t[i], e, n, r).pipe(Sl((function(t) {
                                                                    o[i] = t
                                                                }
                                                                )))
                                                            }
                                                            )), $s(1), at((function() {
                                                                return Object.keys(o).length === i.length ? Ms(o) : Zs
                                                            }
                                                            )))
                                                        }(t._resolve, t, e, r).pipe(W((function(e) {
                                                            return t._resolvedData = e,
                                                            t.data = Object.assign(Object.assign({}, t.data), Ic(t, n).resolve),
                                                            null
                                                        }
                                                        )))
                                                    }(t.route, e, r, i)
                                                }
                                                )), Sl((function() {
                                                    return o++
                                                }
                                                )), $s(1), at((function(e) {
                                                    return o === n.length ? Ms(t) : Zs
                                                }
                                                )))
                                            }
                                            )))
                                        }
                                        ), Sl({
                                            next: function() {
                                                return o = !0
                                            },
                                            complete: function() {
                                                if (!o) {
                                                    var r = new Il(t.id,e.serializeUrl(t.extractedUrl),"At least one route resolver didn't emit any value.");
                                                    n.next(r),
                                                    t.resolve(!1)
                                                }
                                            }
                                        }))
                                    }
                                    )), Sl((function(t) {
                                        var n = new Hl(t.id,e.serializeUrl(t.extractedUrl),e.serializeUrl(t.urlAfterRedirects),t.targetSnapshot);
                                        e.triggerEvent(n)
                                    }
                                    )))
                            }
                            )), Mh((function(t) {
                                var n = t.extras;
                                return e.hooks.afterPreactivation(t.targetSnapshot, {
                                    navigationId: t.id,
                                    appliedUrlTree: t.extractedUrl,
                                    rawUrlTree: t.rawUrl,
                                    skipLocationChange: !!n.skipLocationChange,
                                    replaceUrl: !!n.replaceUrl
                                })
                            }
                            )), W((function(t) {
                                var n, r, i, o = (i = function t(e, n, r) {
                                    if (r && e.shouldReuseRoute(n.value, r.value.snapshot)) {
                                        var i = r.value;
                                        i._futureSnapshot = n.value;
                                        var o = function(e, n, r) {
                                            return n.children.map((function(n) {
                                                var i, o = h(r.children);
                                                try {
                                                    for (o.s(); !(i = o.n()).done; ) {
                                                        var a = i.value;
                                                        if (e.shouldReuseRoute(a.value.snapshot, n.value))
                                                            return t(e, n, a)
                                                    }
                                                } catch (u) {
                                                    o.e(u)
                                                } finally {
                                                    o.f()
                                                }
                                                return t(e, n)
                                            }
                                            ))
                                        }(e, n, r);
                                        return new Tc(i,o)
                                    }
                                    var a = e.retrieve(n.value);
                                    if (a) {
                                        var u = a.route;
                                        return function t(e, n) {
                                            if (e.value.routeConfig !== n.value.routeConfig)
                                                throw new Error("Cannot reattach ActivatedRouteSnapshot created from a different route");
                                            if (e.children.length !== n.children.length)
                                                throw new Error("Cannot reattach ActivatedRouteSnapshot with a different number of children");
                                            n.value._futureSnapshot = e.value;
                                            for (var r = 0; r < e.children.length; ++r)
                                                t(e.children[r], n.children[r])
                                        }(n, u),
                                        u
                                    }
                                    var s, l = new Pc(new Us((s = n.value).url),new Us(s.params),new Us(s.queryParams),new Us(s.fragment),new Us(s.data),s.outlet,s.component,s), c = n.children.map((function(n) {
                                        return t(e, n)
                                    }
                                    ));
                                    return new Tc(l,c)
                                }(e.routeReuseStrategy, (n = t.targetSnapshot)._root, (r = t.currentRouterState) ? r._root : void 0),
                                new Ac(i,n));
                                return Object.assign(Object.assign({}, t), {
                                    targetRouterState: o
                                })
                            }
                            )), Sl((function(t) {
                                e.currentUrlTree = t.urlAfterRedirects,
                                e.rawUrlTree = e.urlHandlingStrategy.merge(e.currentUrlTree, t.rawUrl),
                                e.routerState = t.targetRouterState,
                                "deferred" === e.urlUpdateStrategy && (t.extras.skipLocationChange || e.setBrowserUrl(e.rawUrlTree, !!t.extras.replaceUrl, t.id, t.extras.state),
                                e.browserUrlTree = t.urlAfterRedirects)
                            }
                            )), (i = e.rootContexts,
                            o = e.routeReuseStrategy,
                            a = function(t) {
                                return e.triggerEvent(t)
                            }
                            ,
                            W((function(t) {
                                return new Xc(o,t.targetRouterState,t.currentRouterState,a).activate(i),
                                t
                            }
                            ))), Sl({
                                next: function() {
                                    u = !0
                                },
                                complete: function() {
                                    u = !0
                                }
                            }), (r = function() {
                                if (!u && !s) {
                                    e.resetUrlToCurrentUrlTree();
                                    var r = new Il(t.id,e.serializeUrl(t.extractedUrl),"Navigation ID ".concat(t.id, " is not equal to the current navigation id ").concat(e.navigationId));
                                    n.next(r),
                                    t.resolve(!1)
                                }
                                e.currentNavigation = null
                            }
                            ,
                            function(t) {
                                return t.lift(new Tl(r))
                            }
                            ), ml((function(r) {
                                if (s = !0,
                                (u = r) && u.ngNavigationCancelingError) {
                                    var i = nh(r.url);
                                    i || (e.navigated = !0,
                                    e.resetStateAndUrl(t.currentRouterState, t.currentUrlTree, t.rawUrl));
                                    var o = new Il(t.id,e.serializeUrl(t.extractedUrl),r.message);
                                    n.next(o),
                                    i ? setTimeout((function() {
                                        var n = e.urlHandlingStrategy.merge(r.url, e.rawUrlTree);
                                        return e.scheduleNavigation(n, "imperative", null, {
                                            skipLocationChange: t.extras.skipLocationChange,
                                            replaceUrl: "eager" === e.urlUpdateStrategy
                                        }, {
                                            resolve: t.resolve,
                                            reject: t.reject,
                                            promise: t.promise
                                        })
                                    }
                                    ), 0) : t.resolve(!1)
                                } else {
                                    e.resetStateAndUrl(t.currentRouterState, t.currentUrlTree, t.rawUrl);
                                    var a = new jl(t.id,e.serializeUrl(t.extractedUrl),r);
                                    n.next(a);
                                    try {
                                        t.resolve(e.errorHandler(r))
                                    } catch (l) {
                                        t.reject(l)
                                    }
                                }
                                var u;
                                return Zs
                            }
                            )))
                        }
                        )))
                    }
                }, {
                    key: "resetRootComponentType",
                    value: function(t) {
                        this.rootComponentType = t,
                        this.routerState.root.component = this.rootComponentType
                    }
                }, {
                    key: "getTransition",
                    value: function() {
                        var t = this.transitions.value;
                        return t.urlAfterRedirects = this.browserUrlTree,
                        t
                    }
                }, {
                    key: "setTransition",
                    value: function(t) {
                        this.transitions.next(Object.assign(Object.assign({}, this.getTransition()), t))
                    }
                }, {
                    key: "initialNavigation",
                    value: function() {
                        this.setUpLocationChangeListener(),
                        0 === this.navigationId && this.navigateByUrl(this.location.path(!0), {
                            replaceUrl: !0
                        })
                    }
                }, {
                    key: "setUpLocationChangeListener",
                    value: function() {
                        var t = this;
                        this.locationSubscription || (this.locationSubscription = this.location.subscribe((function(e) {
                            var n = t.extractLocationChangeInfoFromEvent(e);
                            t.shouldScheduleNavigation(t.lastLocationChangeInfo, n) && setTimeout((function() {
                                var e = n.source
                                  , r = n.state
                                  , i = n.urlTree
                                  , o = {
                                    replaceUrl: !0
                                };
                                if (r) {
                                    var a = Object.assign({}, r);
                                    delete a.navigationId,
                                    0 !== Object.keys(a).length && (o.state = a)
                                }
                                t.scheduleNavigation(i, e, r, o)
                            }
                            ), 0),
                            t.lastLocationChangeInfo = n
                        }
                        )))
                    }
                }, {
                    key: "extractLocationChangeInfoFromEvent",
                    value: function(t) {
                        var e;
                        return {
                            source: "popstate" === t.type ? "popstate" : "hashchange",
                            urlTree: this.parseUrl(t.url),
                            state: (null === (e = t.state) || void 0 === e ? void 0 : e.navigationId) ? t.state : null,
                            transitionId: this.getTransition().id
                        }
                    }
                }, {
                    key: "shouldScheduleNavigation",
                    value: function(t, e) {
                        if (!t)
                            return !0;
                        var n = e.urlTree.toString() === t.urlTree.toString();
                        return !(e.transitionId === t.transitionId && n && ("hashchange" === e.source && "popstate" === t.source || "popstate" === e.source && "hashchange" === t.source))
                    }
                }, {
                    key: "getCurrentNavigation",
                    value: function() {
                        return this.currentNavigation
                    }
                }, {
                    key: "triggerEvent",
                    value: function(t) {
                        this.events.next(t)
                    }
                }, {
                    key: "resetConfig",
                    value: function(t) {
                        Hh(t),
                        this.config = t.map(Vh),
                        this.navigated = !1,
                        this.lastSuccessfulId = -1
                    }
                }, {
                    key: "ngOnDestroy",
                    value: function() {
                        this.dispose()
                    }
                }, {
                    key: "dispose",
                    value: function() {
                        this.locationSubscription && (this.locationSubscription.unsubscribe(),
                        this.locationSubscription = void 0)
                    }
                }, {
                    key: "createUrlTree",
                    value: function(t) {
                        var e = arguments.length > 1 && void 0 !== arguments[1] ? arguments[1] : {}
                          , n = e.relativeTo
                          , r = e.queryParams
                          , i = e.fragment
                          , o = e.preserveQueryParams
                          , a = e.queryParamsHandling
                          , u = e.preserveFragment;
                        br() && o && console && console.warn && console.warn("preserveQueryParams is deprecated, use queryParamsHandling instead.");
                        var s = n || this.routerState.root
                          , l = u ? this.currentUrlTree.fragment : i
                          , c = null;
                        if (a)
                            switch (a) {
                            case "merge":
                                c = Object.assign(Object.assign({}, this.currentUrlTree.queryParams), r);
                                break;
                            case "preserve":
                                c = this.currentUrlTree.queryParams;
                                break;
                            default:
                                c = r || null
                            }
                        else
                            c = o ? this.currentUrlTree.queryParams : r || null;
                        return null !== c && (c = this.removeEmptyProps(c)),
                        Fc(s, this.currentUrlTree, t, c, l)
                    }
                }, {
                    key: "navigateByUrl",
                    value: function(t) {
                        var e = arguments.length > 1 && void 0 !== arguments[1] ? arguments[1] : {
                            skipLocationChange: !1
                        };
                        br() && this.isNgZoneEnabled && !Qa.isInAngularZone() && this.console.warn("Navigation triggered outside Angular zone, did you forget to call 'ngZone.run()'?");
                        var n = nh(t) ? t : this.parseUrl(t)
                          , r = this.urlHandlingStrategy.merge(n, this.rawUrlTree);
                        return this.scheduleNavigation(r, "imperative", null, e)
                    }
                }, {
                    key: "navigate",
                    value: function(t) {
                        var e = arguments.length > 1 && void 0 !== arguments[1] ? arguments[1] : {
                            skipLocationChange: !1
                        };
                        return Yh(t),
                        this.navigateByUrl(this.createUrlTree(t, e), e)
                    }
                }, {
                    key: "serializeUrl",
                    value: function(t) {
                        return this.urlSerializer.serialize(t)
                    }
                }, {
                    key: "parseUrl",
                    value: function(t) {
                        var e;
                        try {
                            e = this.urlSerializer.parse(t)
                        } catch (n) {
                            e = this.malformedUriErrorHandler(n, this.urlSerializer, t)
                        }
                        return e
                    }
                }, {
                    key: "isActive",
                    value: function(t, e) {
                        if (nh(t))
                            return rc(this.currentUrlTree, t, e);
                        var n = this.parseUrl(t);
                        return rc(this.currentUrlTree, n, e)
                    }
                }, {
                    key: "removeEmptyProps",
                    value: function(t) {
                        return Object.keys(t).reduce((function(e, n) {
                            var r = t[n];
                            return null != r && (e[n] = r),
                            e
                        }
                        ), {})
                    }
                }, {
                    key: "processNavigations",
                    value: function() {
                        var t = this;
                        this.navigations.subscribe((function(e) {
                            t.navigated = !0,
                            t.lastSuccessfulId = e.id,
                            t.events.next(new Pl(e.id,t.serializeUrl(e.extractedUrl),t.serializeUrl(t.currentUrlTree))),
                            t.lastSuccessfulNavigation = t.currentNavigation,
                            t.currentNavigation = null,
                            e.resolve(!0)
                        }
                        ), (function(e) {
                            t.console.warn("Unhandled Navigation Error: ")
                        }
                        ))
                    }
                }, {
                    key: "scheduleNavigation",
                    value: function(t, e, n, r, i) {
                        var o, a, u, s = this.getTransition(), l = "imperative" !== e && "imperative" === (null == s ? void 0 : s.source), c = (this.lastSuccessfulId === s.id || this.currentNavigation ? s.rawUrl : s.urlAfterRedirects).toString() === t.toString();
                        if (l && c)
                            return Promise.resolve(!0);
                        i ? (o = i.resolve,
                        a = i.reject,
                        u = i.promise) : u = new Promise((function(t, e) {
                            o = t,
                            a = e
                        }
                        ));
                        var h = ++this.navigationId;
                        return this.setTransition({
                            id: h,
                            source: e,
                            restoredState: n,
                            currentUrlTree: this.currentUrlTree,
                            currentRawUrl: this.rawUrlTree,
                            rawUrl: t,
                            extras: r,
                            resolve: o,
                            reject: a,
                            promise: u,
                            currentSnapshot: this.routerState.snapshot,
                            currentRouterState: this.routerState
                        }),
                        u.catch((function(t) {
                            return Promise.reject(t)
                        }
                        ))
                    }
                }, {
                    key: "setBrowserUrl",
                    value: function(t, e, n, r) {
                        var i = this.urlSerializer.serialize(t);
                        r = r || {},
                        this.location.isCurrentPathEqualTo(i) || e ? this.location.replaceState(i, "", Object.assign(Object.assign({}, r), {
                            navigationId: n
                        })) : this.location.go(i, "", Object.assign(Object.assign({}, r), {
                            navigationId: n
                        }))
                    }
                }, {
                    key: "resetStateAndUrl",
                    value: function(t, e, n) {
                        this.routerState = t,
                        this.currentUrlTree = e,
                        this.rawUrlTree = this.urlHandlingStrategy.merge(this.currentUrlTree, n),
                        this.resetUrlToCurrentUrlTree()
                    }
                }, {
                    key: "resetUrlToCurrentUrlTree",
                    value: function() {
                        this.location.replaceState(this.urlSerializer.serialize(this.rawUrlTree), "", {
                            navigationId: this.lastSuccessfulId
                        })
                    }
                }, {
                    key: "url",
                    get: function() {
                        return this.serializeUrl(this.currentUrlTree)
                    }
                }]),
                t
            }();
            return t.\u0275fac = function(e) {
                return new (e || t)(he(Ki),he(lc),he(Zh),he(Bu),he(ho),he(yu),he(Za),he(void 0))
            }
            ,
            t.\u0275prov = Tt({
                token: t,
                factory: t.\u0275fac
            }),
            t
        }();
        function Yh(t) {
            for (var e = 0; e < t.length; e++) {
                var n = t[e];
                if (null == n)
                    throw new Error("The requested path contains ".concat(n, " segment at index ").concat(e))
            }
        }
        var Xh = function() {
            var t = function() {
                function t(e, n, r, i, o) {
                    m(this, t),
                    this.parentContexts = e,
                    this.location = n,
                    this.resolver = r,
                    this.changeDetector = o,
                    this.activated = null,
                    this._activatedRoute = null,
                    this.activateEvents = new Ea,
                    this.deactivateEvents = new Ea,
                    this.name = i || Wl,
                    e.onChildOutletCreated(this.name, this)
                }
                return _(t, [{
                    key: "ngOnDestroy",
                    value: function() {
                        this.parentContexts.onChildOutletDestroyed(this.name)
                    }
                }, {
                    key: "ngOnInit",
                    value: function() {
                        if (!this.activated) {
                            var t = this.parentContexts.getContext(this.name);
                            t && t.route && (t.attachRef ? this.attach(t.attachRef, t.route) : this.activateWith(t.route, t.resolver || null))
                        }
                    }
                }, {
                    key: "detach",
                    value: function() {
                        if (!this.activated)
                            throw new Error("Outlet is not activated");
                        this.location.detach();
                        var t = this.activated;
                        return this.activated = null,
                        this._activatedRoute = null,
                        t
                    }
                }, {
                    key: "attach",
                    value: function(t, e) {
                        this.activated = t,
                        this._activatedRoute = e,
                        this.location.insert(t.hostView)
                    }
                }, {
                    key: "deactivate",
                    value: function() {
                        if (this.activated) {
                            var t = this.component;
                            this.activated.destroy(),
                            this.activated = null,
                            this._activatedRoute = null,
                            this.deactivateEvents.emit(t)
                        }
                    }
                }, {
                    key: "activateWith",
                    value: function(t, e) {
                        if (this.isActivated)
                            throw new Error("Cannot activate an already activated outlet");
                        this._activatedRoute = t;
                        var n = (e = e || this.resolver).resolveComponentFactory(t._futureSnapshot.routeConfig.component)
                          , r = this.parentContexts.getOrCreateContext(this.name).children
                          , i = new $h(t,r,this.location.injector);
                        this.activated = this.location.createComponent(n, this.location.length, i),
                        this.changeDetector.markForCheck(),
                        this.activateEvents.emit(this.activated.instance)
                    }
                }, {
                    key: "isActivated",
                    get: function() {
                        return !!this.activated
                    }
                }, {
                    key: "component",
                    get: function() {
                        if (!this.activated)
                            throw new Error("Outlet is not activated");
                        return this.activated.instance
                    }
                }, {
                    key: "activatedRoute",
                    get: function() {
                        if (!this.activated)
                            throw new Error("Outlet is not activated");
                        return this._activatedRoute
                    }
                }, {
                    key: "activatedRouteData",
                    get: function() {
                        return this._activatedRoute ? this._activatedRoute.snapshot.data : {}
                    }
                }]),
                t
            }();
            return t.\u0275fac = function(e) {
                return new (e || t)(ko(Zh),ko(sa),ko(Ho),("name",
                function(t, e) {
                    var n = t.attrs;
                    if (n)
                        for (var r = n.length, i = 0; i < r; ) {
                            var o = n[i];
                            if (qn(o))
                                break;
                            if (0 === o)
                                i += 2;
                            else if ("number" == typeof o)
                                for (i++; i < r && "string" == typeof n[i]; )
                                    i++;
                            else {
                                if (o === e)
                                    return n[i + 1];
                                i += 2
                            }
                        }
                    return null
                }(dn(), "name")),ko(Qi))
            }
            ,
            t.\u0275dir = Ie({
                type: t,
                selectors: [["router-outlet"]],
                outputs: {
                    activateEvents: "activate",
                    deactivateEvents: "deactivate"
                },
                exportAs: ["outlet"]
            }),
            t
        }()
          , $h = function() {
            function t(e, n, r) {
                m(this, t),
                this.route = e,
                this.childContexts = n,
                this.parent = r
            }
            return _(t, [{
                key: "get",
                value: function(t, e) {
                    return t === Pc ? this.route : t === Zh ? this.childContexts : this.parent.get(t, e)
                }
            }]),
            t
        }()
          , tf = function t() {
            m(this, t)
        }
          , ef = function() {
            function t() {
                m(this, t)
            }
            return _(t, [{
                key: "preload",
                value: function(t, e) {
                    return e().pipe(ml((function() {
                        return Ms(null)
                    }
                    )))
                }
            }]),
            t
        }()
          , nf = function() {
            function t() {
                m(this, t)
            }
            return _(t, [{
                key: "preload",
                value: function(t, e) {
                    return Ms(null)
                }
            }]),
            t
        }()
          , rf = function() {
            var t = function() {
                function t(e, n, r, i, o) {
                    m(this, t),
                    this.router = e,
                    this.injector = i,
                    this.preloadingStrategy = o,
                    this.loader = new qh(n,r,(function(t) {
                        return e.triggerEvent(new Ll(t))
                    }
                    ),(function(t) {
                        return e.triggerEvent(new Fl(t))
                    }
                    ))
                }
                return _(t, [{
                    key: "setUpPreloading",
                    value: function() {
                        var t = this;
                        this.subscription = this.router.events.pipe(Js((function(t) {
                            return t instanceof Pl
                        }
                        )), kl((function() {
                            return t.preload()
                        }
                        ))).subscribe((function() {}
                        ))
                    }
                }, {
                    key: "preload",
                    value: function() {
                        var t = this.injector.get(ge);
                        return this.processRoutes(t, this.router.config)
                    }
                }, {
                    key: "ngOnDestroy",
                    value: function() {
                        this.subscription && this.subscription.unsubscribe()
                    }
                }, {
                    key: "processRoutes",
                    value: function(t, e) {
                        var n, r = [], i = h(e);
                        try {
                            for (i.s(); !(n = i.n()).done; ) {
                                var o = n.value;
                                if (o.loadChildren && !o.canLoad && o._loadedConfig) {
                                    var a = o._loadedConfig;
                                    r.push(this.processRoutes(a.module, a.routes))
                                } else
                                    o.loadChildren && !o.canLoad ? r.push(this.preloadConfig(t, o)) : o.children && r.push(this.processRoutes(t, o.children))
                            }
                        } catch (u) {
                            i.e(u)
                        } finally {
                            i.f()
                        }
                        return nt(r).pipe(lt(), W((function(t) {}
                        )))
                    }
                }, {
                    key: "preloadConfig",
                    value: function(t, e) {
                        var n = this;
                        return this.preloadingStrategy.preload(e, (function() {
                            return n.loader.load(t.injector, e).pipe(at((function(t) {
                                return e._loadedConfig = t,
                                n.processRoutes(t.module, t.routes)
                            }
                            )))
                        }
                        ))
                    }
                }]),
                t
            }();
            return t.\u0275fac = function(e) {
                return new (e || t)(he(Kh),he(yu),he(Za),he(ho),he(tf))
            }
            ,
            t.\u0275prov = Tt({
                token: t,
                factory: t.\u0275fac
            }),
            t
        }()
          , of = function() {
            var t = function() {
                function t(e, n) {
                    var r = arguments.length > 2 && void 0 !== arguments[2] ? arguments[2] : {};
                    m(this, t),
                    this.router = e,
                    this.viewportScroller = n,
                    this.options = r,
                    this.lastId = 0,
                    this.lastSource = "imperative",
                    this.restoredId = 0,
                    this.store = {},
                    r.scrollPositionRestoration = r.scrollPositionRestoration || "disabled",
                    r.anchorScrolling = r.anchorScrolling || "disabled"
                }
                return _(t, [{
                    key: "init",
                    value: function() {
                        "disabled" !== this.options.scrollPositionRestoration && this.viewportScroller.setHistoryScrollRestoration("manual"),
                        this.routerEventsSubscription = this.createScrollEvents(),
                        this.scrollEventsSubscription = this.consumeScrollEvents()
                    }
                }, {
                    key: "createScrollEvents",
                    value: function() {
                        var t = this;
                        return this.router.events.subscribe((function(e) {
                            e instanceof Rl ? (t.store[t.lastId] = t.viewportScroller.getScrollPosition(),
                            t.lastSource = e.navigationTrigger,
                            t.restoredId = e.restoredState ? e.restoredState.navigationId : 0) : e instanceof Pl && (t.lastId = e.id,
                            t.scheduleScrollEvent(e, t.router.parseUrl(e.urlAfterRedirects).fragment))
                        }
                        ))
                    }
                }, {
                    key: "consumeScrollEvents",
                    value: function() {
                        var t = this;
                        return this.router.events.subscribe((function(e) {
                            e instanceof Zl && (e.position ? "top" === t.options.scrollPositionRestoration ? t.viewportScroller.scrollToPosition([0, 0]) : "enabled" === t.options.scrollPositionRestoration && t.viewportScroller.scrollToPosition(e.position) : e.anchor && "enabled" === t.options.anchorScrolling ? t.viewportScroller.scrollToAnchor(e.anchor) : "disabled" !== t.options.scrollPositionRestoration && t.viewportScroller.scrollToPosition([0, 0]))
                        }
                        ))
                    }
                }, {
                    key: "scheduleScrollEvent",
                    value: function(t, e) {
                        this.router.triggerEvent(new Zl(t,"popstate" === this.lastSource ? this.store[this.restoredId] : null,e))
                    }
                }, {
                    key: "ngOnDestroy",
                    value: function() {
                        this.routerEventsSubscription && this.routerEventsSubscription.unsubscribe(),
                        this.scrollEventsSubscription && this.scrollEventsSubscription.unsubscribe()
                    }
                }]),
                t
            }();
            return t.\u0275fac = function(e) {
                return new (e || t)(he(Kh),he(es),he(void 0))
            }
            ,
            t.\u0275prov = Tt({
                token: t,
                factory: t.\u0275fac
            }),
            t
        }()
          , af = new ee("ROUTER_CONFIGURATION")
          , uf = new ee("ROUTER_FORROOT_GUARD")
          , sf = [Bu, {
            provide: lc,
            useClass: cc
        }, {
            provide: Kh,
            useFactory: function(t, e, n, r, i, o, a) {
                var u = arguments.length > 7 && void 0 !== arguments[7] ? arguments[7] : {}
                  , s = arguments.length > 8 ? arguments[8] : void 0
                  , l = arguments.length > 9 ? arguments[9] : void 0
                  , c = new Kh(null,t,e,n,r,i,o,$l(a));
                if (s && (c.urlHandlingStrategy = s),
                l && (c.routeReuseStrategy = l),
                u.errorHandler && (c.errorHandler = u.errorHandler),
                u.malformedUriErrorHandler && (c.malformedUriErrorHandler = u.malformedUriErrorHandler),
                u.enableTracing) {
                    var h = Tu();
                    c.events.subscribe((function(t) {
                        h.logGroup("Router Event: ".concat(t.constructor.name)),
                        h.log(t.toString()),
                        h.log(t),
                        h.logGroupEnd()
                    }
                    ))
                }
                return u.onSameUrlNavigation && (c.onSameUrlNavigation = u.onSameUrlNavigation),
                u.paramsInheritanceStrategy && (c.paramsInheritanceStrategy = u.paramsInheritanceStrategy),
                u.urlUpdateStrategy && (c.urlUpdateStrategy = u.urlUpdateStrategy),
                u.relativeLinkResolution && (c.relativeLinkResolution = u.relativeLinkResolution),
                c
            },
            deps: [lc, Zh, Bu, ho, yu, Za, zh, af, [function t() {
                m(this, t)
            }
            , new bt], [function t() {
                m(this, t)
            }
            , new bt]]
        }, Zh, {
            provide: Pc,
            useFactory: function(t) {
                return t.routerState.root
            },
            deps: [Kh]
        }, {
            provide: yu,
            useClass: _u
        }, rf, nf, ef, {
            provide: af,
            useValue: {
                enableTracing: !1
            }
        }];
        function lf() {
            return new su("Router",Kh)
        }
        var cf = function() {
            var t = function() {
                function t(e, n) {
                    m(this, t)
                }
                return _(t, null, [{
                    key: "forRoot",
                    value: function(e, n) {
                        return {
                            ngModule: t,
                            providers: [sf, vf(e), {
                                provide: uf,
                                useFactory: df,
                                deps: [[Kh, new bt, new St]]
                            }, {
                                provide: af,
                                useValue: n || {}
                            }, {
                                provide: Lu,
                                useFactory: ff,
                                deps: [Ru, [new kt(Vu), new bt], af]
                            }, {
                                provide: of,
                                useFactory: hf,
                                deps: [Kh, es, af]
                            }, {
                                provide: tf,
                                useExisting: n && n.preloadingStrategy ? n.preloadingStrategy : nf
                            }, {
                                provide: su,
                                multi: !0,
                                useFactory: lf
                            }, [pf, {
                                provide: Ta,
                                multi: !0,
                                useFactory: gf,
                                deps: [pf]
                            }, {
                                provide: mf,
                                useFactory: yf,
                                deps: [pf]
                            }, {
                                provide: Na,
                                multi: !0,
                                useExisting: mf
                            }]]
                        }
                    }
                }, {
                    key: "forChild",
                    value: function(e) {
                        return {
                            ngModule: t,
                            providers: [vf(e)]
                        }
                    }
                }]),
                t
            }();
            return t.\u0275mod = Re({
                type: t
            }),
            t.\u0275inj = Ot({
                factory: function(e) {
                    return new (e || t)(he(uf, 8),he(Kh, 8))
                }
            }),
            t
        }();
        function hf(t, e, n) {
            return n.scrollOffset && e.setOffset(n.scrollOffset),
            new of(t,e,n)
        }
        function ff(t, e) {
            var n = arguments.length > 2 && void 0 !== arguments[2] ? arguments[2] : {};
            return n.useHash ? new qu(t,e) : new zu(t,e)
        }
        function df(t) {
            if (t)
                throw new Error("RouterModule.forRoot() called twice. Lazy loaded modules should use RouterModule.forChild() instead.");
            return "guarded"
        }
        function vf(t) {
            return [{
                provide: fo,
                multi: !0,
                useValue: t
            }, {
                provide: zh,
                multi: !0,
                useValue: t
            }]
        }
        var pf = function() {
            var t = function() {
                function t(e) {
                    m(this, t),
                    this.injector = e,
                    this.initNavigation = !1,
                    this.resultOfPreactivationDone = new q
                }
                return _(t, [{
                    key: "appInitializer",
                    value: function() {
                        var t = this;
                        return this.injector.get(Iu, Promise.resolve(null)).then((function() {
                            var e = null
                              , n = new Promise((function(t) {
                                return e = t
                            }
                            ))
                              , r = t.injector.get(Kh)
                              , i = t.injector.get(af);
                            if (t.isLegacyDisabled(i) || t.isLegacyEnabled(i))
                                e(!0);
                            else if ("disabled" === i.initialNavigation)
                                r.setUpLocationChangeListener(),
                                e(!0);
                            else {
                                if ("enabled" !== i.initialNavigation)
                                    throw new Error("Invalid initialNavigation options: '".concat(i.initialNavigation, "'"));
                                r.hooks.afterPreactivation = function() {
                                    return t.initNavigation ? Ms(null) : (t.initNavigation = !0,
                                    e(!0),
                                    t.resultOfPreactivationDone)
                                }
                                ,
                                r.initialNavigation()
                            }
                            return n
                        }
                        ))
                    }
                }, {
                    key: "bootstrapListener",
                    value: function(t) {
                        var e = this.injector.get(af)
                          , n = this.injector.get(rf)
                          , r = this.injector.get(of)
                          , i = this.injector.get(Kh)
                          , o = this.injector.get(pu);
                        t === o.components[0] && (this.isLegacyEnabled(e) ? i.initialNavigation() : this.isLegacyDisabled(e) && i.setUpLocationChangeListener(),
                        n.setUpPreloading(),
                        r.init(),
                        i.resetRootComponentType(o.componentTypes[0]),
                        this.resultOfPreactivationDone.next(null),
                        this.resultOfPreactivationDone.complete())
                    }
                }, {
                    key: "isLegacyEnabled",
                    value: function(t) {
                        return "legacy_enabled" === t.initialNavigation || !0 === t.initialNavigation || void 0 === t.initialNavigation
                    }
                }, {
                    key: "isLegacyDisabled",
                    value: function(t) {
                        return "legacy_disabled" === t.initialNavigation || !1 === t.initialNavigation
                    }
                }]),
                t
            }();
            return t.\u0275fac = function(e) {
                return new (e || t)(he(ho))
            }
            ,
            t.\u0275prov = Tt({
                token: t,
                factory: t.\u0275fac
            }),
            t
        }();
        function gf(t) {
            return t.appInitializer.bind(t)
        }
        function yf(t) {
            return t.bootstrapListener.bind(t)
        }
        var mf = new ee("Router Initializer")
          , wf = []
          , _f = function() {
            function t() {}
            return t.\u0275mod = Re({
                type: t
            }),
            t.\u0275inj = Ot({
                factory: function(e) {
                    return new (e || t)
                },
                imports: [[cf.forRoot(wf)], cf]
            }),
            t
        }()
          , kf = function t() {
            m(this, t)
        }
          , bf = function t() {
            m(this, t)
        }
          , Cf = function() {
            function t(e) {
                var n = this;
                m(this, t),
                this.normalizedNames = new Map,
                this.lazyUpdate = null,
                e ? this.lazyInit = "string" == typeof e ? function() {
                    n.headers = new Map,
                    e.split("\n").forEach((function(t) {
                        var e = t.indexOf(":");
                        if (e > 0) {
                            var r = t.slice(0, e)
                              , i = r.toLowerCase()
                              , o = t.slice(e + 1).trim();
                            n.maybeSetNormalizedName(r, i),
                            n.headers.has(i) ? n.headers.get(i).push(o) : n.headers.set(i, [o])
                        }
                    }
                    ))
                }
                : function() {
                    n.headers = new Map,
                    Object.keys(e).forEach((function(t) {
                        var r = e[t]
                          , i = t.toLowerCase();
                        "string" == typeof r && (r = [r]),
                        r.length > 0 && (n.headers.set(i, r),
                        n.maybeSetNormalizedName(t, i))
                    }
                    ))
                }
                : this.headers = new Map
            }
            return _(t, [{
                key: "has",
                value: function(t) {
                    return this.init(),
                    this.headers.has(t.toLowerCase())
                }
            }, {
                key: "get",
                value: function(t) {
                    this.init();
                    var e = this.headers.get(t.toLowerCase());
                    return e && e.length > 0 ? e[0] : null
                }
            }, {
                key: "keys",
                value: function() {
                    return this.init(),
                    Array.from(this.normalizedNames.values())
                }
            }, {
                key: "getAll",
                value: function(t) {
                    return this.init(),
                    this.headers.get(t.toLowerCase()) || null
                }
            }, {
                key: "append",
                value: function(t, e) {
                    return this.clone({
                        name: t,
                        value: e,
                        op: "a"
                    })
                }
            }, {
                key: "set",
                value: function(t, e) {
                    return this.clone({
                        name: t,
                        value: e,
                        op: "s"
                    })
                }
            }, {
                key: "delete",
                value: function(t, e) {
                    return this.clone({
                        name: t,
                        value: e,
                        op: "d"
                    })
                }
            }, {
                key: "maybeSetNormalizedName",
                value: function(t, e) {
                    this.normalizedNames.has(e) || this.normalizedNames.set(e, t)
                }
            }, {
                key: "init",
                value: function() {
                    var e = this;
                    this.lazyInit && (this.lazyInit instanceof t ? this.copyFrom(this.lazyInit) : this.lazyInit(),
                    this.lazyInit = null,
                    this.lazyUpdate && (this.lazyUpdate.forEach((function(t) {
                        return e.applyUpdate(t)
                    }
                    )),
                    this.lazyUpdate = null))
                }
            }, {
                key: "copyFrom",
                value: function(t) {
                    var e = this;
                    t.init(),
                    Array.from(t.headers.keys()).forEach((function(n) {
                        e.headers.set(n, t.headers.get(n)),
                        e.normalizedNames.set(n, t.normalizedNames.get(n))
                    }
                    ))
                }
            }, {
                key: "clone",
                value: function(e) {
                    var n = new t;
                    return n.lazyInit = this.lazyInit && this.lazyInit instanceof t ? this.lazyInit : this,
                    n.lazyUpdate = (this.lazyUpdate || []).concat([e]),
                    n
                }
            }, {
                key: "applyUpdate",
                value: function(t) {
                    var e = t.name.toLowerCase();
                    switch (t.op) {
                    case "a":
                    case "s":
                        var n = t.value;
                        if ("string" == typeof n && (n = [n]),
                        0 === n.length)
                            return;
                        this.maybeSetNormalizedName(t.name, e);
                        var r = ("a" === t.op ? this.headers.get(e) : void 0) || [];
                        r.push.apply(r, l(n)),
                        this.headers.set(e, r);
                        break;
                    case "d":
                        var i = t.value;
                        if (i) {
                            var o = this.headers.get(e);
                            if (!o)
                                return;
                            0 === (o = o.filter((function(t) {
                                return -1 === i.indexOf(t)
                            }
                            ))).length ? (this.headers.delete(e),
                            this.normalizedNames.delete(e)) : this.headers.set(e, o)
                        } else
                            this.headers.delete(e),
                            this.normalizedNames.delete(e)
                    }
                }
            }, {
                key: "forEach",
                value: function(t) {
                    var e = this;
                    this.init(),
                    Array.from(this.normalizedNames.keys()).forEach((function(n) {
                        return t(e.normalizedNames.get(n), e.headers.get(n))
                    }
                    ))
                }
            }]),
            t
        }()
          , Sf = function() {
            function t() {
                m(this, t)
            }
            return _(t, [{
                key: "encodeKey",
                value: function(t) {
                    return Ef(t)
                }
            }, {
                key: "encodeValue",
                value: function(t) {
                    return Ef(t)
                }
            }, {
                key: "decodeKey",
                value: function(t) {
                    return decodeURIComponent(t)
                }
            }, {
                key: "decodeValue",
                value: function(t) {
                    return decodeURIComponent(t)
                }
            }]),
            t
        }();
        function xf(t, e) {
            var n = new Map;
            return t.length > 0 && t.split("&").forEach((function(t) {
                var r = t.indexOf("=")
                  , i = s(-1 == r ? [e.decodeKey(t), ""] : [e.decodeKey(t.slice(0, r)), e.decodeValue(t.slice(r + 1))], 2)
                  , o = i[0]
                  , a = i[1]
                  , u = n.get(o) || [];
                u.push(a),
                n.set(o, u)
            }
            )),
            n
        }
        function Ef(t) {
            return encodeURIComponent(t).replace(/%40/gi, "@").replace(/%3A/gi, ":").replace(/%24/gi, "$").replace(/%2C/gi, ",").replace(/%3B/gi, ";").replace(/%2B/gi, "+").replace(/%3D/gi, "=").replace(/%3F/gi, "?").replace(/%2F/gi, "/")
        }
        var Tf = function() {
            function t() {
                var e = this
                  , n = arguments.length > 0 && void 0 !== arguments[0] ? arguments[0] : {};
                if (m(this, t),
                this.updates = null,
                this.cloneFrom = null,
                this.encoder = n.encoder || new Sf,
                n.fromString) {
                    if (n.fromObject)
                        throw new Error("Cannot specify both fromString and fromObject.");
                    this.map = xf(n.fromString, this.encoder)
                } else
                    n.fromObject ? (this.map = new Map,
                    Object.keys(n.fromObject).forEach((function(t) {
                        var r = n.fromObject[t];
                        e.map.set(t, Array.isArray(r) ? r : [r])
                    }
                    ))) : this.map = null
            }
            return _(t, [{
                key: "has",
                value: function(t) {
                    return this.init(),
                    this.map.has(t)
                }
            }, {
                key: "get",
                value: function(t) {
                    this.init();
                    var e = this.map.get(t);
                    return e ? e[0] : null
                }
            }, {
                key: "getAll",
                value: function(t) {
                    return this.init(),
                    this.map.get(t) || null
                }
            }, {
                key: "keys",
                value: function() {
                    return this.init(),
                    Array.from(this.map.keys())
                }
            }, {
                key: "append",
                value: function(t, e) {
                    return this.clone({
                        param: t,
                        value: e,
                        op: "a"
                    })
                }
            }, {
                key: "set",
                value: function(t, e) {
                    return this.clone({
                        param: t,
                        value: e,
                        op: "s"
                    })
                }
            }, {
                key: "delete",
                value: function(t, e) {
                    return this.clone({
                        param: t,
                        value: e,
                        op: "d"
                    })
                }
            }, {
                key: "toString",
                value: function() {
                    var t = this;
                    return this.init(),
                    this.keys().map((function(e) {
                        var n = t.encoder.encodeKey(e);
                        return t.map.get(e).map((function(e) {
                            return n + "=" + t.encoder.encodeValue(e)
                        }
                        )).join("&")
                    }
                    )).filter((function(t) {
                        return "" !== t
                    }
                    )).join("&")
                }
            }, {
                key: "clone",
                value: function(e) {
                    var n = new t({
                        encoder: this.encoder
                    });
                    return n.cloneFrom = this.cloneFrom || this,
                    n.updates = (this.updates || []).concat([e]),
                    n
                }
            }, {
                key: "init",
                value: function() {
                    var t = this;
                    null === this.map && (this.map = new Map),
                    null !== this.cloneFrom && (this.cloneFrom.init(),
                    this.cloneFrom.keys().forEach((function(e) {
                        return t.map.set(e, t.cloneFrom.map.get(e))
                    }
                    )),
                    this.updates.forEach((function(e) {
                        switch (e.op) {
                        case "a":
                        case "s":
                            var n = ("a" === e.op ? t.map.get(e.param) : void 0) || [];
                            n.push(e.value),
                            t.map.set(e.param, n);
                            break;
                        case "d":
                            if (void 0 === e.value) {
                                t.map.delete(e.param);
                                break
                            }
                            var r = t.map.get(e.param) || []
                              , i = r.indexOf(e.value);
                            -1 !== i && r.splice(i, 1),
                            r.length > 0 ? t.map.set(e.param, r) : t.map.delete(e.param)
                        }
                    }
                    )),
                    this.cloneFrom = this.updates = null)
                }
            }]),
            t
        }();
        function Of(t) {
            return "undefined" != typeof ArrayBuffer && t instanceof ArrayBuffer
        }
        function Af(t) {
            return "undefined" != typeof Blob && t instanceof Blob
        }
        function Rf(t) {
            return "undefined" != typeof FormData && t instanceof FormData
        }
        var Pf = function() {
            function t(e, n, r, i) {
                var o;
                if (m(this, t),
                this.url = n,
                this.body = null,
                this.reportProgress = !1,
                this.withCredentials = !1,
                this.responseType = "json",
                this.method = e.toUpperCase(),
                function(t) {
                    switch (t) {
                    case "DELETE":
                    case "GET":
                    case "HEAD":
                    case "OPTIONS":
                    case "JSONP":
                        return !1;
                    default:
                        return !0
                    }
                }(this.method) || i ? (this.body = void 0 !== r ? r : null,
                o = i) : o = r,
                o && (this.reportProgress = !!o.reportProgress,
                this.withCredentials = !!o.withCredentials,
                o.responseType && (this.responseType = o.responseType),
                o.headers && (this.headers = o.headers),
                o.params && (this.params = o.params)),
                this.headers || (this.headers = new Cf),
                this.params) {
                    var a = this.params.toString();
                    if (0 === a.length)
                        this.urlWithParams = n;
                    else {
                        var u = n.indexOf("?");
                        this.urlWithParams = n + (-1 === u ? "?" : u < n.length - 1 ? "&" : "") + a
                    }
                } else
                    this.params = new Tf,
                    this.urlWithParams = n
            }
            return _(t, [{
                key: "serializeBody",
                value: function() {
                    return null === this.body ? null : Of(this.body) || Af(this.body) || Rf(this.body) || "string" == typeof this.body ? this.body : this.body instanceof Tf ? this.body.toString() : "object" == typeof this.body || "boolean" == typeof this.body || Array.isArray(this.body) ? JSON.stringify(this.body) : this.body.toString()
                }
            }, {
                key: "detectContentTypeHeader",
                value: function() {
                    return null === this.body || Rf(this.body) ? null : Af(this.body) ? this.body.type || null : Of(this.body) ? null : "string" == typeof this.body ? "text/plain" : this.body instanceof Tf ? "application/x-www-form-urlencoded;charset=UTF-8" : "object" == typeof this.body || "number" == typeof this.body || Array.isArray(this.body) ? "application/json" : null
                }
            }, {
                key: "clone",
                value: function() {
                    var e = arguments.length > 0 && void 0 !== arguments[0] ? arguments[0] : {}
                      , n = e.method || this.method
                      , r = e.url || this.url
                      , i = e.responseType || this.responseType
                      , o = void 0 !== e.body ? e.body : this.body
                      , a = void 0 !== e.withCredentials ? e.withCredentials : this.withCredentials
                      , u = void 0 !== e.reportProgress ? e.reportProgress : this.reportProgress
                      , s = e.headers || this.headers
                      , l = e.params || this.params;
                    return void 0 !== e.setHeaders && (s = Object.keys(e.setHeaders).reduce((function(t, n) {
                        return t.set(n, e.setHeaders[n])
                    }
                    ), s)),
                    e.setParams && (l = Object.keys(e.setParams).reduce((function(t, n) {
                        return t.set(n, e.setParams[n])
                    }
                    ), l)),
                    new t(n,r,o,{
                        params: l,
                        headers: s,
                        reportProgress: u,
                        responseType: i,
                        withCredentials: a
                    })
                }
            }]),
            t
        }()
          , If = function(t) {
            return t[t.Sent = 0] = "Sent",
            t[t.UploadProgress = 1] = "UploadProgress",
            t[t.ResponseHeader = 2] = "ResponseHeader",
            t[t.DownloadProgress = 3] = "DownloadProgress",
            t[t.Response = 4] = "Response",
            t[t.User = 5] = "User",
            t
        }({})
          , jf = function t(e) {
            var n = arguments.length > 1 && void 0 !== arguments[1] ? arguments[1] : 200
              , r = arguments.length > 2 && void 0 !== arguments[2] ? arguments[2] : "OK";
            m(this, t),
            this.headers = e.headers || new Cf,
            this.status = void 0 !== e.status ? e.status : n,
            this.statusText = e.statusText || r,
            this.url = e.url || null,
            this.ok = this.status >= 200 && this.status < 300
        }
          , Nf = function(t) {
            d(n, t);
            var e = y(n);
            function n() {
                var t, r = arguments.length > 0 && void 0 !== arguments[0] ? arguments[0] : {};
                return m(this, n),
                (t = e.call(this, r)).type = If.ResponseHeader,
                t
            }
            return _(n, [{
                key: "clone",
                value: function() {
                    var t = arguments.length > 0 && void 0 !== arguments[0] ? arguments[0] : {};
                    return new n({
                        headers: t.headers || this.headers,
                        status: void 0 !== t.status ? t.status : this.status,
                        statusText: t.statusText || this.statusText,
                        url: t.url || this.url || void 0
                    })
                }
            }]),
            n
        }(jf)
          , Mf = function(t) {
            d(n, t);
            var e = y(n);
            function n() {
                var t, r = arguments.length > 0 && void 0 !== arguments[0] ? arguments[0] : {};
                return m(this, n),
                (t = e.call(this, r)).type = If.Response,
                t.body = void 0 !== r.body ? r.body : null,
                t
            }
            return _(n, [{
                key: "clone",
                value: function() {
                    var t = arguments.length > 0 && void 0 !== arguments[0] ? arguments[0] : {};
                    return new n({
                        body: void 0 !== t.body ? t.body : this.body,
                        headers: t.headers || this.headers,
                        status: void 0 !== t.status ? t.status : this.status,
                        statusText: t.statusText || this.statusText,
                        url: t.url || this.url || void 0
                    })
                }
            }]),
            n
        }(jf)
          , Uf = function(t) {
            d(n, t);
            var e = y(n);
            function n(t) {
                var r;
                return m(this, n),
                (r = e.call(this, t, 0, "Unknown Error")).name = "HttpErrorResponse",
                r.ok = !1,
                r.message = r.status >= 200 && r.status < 300 ? "Http failure during parsing for ".concat(t.url || "(unknown url)") : "Http failure response for ".concat(t.url || "(unknown url)", ": ").concat(t.status, " ").concat(t.statusText),
                r.error = t.error || null,
                r
            }
            return n
        }(jf);
        function Df(t, e) {
            return {
                body: e,
                headers: t.headers,
                observe: t.observe,
                params: t.params,
                reportProgress: t.reportProgress,
                responseType: t.responseType,
                withCredentials: t.withCredentials
            }
        }
        var Hf = function() {
            var t = function() {
                function t(e) {
                    m(this, t),
                    this.handler = e
                }
                return _(t, [{
                    key: "request",
                    value: function(t, e) {
                        var n, r = this, i = arguments.length > 2 && void 0 !== arguments[2] ? arguments[2] : {};
                        if (t instanceof Pf)
                            n = t;
                        else {
                            var o = void 0;
                            o = i.headers instanceof Cf ? i.headers : new Cf(i.headers);
                            var a = void 0;
                            i.params && (a = i.params instanceof Tf ? i.params : new Tf({
                                fromObject: i.params
                            })),
                            n = new Pf(t,e,void 0 !== i.body ? i.body : null,{
                                headers: o,
                                params: a,
                                reportProgress: i.reportProgress,
                                responseType: i.responseType || "json",
                                withCredentials: i.withCredentials
                            })
                        }
                        var u = Ms(n).pipe(kl((function(t) {
                            return r.handler.handle(t)
                        }
                        )));
                        if (t instanceof Pf || "events" === i.observe)
                            return u;
                        var s = u.pipe(Js((function(t) {
                            return t instanceof Mf
                        }
                        )));
                        switch (i.observe || "body") {
                        case "body":
                            switch (n.responseType) {
                            case "arraybuffer":
                                return s.pipe(W((function(t) {
                                    if (null !== t.body && !(t.body instanceof ArrayBuffer))
                                        throw new Error("Response is not an ArrayBuffer.");
                                    return t.body
                                }
                                )));
                            case "blob":
                                return s.pipe(W((function(t) {
                                    if (null !== t.body && !(t.body instanceof Blob))
                                        throw new Error("Response is not a Blob.");
                                    return t.body
                                }
                                )));
                            case "text":
                                return s.pipe(W((function(t) {
                                    if (null !== t.body && "string" != typeof t.body)
                                        throw new Error("Response is not a string.");
                                    return t.body
                                }
                                )));
                            case "json":
                            default:
                                return s.pipe(W((function(t) {
                                    return t.body
                                }
                                )))
                            }
                        case "response":
                            return s;
                        default:
                            throw new Error("Unreachable: unhandled observe type ".concat(i.observe, "}"))
                        }
                    }
                }, {
                    key: "delete",
                    value: function(t) {
                        var e = arguments.length > 1 && void 0 !== arguments[1] ? arguments[1] : {};
                        return this.request("DELETE", t, e)
                    }
                }, {
                    key: "get",
                    value: function(t) {
                        var e = arguments.length > 1 && void 0 !== arguments[1] ? arguments[1] : {};
                        return this.request("GET", t, e)
                    }
                }, {
                    key: "head",
                    value: function(t) {
                        var e = arguments.length > 1 && void 0 !== arguments[1] ? arguments[1] : {};
                        return this.request("HEAD", t, e)
                    }
                }, {
                    key: "jsonp",
                    value: function(t, e) {
                        return this.request("JSONP", t, {
                            params: (new Tf).append(e, "JSONP_CALLBACK"),
                            observe: "body",
                            responseType: "json"
                        })
                    }
                }, {
                    key: "options",
                    value: function(t) {
                        var e = arguments.length > 1 && void 0 !== arguments[1] ? arguments[1] : {};
                        return this.request("OPTIONS", t, e)
                    }
                }, {
                    key: "patch",
                    value: function(t, e) {
                        var n = arguments.length > 2 && void 0 !== arguments[2] ? arguments[2] : {};
                        return this.request("PATCH", t, Df(n, e))
                    }
                }, {
                    key: "post",
                    value: function(t, e) {
                        var n = arguments.length > 2 && void 0 !== arguments[2] ? arguments[2] : {};
                        return this.request("POST", t, Df(n, e))
                    }
                }, {
                    key: "put",
                    value: function(t, e) {
                        var n = arguments.length > 2 && void 0 !== arguments[2] ? arguments[2] : {};
                        return this.request("PUT", t, Df(n, e))
                    }
                }]),
                t
            }();
            return t.\u0275fac = function(e) {
                return new (e || t)(he(kf))
            }
            ,
            t.\u0275prov = Tt({
                token: t,
                factory: t.\u0275fac
            }),
            t
        }()
          , Lf = function() {
            function t(e, n) {
                m(this, t),
                this.next = e,
                this.interceptor = n
            }
            return _(t, [{
                key: "handle",
                value: function(t) {
                    return this.interceptor.intercept(t, this.next)
                }
            }]),
            t
        }()
          , Ff = new ee("HTTP_INTERCEPTORS")
          , Vf = function() {
            var t = function() {
                function t() {
                    m(this, t)
                }
                return _(t, [{
                    key: "intercept",
                    value: function(t, e) {
                        return e.handle(t)
                    }
                }]),
                t
            }();
            return t.\u0275fac = function(e) {
                return new (e || t)
            }
            ,
            t.\u0275prov = Tt({
                token: t,
                factory: t.\u0275fac
            }),
            t
        }()
          , zf = /^\)\]\}',?\n/
          , qf = function t() {
            m(this, t)
        }
          , Bf = function() {
            var t = function() {
                function t() {
                    m(this, t)
                }
                return _(t, [{
                    key: "build",
                    value: function() {
                        return new XMLHttpRequest
                    }
                }]),
                t
            }();
            return t.\u0275fac = function(e) {
                return new (e || t)
            }
            ,
            t.\u0275prov = Tt({
                token: t,
                factory: t.\u0275fac
            }),
            t
        }()
          , Zf = function() {
            var t = function() {
                function t(e) {
                    m(this, t),
                    this.xhrFactory = e
                }
                return _(t, [{
                    key: "handle",
                    value: function(t) {
                        var e = this;
                        if ("JSONP" === t.method)
                            throw new Error("Attempted to construct Jsonp request without HttpClientJsonpModule installed.");
                        return new H((function(n) {
                            var r = e.xhrFactory.build();
                            if (r.open(t.method, t.urlWithParams),
                            t.withCredentials && (r.withCredentials = !0),
                            t.headers.forEach((function(t, e) {
                                return r.setRequestHeader(t, e.join(","))
                            }
                            )),
                            t.headers.has("Accept") || r.setRequestHeader("Accept", "application/json, text/plain, */*"),
                            !t.headers.has("Content-Type")) {
                                var i = t.detectContentTypeHeader();
                                null !== i && r.setRequestHeader("Content-Type", i)
                            }
                            if (t.responseType) {
                                var o = t.responseType.toLowerCase();
                                r.responseType = "json" !== o ? o : "text"
                            }
                            var a = t.serializeBody()
                              , u = null
                              , s = function() {
                                if (null !== u)
                                    return u;
                                var e = 1223 === r.status ? 204 : r.status
                                  , n = r.statusText || "OK"
                                  , i = new Cf(r.getAllResponseHeaders())
                                  , o = function(t) {
                                    return "responseURL"in t && t.responseURL ? t.responseURL : /^X-Request-URL:/m.test(t.getAllResponseHeaders()) ? t.getResponseHeader("X-Request-URL") : null
                                }(r) || t.url;
                                return u = new Nf({
                                    headers: i,
                                    status: e,
                                    statusText: n,
                                    url: o
                                })
                            }
                              , l = function() {
                                var e = s()
                                  , i = e.headers
                                  , o = e.status
                                  , a = e.statusText
                                  , u = e.url
                                  , l = null;
                                204 !== o && (l = void 0 === r.response ? r.responseText : r.response),
                                0 === o && (o = l ? 200 : 0);
                                var c = o >= 200 && o < 300;
                                if ("json" === t.responseType && "string" == typeof l) {
                                    var h = l;
                                    l = l.replace(zf, "");
                                    try {
                                        l = "" !== l ? JSON.parse(l) : null
                                    } catch (f) {
                                        l = h,
                                        c && (c = !1,
                                        l = {
                                            error: f,
                                            text: l
                                        })
                                    }
                                }
                                c ? (n.next(new Mf({
                                    body: l,
                                    headers: i,
                                    status: o,
                                    statusText: a,
                                    url: u || void 0
                                })),
                                n.complete()) : n.error(new Uf({
                                    error: l,
                                    headers: i,
                                    status: o,
                                    statusText: a,
                                    url: u || void 0
                                }))
                            }
                              , c = function(t) {
                                var e = s()
                                  , i = new Uf({
                                    error: t,
                                    status: r.status || 0,
                                    statusText: r.statusText || "Unknown Error",
                                    url: e.url || void 0
                                });
                                n.error(i)
                            }
                              , h = !1
                              , f = function(e) {
                                h || (n.next(s()),
                                h = !0);
                                var i = {
                                    type: If.DownloadProgress,
                                    loaded: e.loaded
                                };
                                e.lengthComputable && (i.total = e.total),
                                "text" === t.responseType && r.responseText && (i.partialText = r.responseText),
                                n.next(i)
                            }
                              , d = function(t) {
                                var e = {
                                    type: If.UploadProgress,
                                    loaded: t.loaded
                                };
                                t.lengthComputable && (e.total = t.total),
                                n.next(e)
                            };
                            return r.addEventListener("load", l),
                            r.addEventListener("error", c),
                            t.reportProgress && (r.addEventListener("progress", f),
                            null !== a && r.upload && r.upload.addEventListener("progress", d)),
                            r.send(a),
                            n.next({
                                type: If.Sent
                            }),
                            function() {
                                r.removeEventListener("error", c),
                                r.removeEventListener("load", l),
                                t.reportProgress && (r.removeEventListener("progress", f),
                                null !== a && r.upload && r.upload.removeEventListener("progress", d)),
                                r.readyState !== r.DONE && r.abort()
                            }
                        }
                        ))
                    }
                }]),
                t
            }();
            return t.\u0275fac = function(e) {
                return new (e || t)(he(qf))
            }
            ,
            t.\u0275prov = Tt({
                token: t,
                factory: t.\u0275fac
            }),
            t
        }()
          , Wf = new ee("XSRF_COOKIE_NAME")
          , Gf = new ee("XSRF_HEADER_NAME")
          , Qf = function t() {
            m(this, t)
        }
          , Jf = function() {
            var t = function() {
                function t(e, n, r) {
                    m(this, t),
                    this.doc = e,
                    this.platform = n,
                    this.cookieName = r,
                    this.lastCookieString = "",
                    this.lastToken = null,
                    this.parseCount = 0
                }
                return _(t, [{
                    key: "getToken",
                    value: function() {
                        if ("server" === this.platform)
                            return null;
                        var t = this.doc.cookie || "";
                        return t !== this.lastCookieString && (this.parseCount++,
                        this.lastToken = Ku(t, this.cookieName),
                        this.lastCookieString = t),
                        this.lastToken
                    }
                }]),
                t
            }();
            return t.\u0275fac = function(e) {
                return new (e || t)(he(Au),he(ja),he(Wf))
            }
            ,
            t.\u0275prov = Tt({
                token: t,
                factory: t.\u0275fac
            }),
            t
        }()
          , Kf = function() {
            var t = function() {
                function t(e, n) {
                    m(this, t),
                    this.tokenService = e,
                    this.headerName = n
                }
                return _(t, [{
                    key: "intercept",
                    value: function(t, e) {
                        var n = t.url.toLowerCase();
                        if ("GET" === t.method || "HEAD" === t.method || n.startsWith("http://") || n.startsWith("https://"))
                            return e.handle(t);
                        var r = this.tokenService.getToken();
                        return null === r || t.headers.has(this.headerName) || (t = t.clone({
                            headers: t.headers.set(this.headerName, r)
                        })),
                        e.handle(t)
                    }
                }]),
                t
            }();
            return t.\u0275fac = function(e) {
                return new (e || t)(he(Qf),he(Gf))
            }
            ,
            t.\u0275prov = Tt({
                token: t,
                factory: t.\u0275fac
            }),
            t
        }()
          , Yf = function() {
            var t = function() {
                function t(e, n) {
                    m(this, t),
                    this.backend = e,
                    this.injector = n,
                    this.chain = null
                }
                return _(t, [{
                    key: "handle",
                    value: function(t) {
                        if (null === this.chain) {
                            var e = this.injector.get(Ff, []);
                            this.chain = e.reduceRight((function(t, e) {
                                return new Lf(t,e)
                            }
                            ), this.backend)
                        }
                        return this.chain.handle(t)
                    }
                }]),
                t
            }();
            return t.\u0275fac = function(e) {
                return new (e || t)(he(bf),he(ho))
            }
            ,
            t.\u0275prov = Tt({
                token: t,
                factory: t.\u0275fac
            }),
            t
        }()
          , Xf = function() {
            var t = function() {
                function t() {
                    m(this, t)
                }
                return _(t, null, [{
                    key: "disable",
                    value: function() {
                        return {
                            ngModule: t,
                            providers: [{
                                provide: Kf,
                                useClass: Vf
                            }]
                        }
                    }
                }, {
                    key: "withOptions",
                    value: function() {
                        var e = arguments.length > 0 && void 0 !== arguments[0] ? arguments[0] : {};
                        return {
                            ngModule: t,
                            providers: [e.cookieName ? {
                                provide: Wf,
                                useValue: e.cookieName
                            } : [], e.headerName ? {
                                provide: Gf,
                                useValue: e.headerName
                            } : []]
                        }
                    }
                }]),
                t
            }();
            return t.\u0275mod = Re({
                type: t
            }),
            t.\u0275inj = Ot({
                factory: function(e) {
                    return new (e || t)
                },
                providers: [Kf, {
                    provide: Ff,
                    useExisting: Kf,
                    multi: !0
                }, {
                    provide: Qf,
                    useClass: Jf
                }, {
                    provide: Wf,
                    useValue: "XSRF-TOKEN"
                }, {
                    provide: Gf,
                    useValue: "X-XSRF-TOKEN"
                }]
            }),
            t
        }()
          , $f = function() {
            var t = function t() {
                m(this, t)
            };
            return t.\u0275mod = Re({
                type: t
            }),
            t.\u0275inj = Ot({
                factory: function(e) {
                    return new (e || t)
                },
                providers: [Hf, {
                    provide: kf,
                    useClass: Yf
                }, Zf, {
                    provide: bf,
                    useExisting: Zf
                }, Bf, {
                    provide: qf,
                    useExisting: Bf
                }],
                imports: [[Xf.withOptions({
                    cookieName: "XSRF-TOKEN",
                    headerName: "X-XSRF-TOKEN"
                })]]
            }),
            t
        }();
        function td(t, e) {
            if (1 & t && (So(0, "marquee", 16, 17),
            jo(2),
            xo()),
            2 & t) {
                var n = function() {
                    return An(arguments.length > 0 && void 0 !== arguments[0] ? arguments[0] : 1)
                }();
                Fr(2),
                No("", n.marqueeId, " service currently undergoing maintenance, please try again later..")
            }
        }
        var ed = function() {
            function t(t) {
                this.httpClient = t,
                this.title = "JIIT Web kiosk",
                this.serverdown = !1
            }
            return t.prototype.checkServer = function(t, e, n) {
                var r = this;
                this.httpClient.get("" + xu + e + "/token/marqeelist").subscribe((function(e) {
                    "Success" === e.status.responseStatus && window.open("" + xu + t)
                }
                ), (function(t) {
                    r.serverdown = !0,
                    r.marqueeId = n,
                    clearTimeout(r.timeOut),
                    r.timeOut = setTimeout((function() {
                        r.serverdown && (r.serverdown = !1)
                    }
                    ), 6e4)
                }
                ))
            }
            ,
            t.\u0275fac = function(e) {
                return new (e || t)(ko(Hf))
            }
            ,
            t.\u0275cmp = Ee({
                type: t,
                selectors: [["app-root"]],
                decls: 44,
                vars: 1,
                consts: [["role", "banner", 1, "toolbar"], ["alt", "Angular Logo", "src", "assets/campusLynx-logo.png", "width", "130px", "height", "63px"], ["id", "heading", 2, "margin", "0px auto"], ["role", "main", 1, "content"], ["src", "assets/Logo-jiit.png", "width", "150", "height", "150"], ["behavior", "alternate", "style", "font-size:20px; color: red;", 4, "ngIf"], [1, "card-container"], ["target", "_blank", "rel", "noopener", 1, "card", 2, "background", "#e011112e", "height", "50px", "width", "230px", 3, "click"], ["xmlns", "http://www.w3.org/2000/svg", "width", "24", "height", "24", "viewBox", "0 0 24 24", 1, "material-icons"], ["d", "M5 13.18v4L12 21l7-3.82v-4L12 17l-7-3.82zM12 3L1 9l11 6 9-4.91V17h2V9L12 3z"], ["d", "M10 6L8.59 7.41 13.17 12l-4.58 4.59L10 18l6-6z"], ["id", "clouds", "alt", "Gray Clouds Background", "xmlns", "http://www.w3.org/2000/svg", "width", "2611.084", "height", "485.677", "viewBox", "0 0 2611.084 485.677"], ["id", "Path_39", "data-name", "Path 39", "d", "M2379.709,863.793c10-93-77-171-168-149-52-114-225-105-264,15-75,3-140,59-152,133-30,2.83-66.725,9.829-93.5,26.25-26.771-16.421-63.5-23.42-93.5-26.25-12-74-77-130-152-133-39-120-212-129-264-15-54.084-13.075-106.753,9.173-138.488,48.9-31.734-39.726-84.4-61.974-138.487-48.9-52-114-225-105-264,15a162.027,162.027,0,0,0-103.147,43.044c-30.633-45.365-87.1-72.091-145.206-58.044-52-114-225-105-264,15-75,3-140,59-152,133-53,5-127,23-130,83-2,42,35,72,70,86,49,20,106,18,157,5a165.625,165.625,0,0,0,120,0c47,94,178,113,251,33,61.112,8.015,113.854-5.72,150.492-29.764a165.62,165.62,0,0,0,110.861-3.236c47,94,178,113,251,33,31.385,4.116,60.563,2.495,86.487-3.311,25.924,5.806,55.1,7.427,86.488,3.311,73,80,204,61,251-33a165.625,165.625,0,0,0,120,0c51,13,108,15,157-5a147.188,147.188,0,0,0,33.5-18.694,147.217,147.217,0,0,0,33.5,18.694c49,20,106,18,157,5a165.625,165.625,0,0,0,120,0c47,94,178,113,251,33C2446.709,1093.793,2554.709,922.793,2379.709,863.793Z", "transform", "translate(142.69 -634.312)", "fill", "#eee"], [2, "color", "white", "position", "fixed", "bottom", "0px", "left", "0px", "height", "30px", "padding", "10px", "background-color", "#1976d2", "width", "100%"], ["href", "http://www.jilit.co.in/", "target", "_blank", 2, "color", "white", "text-decoration", "underline"], ["src", "assets/jilit-logo.png", 2, "position", "absolute", "right", "30px", "top", "10px"], ["behavior", "alternate", 2, "font-size", "20px", "color", "red"], ["testmarquee", ""]],
                template: function(t, e) {
                    1 & t && (So(0, "div", 0),
                    Eo(1, "img", 1),
                    jo(2, " \xa0\xa0 \xa0\xa0 \xa0\xa0 \xa0\xa0 \xa0\xa0 \xa0\xa0 "),
                    So(3, "span", 2),
                    jo(4, "Jaypee Institute Of Information Technology (JIIT) Noida"),
                    xo(),
                    xo(),
                    Eo(5, "br"),
                    Eo(6, "br"),
                    So(7, "div", 3),
                    So(8, "div"),
                    Eo(9, "img", 4),
                    xo(),
                    Eo(10, "br"),
                    function(t, e, n, r, i, o, a, u) {
                        var s = hn()
                          , l = fn()
                          , c = l.firstCreatePass ? function(t, e, n, r, i, o, a, u, s) {
                            var l = e.consts
                              , c = Zr(e, 11, 0, "marquee", an(l, 5));
                            ei(e, n, c, an(l, undefined)),
                            Nn(e, c);
                            var h = c.tViews = $r(2, c, r, 3, 1, e.directiveRegistry, e.pipeRegistry, null, e.schemas, l);
                            return null !== e.queries && (e.queries.template(e, c),
                            h.queries = e.queries.embeddedTView(c)),
                            c
                        }(0, l, s, e) : l.data[31];
                        vn(c, !1);
                        var h = s[11].createComment("");
                        Mi(l, s, h, c),
                        Cr(h, s),
                        gi(s, s[31] = di(h, s, h, c)),
                        ze(c) && Kr(l, s, c)
                    }(0, td),
                    So(12, "label"),
                    jo(13, "Click Here For Student or Parent Portal"),
                    xo(),
                    So(14, "div", 6),
                    So(15, "a", 7),
                    Oo("click", (function() {
                        return e.checkServer(":6011/studentportal/", ":6011/StudentPortalAPI", "Student or Parent Portal ")
                    }
                    )),
                    In(),
                    So(16, "svg", 8),
                    Eo(17, "path", 9),
                    xo(),
                    jn(),
                    So(18, "span"),
                    So(19, "b"),
                    jo(20, "Student or Parent Portal"),
                    xo(),
                    xo(),
                    In(),
                    So(21, "svg", 8),
                    Eo(22, "path", 10),
                    xo(),
                    xo(),
                    xo(),
                    jn(),
                    Eo(23, "br"),
                    So(24, "label"),
                    jo(25, "Click Here For Employee Portal"),
                    xo(),
                    So(26, "div", 6),
                    So(27, "a", 7),
                    Oo("click", (function() {
                        return e.checkServer(":6010/employeeportal", ":6010/EmployeePortalAPI", "Employee Portal ")
                    }
                    )),
                    In(),
                    So(28, "svg", 8),
                    Eo(29, "path", 9),
                    xo(),
                    jn(),
                    So(30, "span"),
                    So(31, "b"),
                    jo(32, "Employee Portal"),
                    xo(),
                    xo(),
                    In(),
                    So(33, "svg", 8),
                    Eo(34, "path", 10),
                    xo(),
                    xo(),
                    xo(),
                    So(35, "svg", 11),
                    Eo(36, "path", 12),
                    xo(),
                    jn(),
                    So(37, "div", 13),
                    So(38, "p"),
                    jo(39, "Powered By: "),
                    So(40, "a", 14),
                    jo(41, "JIL Information Technology Ltd."),
                    xo(),
                    xo(),
                    Eo(42, "img", 15),
                    xo(),
                    xo(),
                    Eo(43, "router-outlet")),
                    2 & t && (Fr(11),
                    bo("ngIf", e.serverdown))
                },
                directives: [Yu, Xh],
                styles: ["", '[_nghost-%COMP%] {\n    font-family: -apple-system, BlinkMacSystemFont, "Segoe UI", Roboto, Helvetica, Arial, sans-serif, "Apple Color Emoji", "Segoe UI Emoji", "Segoe UI Symbol";\n    font-size: 14px;\n    color: #333;\n    box-sizing: border-box;\n    -webkit-font-smoothing: antialiased;\n    -moz-osx-font-smoothing: grayscale;\n  }\n\n  h1[_ngcontent-%COMP%], h2[_ngcontent-%COMP%], h3[_ngcontent-%COMP%], h4[_ngcontent-%COMP%], h5[_ngcontent-%COMP%], h6[_ngcontent-%COMP%] {\n    margin: 8px 0;\n  }\n\n  p[_ngcontent-%COMP%] {\n    margin: 0;\n  }\n\n  .spacer[_ngcontent-%COMP%] {\n    flex: 1;\n  }\n\n  .toolbar[_ngcontent-%COMP%] {\n    position: absolute;\n    top: 0;\n    left: 0;\n    right: 0;\n    height: 60px;\n    display: flex;\n    align-items: center;\n    background-color: #1976d2;\n    color: white;\n    font-weight: 600;\n  }\n\n  .toolbar[_ngcontent-%COMP%]   img[_ngcontent-%COMP%] {\n    margin: 0 16px;\n  }\n\n  .toolbar[_ngcontent-%COMP%]   #twitter-logo[_ngcontent-%COMP%] {\n    height: 40px;\n    margin: 0 16px;\n  }\n\n  .toolbar[_ngcontent-%COMP%]   #twitter-logo[_ngcontent-%COMP%]:hover {\n    opacity: 0.8;\n  }\n\n  .content[_ngcontent-%COMP%] {\n    display: flex;\n    margin: 82px auto 32px;\n    padding: 0 16px;\n    max-width: 960px;\n    flex-direction: column;\n    align-items: center;\n  }\n\n  svg.material-icons[_ngcontent-%COMP%] {\n    height: 24px;\n    width: auto;\n  }\n\n  svg.material-icons[_ngcontent-%COMP%]:not(:last-child) {\n    margin-right: 8px;\n  }\n\n  .card[_ngcontent-%COMP%]   svg.material-icons[_ngcontent-%COMP%]   path[_ngcontent-%COMP%] {\n    fill: #888;\n  }\n\n  .card-container[_ngcontent-%COMP%] {\n    display: flex;\n    flex-wrap: wrap;\n    justify-content: center;\n    margin-top: 16px;\n  }\n\n  .card[_ngcontent-%COMP%] {\n    border-radius: 4px;\n    border: 1px solid #eee;\n    background-color: #fafafa;\n    height: 40px;\n    width: 200px;\n    margin: 0 8px 16px;\n    padding: 16px;\n    display: flex;\n    flex-direction: row;\n    justify-content: center;\n    align-items: center;\n    transition: all 0.2s ease-in-out;\n    line-height: 24px;\n  }\n\n  .card-container[_ngcontent-%COMP%]   .card[_ngcontent-%COMP%]:not(:last-child) {\n    margin-right: 0;\n  }\n\n  .card.card-small[_ngcontent-%COMP%] {\n    height: 16px;\n    width: 168px;\n  }\n\n  .card-container[_ngcontent-%COMP%]   .card[_ngcontent-%COMP%]:not(.highlight-card) {\n    cursor: pointer;\n  }\n\n  .card-container[_ngcontent-%COMP%]   .card[_ngcontent-%COMP%]:not(.highlight-card):hover {\n    transform: translateY(-3px);\n    box-shadow: 0 4px 17px rgba(0, 0, 0, 0.35);\n  }\n\n  .card-container[_ngcontent-%COMP%]   .card[_ngcontent-%COMP%]:not(.highlight-card):hover   .material-icons[_ngcontent-%COMP%]   path[_ngcontent-%COMP%] {\n    fill: rgb(105, 103, 103);\n  }\n\n  .card.highlight-card[_ngcontent-%COMP%] {\n    background-color: #1976d2;\n    color: white;\n    font-weight: 600;\n    border: none;\n    width: auto;\n    min-width: 30%;\n    position: relative;\n  }\n\n  .card.card.highlight-card[_ngcontent-%COMP%]   span[_ngcontent-%COMP%] {\n    margin-left: 60px;\n  }\n\n  svg#rocket[_ngcontent-%COMP%] {\n    width: 80px;\n    position: absolute;\n    left: -10px;\n    top: -24px;\n  }\n\n  svg#rocket-smoke[_ngcontent-%COMP%] {\n    height: calc(100vh - 95px);\n    position: absolute;\n    top: 10px;\n    right: 180px;\n    z-index: -10;\n  }\n\n  a[_ngcontent-%COMP%], a[_ngcontent-%COMP%]:visited, a[_ngcontent-%COMP%]:hover {\n    color: #1976d2;\n    text-decoration: none;\n  }\n\n  a[_ngcontent-%COMP%]:hover {\n    color: #125699;\n  }\n\n  .terminal[_ngcontent-%COMP%] {\n    position: relative;\n    width: 80%;\n    max-width: 600px;\n    border-radius: 6px;\n    padding-top: 45px;\n    margin-top: 8px;\n    overflow: hidden;\n    background-color: rgb(15, 15, 16);\n  }\n\n  .terminal[_ngcontent-%COMP%]::before {\n    content: "\\2022 \\2022 \\2022";\n    position: absolute;\n    top: 0;\n    left: 0;\n    height: 4px;\n    background: rgb(58, 58, 58);\n    color: #c2c3c4;\n    width: 100%;\n    font-size: 2rem;\n    line-height: 0;\n    padding: 14px 0;\n    text-indent: 4px;\n  }\n\n  .terminal[_ngcontent-%COMP%]   pre[_ngcontent-%COMP%] {\n    font-family: SFMono-Regular, Consolas, Liberation Mono, Menlo, monospace;\n    color: white;\n    padding: 0 1rem 1rem;\n    margin: 0;\n  }\n\n  .circle-link[_ngcontent-%COMP%] {\n    height: 40px;\n    width: 40px;\n    border-radius: 40px;\n    margin: 8px;\n    background-color: white;\n    border: 1px solid #eeeeee;\n    display: flex;\n    justify-content: center;\n    align-items: center;\n    cursor: pointer;\n    box-shadow: 0 1px 3px rgba(0, 0, 0, 0.12), 0 1px 2px rgba(0, 0, 0, 0.24);\n    transition: 1s ease-out;\n  }\n\n  .circle-link[_ngcontent-%COMP%]:hover {\n    transform: translateY(-0.25rem);\n    box-shadow: 0px 3px 15px rgba(0, 0, 0, 0.2);\n  }\n\n  footer[_ngcontent-%COMP%] {\n    margin-top: 8px;\n    display: flex;\n    align-items: center;\n    line-height: 20px;\n  }\n\n  footer[_ngcontent-%COMP%]   a[_ngcontent-%COMP%] {\n    display: flex;\n    align-items: center;\n  }\n\n  .github-star-badge[_ngcontent-%COMP%] {\n    color: #24292e;\n    display: flex;\n    align-items: center;\n    font-size: 12px;\n    padding: 3px 10px;\n    border: 1px solid rgba(27, 31, 35, .2);\n    border-radius: 3px;\n    background-image: linear-gradient(-180deg, #fafbfc, #eff3f6 90%);\n    margin-left: 4px;\n    font-weight: 600;\n    font-family: -apple-system, BlinkMacSystemFont, Segoe UI, Helvetica, Arial, sans-serif, Apple Color Emoji, Segoe UI Emoji, Segoe UI Symbol;\n  }\n\n  .github-star-badge[_ngcontent-%COMP%]:hover {\n    background-image: linear-gradient(-180deg, #f0f3f6, #e6ebf1 90%);\n    border-color: rgba(27, 31, 35, .35);\n    background-position: -.5em;\n  }\n\n  .github-star-badge[_ngcontent-%COMP%]   .material-icons[_ngcontent-%COMP%] {\n    height: 16px;\n    width: 16px;\n    margin-right: 4px;\n  }\n\n  svg#clouds[_ngcontent-%COMP%] {\n    position: fixed;\n    bottom: -160px;\n    left: -230px;\n    z-index: -10;\n    width: 1920px;\n  }\n\n  #heading[_ngcontent-%COMP%] {\n    font-size: 30px;\n  }\n\n  \n  @media screen and (max-width: 767px) {\n\n    .card-container[_ngcontent-%COMP%] > *[_ngcontent-%COMP%]:not(.circle-link), .terminal[_ngcontent-%COMP%] {\n      width: 100%;\n    }\n\n    .card[_ngcontent-%COMP%]:not(.highlight-card) {\n      height: 16px;\n      margin: 8px 0;\n    }\n\n    .card.highlight-card[_ngcontent-%COMP%]   span[_ngcontent-%COMP%] {\n      margin-left: 72px;\n    }\n\n    svg#rocket-smoke[_ngcontent-%COMP%] {\n      right: 120px;\n      transform: rotate(-5deg);\n    }\n\n    #heading[_ngcontent-%COMP%] {\n      font-size: 15px;\n    }\n  }\n\n  @media screen and (max-width: 575px) {\n    svg#rocket-smoke[_ngcontent-%COMP%] {\n      display: none;\n      visibility: hidden;\n    }\n\n    #heading[_ngcontent-%COMP%] {\n      font-size: 15px;\n    }\n  }']
            }),
            t
        }()
          , nd = function() {
            function t() {}
            return t.\u0275mod = Re({
                type: t,
                bootstrap: [ed]
            }),
            t.\u0275inj = Ot({
                factory: function(e) {
                    return new (e || t)
                },
                providers: [],
                imports: [[Ns, _f, $f]]
            }),
            t
        }();
        (function() {
            if (kr)
                throw new Error("Cannot enable prod mode after platform setup.");
            _r = !1
        }
        )(),
        Is().bootstrapModule(nd).catch((function(t) {
            return console.error(t)
        }
        ))
    },
    zn8P: function(t, e) {
        function n(t) {
            return Promise.resolve().then((function() {
                var e = new Error("Cannot find module '" + t + "'");
                throw e.code = "MODULE_NOT_FOUND",
                e
            }
            ))
        }
        n.keys = function() {
            return []
        }
        ,
        n.resolve = n,
        t.exports = n,
        n.id = "zn8P"
    }
}, [[0, 0]]]);