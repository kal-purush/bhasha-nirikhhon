! function (g, i, m, f, d, k, e) {
    new(function () {});
    var c = {
            B: m.PI,
            C: m.max,
            u: m.min,
            ub: m.ceil,
            cb: m.floor,
            tb: m.abs,
            sb: m.sin,
            oc: m.cos,
            pd: m.tan,
            Te: m.atan,
            lc: m.sqrt,
            q: m.pow,
            G: m.round
        },
        h = {
            Qe: function (a) {
                return -c.oc(a * c.B) / 2 + .5
            },
            ac: function (a) {
                return a
            },
            Pe: function (a) {
                return a * a
            },
            Xd: function (a) {
                return -a * (a - 2)
            },
            nf: function (a) {
                return (a *= 2) < 1 ? 1 / 2 * a * a : -1 / 2 * (--a * (a - 2) - 1)
            },
            of: function (a) {
                return a * a * a
            },
            Kf: function (a) {
                return (a -= 1) * a * a + 1
            },
            Jf: function (a) {
                return (a *= 2) < 1 ? 1 / 2 * a * a * a : 1 / 2 * ((a -= 2) * a * a + 2)
            },
            If: function (a) {
                return a * a * a * a
            },
            Hf: function (a) {
                return -((a -= 1) * a * a * a - 1)
            },
            Lf: function (a) {
                return (a *= 2) < 1 ? 1 / 2 * a * a * a * a : -1 / 2 * ((a -= 2) * a * a * a - 2)
            },
            og: function (a) {
                return a * a * a * a * a
            },
            hf: function (a) {
                return (a -= 1) * a * a * a * a + 1
            },
            pf: function (a) {
                return (a *= 2) < 1 ? 1 / 2 * a * a * a * a * a : 1 / 2 * ((a -= 2) * a * a * a * a + 2)
            },
            qf: function (a) {
                return 1 - c.oc(c.B / 2 * a)
            },
            rf: function (a) {
                return c.sb(c.B / 2 * a)
            },
            sf: function (a) {
                return -1 / 2 * (c.oc(c.B * a) - 1)
            },
            tf: function (a) {
                return a == 0 ? 0 : c.q(2, 10 * (a - 1))
            },
            uf: function (a) {
                return a == 1 ? 1 : -c.q(2, -10 * a) + 1
            },
            vf: function (a) {
                return a == 0 || a == 1 ? a : (a *= 2) < 1 ? 1 / 2 * c.q(2, 10 * (a - 1)) : 1 / 2 * (-c.q(2, -10 * --a) + 2)
            },
            wf: function (a) {
                return -(c.lc(1 - a * a) - 1)
            },
            xf: function (a) {
                return c.lc(1 - (a -= 1) * a)
            },
            yf: function (a) {
                return (a *= 2) < 1 ? -1 / 2 * (c.lc(1 - a * a) - 1) : 1 / 2 * (c.lc(1 - (a -= 2) * a) + 1)
            },
            Af: function (a) {
                if (!a || a == 1) return a;
                var b = .3,
                    d = .075;
                return -(c.q(2, 10 * (a -= 1)) * c.sb((a - d) * 2 * c.B / b))
            },
            Bf: function (a) {
                if (!a || a == 1) return a;
                var b = .3,
                    d = .075;
                return c.q(2, -10 * a) * c.sb((a - d) * 2 * c.B / b) + 1
            },
            Cf: function (a) {
                if (!a || a == 1) return a;
                var b = .45,
                    d = .1125;
                return (a *= 2) < 1 ? -.5 * c.q(2, 10 * (a -= 1)) * c.sb((a - d) * 2 * c.B / b) : c.q(2, -10 * (a -= 1)) * c.sb((a - d) * 2 * c.B / b) * .5 + 1
            },
            Df: function (a) {
                var b = 1.70158;
                return a * a * ((b + 1) * a - b)
            },
            Ef: function (a) {
                var b = 1.70158;
                return (a -= 1) * a * ((b + 1) * a + b) + 1
            },
            Ff: function (a) {
                var b = 1.70158;
                return (a *= 2) < 1 ? 1 / 2 * a * a * (((b *= 1.525) + 1) * a - b) : 1 / 2 * ((a -= 2) * a * (((b *= 1.525) + 1) * a + b) + 2)
            },
            sd: function (a) {
                return 1 - h.dd(1 - a)
            },
            dd: function (a) {
                return a < 1 / 2.75 ? 7.5625 * a * a : a < 2 / 2.75 ? 7.5625 * (a -= 1.5 / 2.75) * a + .75 : a < 2.5 / 2.75 ? 7.5625 * (a -= 2.25 / 2.75) * a + .9375 : 7.5625 * (a -= 2.625 / 2.75) * a + .984375
            },
            Gf: function (a) {
                return a < 1 / 2 ? h.sd(a * 2) * .5 : h.dd(a * 2 - 1) * .5 + .5
            },
            mf: c.ub,
            lf: c.cb
        };
    var b = new function () {
        var j = this,
            vb = /\S+/g,
            G = 1,
            pb = 2,
            sb = 3,
            rb = 4,
            Z = 5,
            H, r = 0,
            n = 0,
            B = 0,
            A = navigator,
            fb = A.appName,
            p = A.userAgent,
            q = parseFloat;

        function Eb() {
            if (!H) {
                H = {
                    bd: "ontouchstart" in g || "createTouch" in i
                };
                var a;
                if (A.pointerEnabled || (a = A.msPointerEnabled)) H.de = a ? "msTouchAction" : "touchAction"
            }
            return H
        }

        function u(h) {
            if (!r) {
                r = -1;
                if (fb == "Microsoft Internet Explorer" && !!g.attachEvent && !!g.ActiveXObject) {
                    var e = p.indexOf("MSIE");
                    r = G;
                    n = q(p.substring(e + 5, p.indexOf(";", e))); /*@cc_on@*/
                } else if (fb == "Netscape" && !!g.addEventListener) {
                    var d = p.indexOf("Firefox"),
                        b = p.indexOf("Safari"),
                        f = p.indexOf("Chrome"),
                        c = p.indexOf("AppleWebKit");
                    if (d >= 0) {
                        r = pb;
                        n = q(p.substring(d + 8))
                    } else if (b >= 0) {
                        var i = p.substring(0, b).lastIndexOf("/");
                        r = f >= 0 ? rb : sb;
                        n = q(p.substring(i + 1, b))
                    } else {
                        var a = /Trident\/.*rv:([0-9]{1,}[\.0-9]{0,})/i.exec(p);
                        if (a) {
                            r = G;
                            n = q(a[1])
                        }
                    }
                    if (c >= 0) B = q(p.substring(c + 12))
                } else {
                    var a = /(opera)(?:.*version|)[ \/]([\w.]+)/i.exec(p);
                    if (a) {
                        r = Z;
                        n = q(a[2])
                    }
                }
            }
            return h == r
        }

        function v() {
            return u(G)
        }

        function qb() {
            return u(sb)
        }

        function ub() {
            return u(Z)
        }

        function mb() {
            return qb() && B > 534 && B < 535
        }

        function I() {
            u();
            return B > 537 || n > 42 || r == G && n >= 11
        }

        function nb(a) {
            var b, c;
            return function (g) {
                if (!b) {
                    b = d;
                    var f = a.substr(0, 1).toUpperCase() + a.substr(1);
                    l([a].concat(["WebKit", "ms", "Moz", "O", "webkit"]), function (h, d) {
                        var b = a;
                        if (d) b = h + f;
                        if (g.style[b] != e) return c = b
                    })
                }
                return c
            }
        }

        function lb(b) {
            var a;
            return function (c) {
                a = a || nb(b)(c) || b;
                return a
            }
        }
        var L = lb("transform");

        function eb(a) {
            return {}.toString.call(a)
        }
        var bb = {};
        l(["Boolean", "Number", "String", "Function", "Array", "Date", "RegExp", "Object"], function (a) {
            bb["[object " + a + "]"] = a.toLowerCase()
        });

        function l(b, d) {
            var a, c;
            if (eb(b) == "[object Array]") {
                for (a = 0; a < b.length; a++)
                    if (c = d(b[a], a, b)) return c
            } else
                for (a in b)
                    if (c = d(b[a], a, b)) return c
        }

        function F(a) {
            return a == f ? String(a) : bb[eb(a)] || "object"
        }

        function cb(a) {
            for (var b in a) return d
        }

        function C(a) {
            try {
                return F(a) == "object" && !a.nodeType && a != a.window && (!a.constructor || {}.hasOwnProperty.call(a.constructor.prototype, "isPrototypeOf"))
            } catch (b) {}
        }

        function jb(b, a) {
            setTimeout(b, a || 0)
        }

        function ab(b, d, c) {
            var a = !b || b == "inherit" ? "" : b;
            l(d, function (c) {
                var b = c.exec(a);
                if (b) {
                    var d = a.substr(0, b.index),
                        e = a.substr(b.index + b[0].length + 1, a.length - 1);
                    a = d + e
                }
            });
            a && (c += (!a.indexOf(" ") ? "" : " ") + a);
            return c
        }

        function gb(a, b) {
            if (a === e) a = b;
            return a
        }
        j.Vc = Eb;
        j.Yd = v;
        j.Bg = qb;
        j.xg = I;
        nb("transform");
        j.Xc = function () {
            return n
        };
        j.hc = jb;
        j.K = gb;
        j.ab = function (a, b) {
            b.call(a);
            return z({}, a)
        };

        function U(a) {
            a.constructor === U.caller && a.s && a.s.apply(a, U.caller.arguments)
        }
        j.s = U;
        j.Rb = function (a) {
            if (j.tg(a)) a = i.getElementById(a);
            return a
        };

        function t(a) {
            return a || g.event
        }
        j.vg = t;
        j.Lb = function (b) {
            b = t(b);
            var a = b.target || b.srcElement || i;
            if (a.nodeType == 3) a = j.ad(a);
            return a
        };
        j.Nd = function (a) {
            a = t(a);
            return a.relatedTarget || a.toElement
        };
        j.Md = function (a) {
            a = t(a);
            return a.which || ([0, 1, 3, 0, 2])[a.button] || a.charCode || a.keyCode
        };
        j.ed = function (a) {
            a = t(a);
            return {
                x: a.clientX || 0,
                y: a.clientY || 0
            }
        };
        j.wg = function (b, a) {
            return b.x >= a.x && b.x <= a.x + a.w && b.y >= a.y && b.y <= a.y + a.h
        };
        j.Id = function (c, e) {
            var a = b.pg(e),
                d = b.ed(c);
            return j.wg(d, a)
        };

        function w(c, d, a) {
            if (a !== e) c.style[d] = a == e ? "" : a;
            else {
                var b = c.currentStyle || c.style;
                a = b[d];
                if (a == "" && g.getComputedStyle) {
                    b = c.ownerDocument.defaultView.getComputedStyle(c, f);
                    b && (a = b.getPropertyValue(d) || b[d])
                }
                return a
            }
        }

        function X(b, c, a, d) {
            if (a === e) {
                a = q(w(b, c));
                isNaN(a) && (a = f);
                return a
            }
            if (a == f) a = "";
            else d && (a += "px");
            w(b, c, a)
        }

        function m(c, a) {
            var d = a ? X : w,
                b;
            if (a & 4) b = lb(c);
            return function (e, f) {
                return d(e, b ? b(e) : c, f, a & 2)
            }
        }

        function zb(a) {
            return q(a.style.opacity || "1")
        }

        function Bb(b, a) {
            b.style.opacity = a == 1 || a == f ? "" : c.G(a * 100) / 100
        }
        var N = {
            bb: ["rotate"],
            mb: ["rotateX"],
            fb: ["rotateY"],
            bc: ["skewX"],
            nc: ["skewY"]
        };
        if (!I()) N = z(N, {
            V: ["scaleX", 2],
            U: ["scaleY", 2],
            gb: ["translateZ", 1]
        });

        function M(c, a) {
            var b = "";
            if (a) {
                if (v() && n && n < 10) {
                    delete a.mb;
                    delete a.fb;
                    delete a.gb
                }
                l(a, function (d, c) {
                    var a = N[c];
                    if (a) {
                        var e = a[1] || 0;
                        if (O[c] != d) b += " " + a[0] + "(" + d + (["deg", "px", ""])[e] + ")"
                    }
                });
                if (I()) {
                    if (a.zb || a.Jb || a.gb != e) b += " translate3d(" + (a.zb || 0) + "px," + (a.Jb || 0) + "px," + (a.gb || 0) + "px)";
                    if (a.V == e) a.V = 1;
                    if (a.U == e) a.U = 1;
                    if (a.V != 1 || a.U != 1) b += " scale3d(" + a.V + ", " + a.U + ", 1)"
                }
            }
            c.style[L(c)] = b
        }
        j.Ag = m("transformOrigin", 4);
        j.Fg = m("backfaceVisibility", 4);
        j.cc = m("transformStyle", 4);
        j.Gg = m("perspective", 6);
        j.ug = m("perspectiveOrigin", 4);
        j.Sd = function (b, a) {
            if (v() && n < 9) b.style.zoom = a == 1 ? "" : a;
            else {
                var c = L(b),
                    f = a == 1 ? "" : "scale(" + a + ")",
                    e = b.style[c],
                    g = new RegExp(/[\s]*scale\(.*?\)/g),
                    d = ab(e, [g], f);
                b.style[c] = d
            }
        };
        j.hb = function (a, d, b, c) {
            a = j.Rb(a);
            if (a.addEventListener) {
                d == "mousewheel" && a.addEventListener("DOMMouseScroll", b, c);
                a.addEventListener(d, b, c)
            } else if (a.attachEvent) {
                a.attachEvent("on" + d, b);
                c && a.setCapture && a.setCapture()
            }
        };
        j.Hb = function (a, c, d, b) {
            a = j.Rb(a);
            if (a.removeEventListener) {
                c == "mousewheel" && a.removeEventListener("DOMMouseScroll", d, b);
                a.removeEventListener(c, d, b)
            } else if (a.detachEvent) {
                a.detachEvent("on" + c, d);
                b && a.releaseCapture && a.releaseCapture()
            }
        };
        j.Sb = function (a) {
            a = t(a);
            a.preventDefault && a.preventDefault();
            a.cancel = d;
            a.returnValue = k
        };
        j.mg = function (a) {
            a = t(a);
            a.stopPropagation && a.stopPropagation();
            a.cancelBubble = d
        };
        j.db = function (d, c) {
            var a = [].slice.call(arguments, 2),
                b = function () {
                    var b = a.concat([].slice.call(arguments, 0));
                    return c.apply(d, b)
                };
            return b
        };
        j.Pf = function (a, b) {
            if (b == e) return a.textContent || a.innerText;
            var c = i.createTextNode(b);
            j.Vb(a);
            a.appendChild(c)
        };
        j.pg = function (b) {
            var a = b.getBoundingClientRect();
            return {
                x: a.left,
                y: a.top,
                w: a.right - a.left,
                h: a.bottom - a.top
            }
        };
        j.Gb = function (d, c) {
            for (var b = [], a = d.firstChild; a; a = a.nextSibling)(c || a.nodeType == 1) && b.push(a);
            return b
        };

        function db(a, c, e, b) {
            b = b || "u";
            for (a = a ? a.firstChild : f; a; a = a.nextSibling)
                if (a.nodeType == 1) {
                    if (D(a, b) == c) return a;
                    if (!e) {
                        var d = db(a, c, e, b);
                        if (d) return d
                    }
                }
        }
        j.be = db;

        function S(a, d, g, b) {
            b = b || "u";
            var c = [];
            for (a = a ? a.firstChild : f; a; a = a.nextSibling)
                if (a.nodeType == 1) {
                    D(a, b) == d && c.push(a);
                    if (!g) {
                        var e = S(a, d, g, b);
                        if (e.length) c = c.concat(e)
                    }
                } return c
        }
        j.Xf = function (b, a) {
            return b.getElementsByTagName(a)
        };
        j.eb = function (a, f, d, g) {
            d = d || "u";
            var e;
            do {
                if (a.nodeType == 1) {
                    var c;
                    d && (c = D(a, d));
                    if (c && c == gb(f, c) || g == a.tagName) {
                        e = a;
                        break
                    }
                }
                a = b.ad(a)
            } while (a && a != i.body);
            return e
        };
        j.Yf = function (a) {
            return W(["INPUT", "TEXTAREA", "SELECT"])[a.tagName]
        };

        function z() {
            var f = arguments,
                d, c, b, a, h = 1 & f[0],
                g = 1 + h;
            d = f[g - 1] || {};
            for (; g < f.length; g++)
                if (c = f[g])
                    for (b in c) {
                        a = c[b];
                        if (a !== e) {
                            a = c[b];
                            var i = d[b];
                            d[b] = h && (C(i) || C(a)) ? z(h, {}, i, a) : a
                        }
                    }
            return d
        }
        j.jb = z;

        function V(f, g) {
            var d = {},
                c, a, b;
            for (c in f) {
                a = f[c];
                b = g[c];
                if (a !== b) {
                    var e;
                    if (C(a) && C(b)) {
                        a = V(a, b);
                        e = !cb(a)
                    }!e && (d[c] = a)
                }
            }
            return d
        }
        j.Ed = function (a) {
            return F(a) == "function"
        };
        j.ag = function (a) {
            return F(a) == "array"
        };
        j.tg = function (a) {
            return F(a) == "string"
        };
        j.Ic = function (a) {
            return !isNaN(q(a)) && isFinite(a)
        };
        j.g = l;
        j.af = C;

        function Q(a) {
            return i.createElement(a)
        }
        j.Mc = function () {
            return Q("DIV")
        };
        j.cg = function () {
            return Q("SPAN")
        };
        j.Ne = function () {};

        function E(b, c, a) {
            if (a == e) return b.getAttribute(c);
            b.setAttribute(c, a)
        }

        function D(a, b) {
            return E(a, b) || E(a, "data-" + b)
        }
        j.l = E;
        j.W = D;
        j.n = function (d, b, c) {
            var a = j.eg(D(d, b));
            if (isNaN(a)) a = c;
            return a
        };

        function x(b, a) {
            return E(b, "class", a) || ""
        }

        function W(b) {
            var a = {};
            l(b, function (b) {
                if (b != e) a[b] = b
            });
            return a
        }

        function kb(b, a) {
            return b.match(a || vb)
        }

        function P(b, a) {
            return W(kb(b || "", a))
        }
        j.jd = W;
        j.gg = kb;
        j.hg = function (a) {
            a && (a = a.toLowerCase());
            return a
        };

        function Y(b, c) {
            var a = "";
            l(c, function (c) {
                a && (a += b);
                a += c
            });
            return a
        }

        function J(a, c, b) {
            x(a, Y(" ", z(V(P(x(a)), P(c)), P(b))))
        }
        j.ad = function (a) {
            return a.parentNode
        };
        j.vb = function (a) {
            j.qb(a, "none")
        };
        j.Tb = function (a, b) {
            j.qb(a, b ? "none" : "")
        };
        j.jg = function (b, a) {
            b.removeAttribute(a)
        };
        j.kg = function (d, a) {
            if (a) d.style.clip = "rect(" + c.G(a.i || a.A || 0) + "px " + c.G(a.v) + "px " + c.G(a.z) + "px " + c.G(a.j || a.J || 0) + "px)";
            else if (a !== e) {
                var h = d.style.cssText,
                    g = [new RegExp(/[\s]*clip: rect\(.*?\)[;]?/i), new RegExp(/[\s]*cliptop: .*?[;]?/i), new RegExp(/[\s]*clipright: .*?[;]?/i), new RegExp(/[\s]*clipbottom: .*?[;]?/i), new RegExp(/[\s]*clipleft: .*?[;]?/i)],
                    f = ab(h, g, "");
                b.zd(d, f)
            }
        };
        j.lg = function (b, a) {
            if (a) b.style.backgroundColor = "rgba(" + c.G(a.yd) + "," + c.G(a.xd) + "," + c.G(a.wd) + "," + a.S + ")"
        };
        j.Qb = function () {
            return +new Date
        };
        j.Z = function (b, a) {
            b.appendChild(a)
        };
        j.pb = function (b, a, c) {
            (c || a.parentNode).insertBefore(b, a)
        };
        j.Pb = function (b, a) {
            a = a || b.parentNode;
            a && a.removeChild(b)
        };
        j.Ve = function (a, b) {
            l(a, function (a) {
                j.Pb(a, b)
            })
        };
        j.Vb = function (a) {
            j.Ve(j.Gb(a, d), a)
        };

        function hb() {
            l([].slice.call(arguments, 0), function (a) {
                if (j.ag(a)) hb.apply(f, a);
                else a && a.p()
            })
        }
        j.p = hb;
        j.td = function (a, b) {
            var c = j.ad(a);
            if (b & 1) {
                j.P(a, (j.I(c) - j.I(a)) / 2);
                j.Rd(a, f)
            }
            if (b & 2) {
                j.O(a, (j.F(c) - j.F(a)) / 2);
                j.ud(a, f)
            }
        };
        var R = {
            i: f,
            v: f,
            z: f,
            j: f,
            T: f,
            R: f
        };
        j.Fe = function (a) {
            var b = j.Mc();
            s(b, {
                Fd: "block",
                wb: j.Kb(a),
                i: 0,
                j: 0,
                T: 0,
                R: 0
            });
            var d = j.md(a, R);
            j.pb(b, a);
            j.Z(b, a);
            var e = j.md(a, R),
                c = {};
            l(d, function (b, a) {
                if (b == e[a]) c[a] = b
            });
            s(b, R);
            s(b, c);
            s(a, {
                i: 0,
                j: 0
            });
            return c
        };
        j.ve = function (b, a) {
            return parseInt(b, a || 10)
        };
        j.eg = q;
        j.Ud = function (b, a) {
            var c = i.body;
            while (a && b !== a && c !== a) a = a.parentNode;
            return b === a
        };

        function T(d, c, b) {
            var a = d.cloneNode(!c);
            !b && j.jg(a, "id");
            return a
        }
        j.pc = T;
        j.yb = function (e, f) {
            var a = new Image;

            function b(e, d) {
                j.Hb(a, "load", b);
                j.Hb(a, "abort", c);
                j.Hb(a, "error", c);
                f && f(a, d)
            }

            function c(a) {
                b(a, d)
            }
            if (ub() && n < 11.6 || !e) b(!e);
            else {
                j.hb(a, "load", b);
                j.hb(a, "abort", c);
                j.hb(a, "error", c);
                a.src = e
            }
        };
        j.Ge = function (e, a, d) {
            var b = 1;

            function c(c) {
                b--;
                if (a && c && c.src == a.src) a = c;
                !b && d && d(a)
            }
            l(e, function (a) {
                if (a.src) {
                    b++;
                    j.yb(a.src, c)
                }
            });
            c()
        };
        j.Ee = function (a, g, i, h) {
            if (h) a = T(a);
            var c = S(a, g);
            if (!c.length) c = b.Xf(a, g);
            for (var f = c.length - 1; f > -1; f--) {
                var d = c[f],
                    e = T(i);
                x(e, x(d));
                b.zd(e, d.style.cssText);
                b.pb(e, d);
                b.Pb(d)
            }
            return a
        };

        function Cb() {
            var a = this;
            b.ab(a, o);
            var d, q = "",
                s = ["av", "pv", "ds", "dn"],
                f = [],
                r, n = 0,
                k = 0,
                h = 0;

            function m() {
                J(d, r, (f[h || k & 2 || k] || "") + " " + (f[n] || ""));
                j.kc(d, h ? "none" : "")
            }

            function c() {
                n = 0;
                a.M(g, "mouseup", c);
                a.M(i, "mouseup", c);
                a.M(i, "touchend", c);
                a.M(i, "touchcancel", c);
                a.M(g, "blur", c);
                m()
            }

            function p(b) {
                if (h) j.Sb(b);
                else {
                    n = 4;
                    m();
                    a.a(g, "mouseup", c);
                    a.a(i, "mouseup", c);
                    a.a(i, "touchend", c);
                    a.a(i, "touchcancel", c);
                    a.a(g, "blur", c)
                }
            }
            a.Me = function (a) {
                if (a === e) return k;
                k = a & 2 || a & 1;
                m()
            };
            a.Jc = function (a) {
                if (a === e) return !h;
                h = a ? 0 : 3;
                m()
            };
            a.s = function (e) {
                a.Q = d = j.Rb(e);
                E(d, "data-jssor-button", "1");
                var c = b.gg(x(d));
                if (c) q = c.shift();
                l(s, function (a) {
                    f.push(q + a)
                });
                r = Y(" ", f);
                f.unshift("");
                a.a(d, "mousedown", p);
                a.a(d, "touchstart", p)
            };
            b.s(a)
        }
        j.Lc = function (a) {
            return new Cb(a)
        };
        j.E = w;
        m("backgroundColor");
        j.Pc = m("overflow");
        j.kc = m("pointerEvents");
        j.O = m("top", 2);
        j.Rd = m("right", 2);
        j.ud = m("bottom", 2);
        j.P = m("left", 2);
        j.I = m("width", 2);
        j.F = m("height", 2);
        m("marginLeft", 2);
        m("marginTop", 2);
        j.Kb = m("position");
        j.qb = m("display");
        j.L = m("zIndex", 1);
        j.Be = function (b, a, c) {
            if (a !== e) Bb(b, a, c);
            else return zb(b)
        };
        j.zd = function (a, b) {
            if (b != e) a.style.cssText = b;
            else return a.style.cssText
        };
        j.Ae = function (b, a) {
            if (a === e) {
                a = w(b, "backgroundImage") || "";
                var c = /\burl\s*\(\s*["']?([^"'\r\n,]+)["']?\s*\)/gi.exec(a) || [];
                return c[1]
            }
            w(b, "backgroundImage", a ? "url('" + a + "')" : "")
        };
        var K;
        j.we = K = {
            S: j.Be,
            i: j.O,
            v: j.Rd,
            z: j.ud,
            j: j.P,
            T: j.I,
            R: j.F,
            wb: j.Kb,
            Fd: j.qb,
            Eb: j.L
        };
        j.md = function (c, b) {
            var a = {};
            l(b, function (d, b) {
                if (K[b]) a[b] = K[b](c)
            });
            return a
        };

        function s(b, h) {
            var a = I(),
                d = mb(),
                g = L(b);

            function c(l, a) {
                a = a || {};
                var h = a.gb || 0,
                    i = (a.mb || 0) % 360,
                    j = (a.fb || 0) % 360,
                    k = (a.bb || 0) % 360,
                    c = a.V,
                    d = a.U,
                    f = a.Ng;
                if (c == e) c = 1;
                if (d == e) d = 1;
                if (f == e) f = 1;
                var b = new yb(a.zb, a.Jb, h);
                b.Oc(c, d, f);
                b.le(a.bc, a.nc);
                b.mb(i);
                b.fb(j);
                b.Le(k);
                b.mc(a.J, a.A);
                l.style[g] = b.re()
            }
            s = function (g, b) {
                b = b || {};
                var i = b.J,
                    k = b.A,
                    h;
                l(K, function (a, c) {
                    h = b[c];
                    h !== e && a(g, h)
                });
                j.kg(g, b.f);
                j.lg(g, b.rb);
                if (!a) {
                    i != e && j.P(g, (b.Kd || 0) + i);
                    k != e && j.O(g, (b.Vd || 0) + k)
                }
                if (b.Ie)
                    if (d) jb(j.db(f, M, g, b));
                    else if (a) c(g, b);
                else M(g, b)
            };
            j.Y = s;
            s(b, h)
        }
        j.Y = s;

        function yb(j, k, n) {
            var d = this,
                b = [1, 0, 0, 0, 0, 1, 0, 0, 0, 0, 1, 0, j || 0, k || 0, n || 0, 1],
                i = c.sb,
                h = c.oc,
                l = c.pd;

            function g(a) {
                return a * c.B / 180
            }

            function m(c, e, l, m, o, r, t, u, w, z, A, C, E, b, f, k, a, g, i, n, p, q, s, v, x, y, B, D, F, d, h, j) {
                return [c * a + e * p + l * x + m * F, c * g + e * q + l * y + m * d, c * i + e * s + l * B + m * h, c * n + e * v + l * D + m * j, o * a + r * p + t * x + u * F, o * g + r * q + t * y + u * d, o * i + r * s + t * B + u * h, o * n + r * v + t * D + u * j, w * a + z * p + A * x + C * F, w * g + z * q + A * y + C * d, w * i + z * s + A * B + C * h, w * n + z * v + A * D + C * j, E * a + b * p + f * x + k * F, E * g + b * q + f * y + k * d, E * i + b * s + f * B + k * h, E * n + b * v + f * D + k * j]
            }

            function e(c, a) {
                return m.apply(f, (a || b).concat(c))
            }
            d.Oc = function (a, c, d) {
                if (a != 1 || c != 1 || d != 1) b = e([a, 0, 0, 0, 0, c, 0, 0, 0, 0, d, 0, 0, 0, 0, 1])
            };
            d.mc = function (a, c, d) {
                b[12] += a || 0;
                b[13] += c || 0;
                b[14] += d || 0
            };
            d.mb = function (c) {
                if (c) {
                    a = g(c);
                    var d = h(a),
                        f = i(a);
                    b = e([1, 0, 0, 0, 0, d, f, 0, 0, -f, d, 0, 0, 0, 0, 1])
                }
            };
            d.fb = function (c) {
                if (c) {
                    a = g(c);
                    var d = h(a),
                        f = i(a);
                    b = e([d, 0, -f, 0, 0, 1, 0, 0, f, 0, d, 0, 0, 0, 0, 1])
                }
            };
            d.Le = function (c) {
                if (c) {
                    a = g(c);
                    var d = h(a),
                        f = i(a);
                    b = e([d, f, 0, 0, -f, d, 0, 0, 0, 0, 1, 0, 0, 0, 0, 1])
                }
            };
            d.le = function (a, c) {
                if (a || c) {
                    j = g(a);
                    k = g(c);
                    b = e([1, l(k), 0, 0, l(j), 1, 0, 0, 0, 0, 1, 0, 0, 0, 0, 1])
                }
            };
            d.re = function () {
                return "matrix3d(" + b.join(",") + ")"
            }
        }
        var O = {
            Kd: 0,
            Vd: 0,
            J: 0,
            A: 0,
            Fb: 1,
            V: 1,
            U: 1,
            bb: 0,
            mb: 0,
            fb: 0,
            zb: 0,
            Jb: 0,
            gb: 0,
            bc: 0,
            nc: 0
        };
        j.ge = function (c, d) {
            var a = c || {};
            if (c)
                if (b.Ed(c)) a = {
                    K: a
                };
                else if (b.Ed(c.f)) a.f = {
                K: c.f
            };
            a.K = a.K || d;
            if (a.f) a.f.K = a.f.K || d;
            if (a.rb) a.rb.K = a.rb.K || d;
            return a
        };

        function ib(c, a) {
            var b = {};
            l(c, function (c, d) {
                var f = c;
                if (a[d] != e)
                    if (j.Ic(c)) f = c + a[d];
                    else f = ib(c, a[d]);
                b[d] = f
            });
            return b
        }
        j.pe = ib;
        j.me = function (o, j, s, t, E, F, p) {
            var a = j;
            if (o) {
                a = {};
                for (var i in j) {
                    var G = F[i] || 1,
                        B = E[i] || [0, 1],
                        g = (s - B[0]) / B[1];
                    g = c.u(c.C(g, 0), 1);
                    g = g * G;
                    var y = c.cb(g);
                    if (g != y) g -= y;
                    var k = t.K || h.ac,
                        m, C = o[i],
                        r = j[i];
                    if (b.Ic(r)) {
                        k = t[i] || k;
                        var D = k(g);
                        m = C + r * D
                    } else {
                        m = z({
                            qc: {}
                        }, o[i]);
                        var A = t[i] || {};
                        l(r.qc || r, function (d, a) {
                            k = A[a] || A.K || k;
                            var c = k(g),
                                b = d * c;
                            m.qc[a] = b;
                            m[a] += b
                        })
                    }
                    a[i] = m
                }
                var x = l(j, function (b, a) {
                    return O[a] != e
                });
                x && l(O, function (c, b) {
                    if (a[b] == e && o[b] !== e) a[b] = o[b]
                });
                if (x) {
                    if (a.Fb) a.V = a.U = a.Fb;
                    a.Db = p.Db;
                    a.Cb = p.Cb;
                    if (v() && n >= 11 && (j.J || j.A) && s != 0 && s != 1) a.bb = a.bb || 1e-8;
                    a.Ie = d
                }
            }
            if (j.f && p.mc) {
                var q = a.f.qc,
                    w = (q.i || 0) + (q.z || 0),
                    u = (q.j || 0) + (q.v || 0);
                a.j = (a.j || 0) + u;
                a.i = (a.i || 0) + w;
                a.f.j -= u;
                a.f.v -= u;
                a.f.i -= w;
                a.f.z -= w
            }
            if (a.f && !a.f.i && !a.f.j && !a.f.A && !a.f.J && a.f.v == p.Db && a.f.z == p.Cb) a.f = f;
            return a
        }
    };

    function o() {
        var a = this,
            f, e = [],
            c = [];

        function k(a, b) {
            e.push({
                Bb: a,
                Ab: b
            })
        }

        function j(a, c) {
            b.g(e, function (b, d) {
                b.Bb == a && b.Ab === c && e.splice(d, 1)
            })
        }

        function i() {
            e = []
        }

        function h() {
            b.g(c, function (a) {
                b.Hb(a.gd, a.Bb, a.Ab, a.hd)
            });
            c = []
        }
        a.Wc = function () {
            return f
        };
        a.a = function (e, a, d, f) {
            b.hb(e, a, d, f);
            c.push({
                gd: e,
                Bb: a,
                Ab: d,
                hd: f
            })
        };
        a.M = function (e, a, d, f) {
            b.g(c, function (g, h) {
                if (g.gd === e && g.Bb == a && g.Ab === d && g.hd == f) {
                    b.Hb(e, a, d, f);
                    c.splice(h, 1)
                }
            })
        };
        a.kd = h;
        a.Sc = a.addEventListener = k;
        a.removeEventListener = j;
        a.k = function (a) {
            var c = [].slice.call(arguments, 1);
            b.g(e, function (b) {
                b.Bb == a && b.Ab.apply(g, c)
            })
        };
        a.p = function () {
            if (!f) {
                f = d;
                h();
                i()
            }
        }
    }
    var l = function (C, D, i, m, R, Q) {
        C = C || 0;
        var a = this,
            p, n, o, s, F = 0,
            O = 1,
            L, M, K, G, B = 0,
            j = 0,
            r = 0,
            A, l, f, h, q, z, u = [],
            y, I = k,
            J, H = k;

        function T(a) {
            f += a;
            h += a;
            l += a;
            j += a;
            r += a;
            B += a
        }

        function x(C) {
            var k = C;
            if (q)
                if (!z && (k >= h || k < f) || z && k >= f) k = ((k - f) % q + q) % q + f;
            if (!A || s || j != k) {
                var g = c.u(k, h);
                g = c.C(g, f);
                if (i.Rc) g = h - g + f;
                if (!A || s || g != r) {
                    if (Q) {
                        var x = (g - l) / (D || 1),
                            o = b.me(R, Q, x, L, K, M, i);
                        if (y) b.g(o, function (b, a) {
                            y[a] && y[a](m, b)
                        });
                        else b.Y(m, o);
                        var n;
                        if (J) {
                            var p = g > f && g < h;
                            if (p != H) n = H = p
                        }
                        if (!n && o.S != e) {
                            var t = o.S < .001;
                            if (t != I) n = I = t
                        }
                        if (n != e) {
                            n && b.kc(m, "none");
                            !n && b.kc(m, b.l(m, "data-events"))
                        }
                    }
                    var w = r,
                        v = r = g;
                    b.g(u, function (b, c) {
                        var a = !A && z || k <= j ? u[u.length - c - 1] : b;
                        a.D(g - B)
                    });
                    j = k;
                    A = d;
                    a.Zc(w - l, v - l);
                    a.Ib(w, v)
                }
            }
        }

        function E(a, b, d) {
            b && a.Xb(h);
            if (!d) {
                f = c.u(f, a.Wb() + B);
                h = c.C(h, a.kb() + B)
            }
            u.push(a)
        }
        var v = g.requestAnimationFrame || g.webkitRequestAnimationFrame || g.mozRequestAnimationFrame || g.msRequestAnimationFrame;
        if (b.Bg() && b.Xc() < 7 || !v) v = function (a) {
            b.hc(a, i.rd)
        };

        function N() {
            if (p) {
                var d = b.Qb(),
                    e = c.u(d - F, i.qd),
                    a = j + e * o * O;
                F = d;
                if (a * o >= n * o) a = n;
                x(a);
                if (!s && a * o >= n * o) P(G);
                else v(N)
            }
        }

        function w(e, g, i) {
            if (!p) {
                p = d;
                s = i;
                G = g;
                e = c.C(e, f);
                e = c.u(e, h);
                n = e;
                o = n < j ? -1 : 1;
                a.Tc();
                F = b.Qb();
                v(N)
            }
        }

        function P(b) {
            if (p) {
                s = p = G = k;
                a.Uc();
                b && b()
            }
        }
        a.Dd = function (a, b, c) {
            w(a ? j + a : h, b, c)
        };
        a.Ub = w;
        a.De = function (a, b) {
            w(h, a, b)
        };
        a.H = P;
        a.Cd = function () {
            return j
        };
        a.Bd = function () {
            return n
        };
        a.m = function () {
            return r
        };
        a.D = x;
        a.se = function () {
            x(h, d)
        };
        a.cd = function () {
            return p
        };
        a.ae = function (a) {
            O = a
        };
        a.Xb = T;
        a.N = function (a, b) {
            E(a, 0, b)
        };
        a.rc = function (a) {
            E(a, 1)
        };
        a.fd = function (a) {
            h += a
        };
        a.Wb = function () {
            return f
        };
        a.kb = function () {
            return h
        };
        a.Ib = a.Tc = a.Uc = a.Zc = b.Ne;
        a.Yc = b.Qb();
        i = b.jb({
            rd: 16,
            qd: 50
        }, i);
        m && (J = b.l(m, "data-inactive"));
        q = i.ec;
        z = i.ze;
        y = i.Je;
        f = l = C;
        h = C + D;
        M = i.G || {};
        K = i.ye || {};
        L = b.ge(i.ib)
    };
    var n = {
            ke: "data-scale",
            Nb: "data-autocenter",
            uc: "data-nofreeze",
            Od: "data-nodrag"
        },
        q = new function () {
            var a = this;
            a.sc = function (c, a, e, d) {
                (d || !b.l(c, a)) && b.l(c, a, e)
            };
            a.tc = function (a) {
                var c = b.n(a, n.Nb);
                b.td(a, c)
            }
        };
    new(function () {});
    var r = {
            Bc: 1
        },
        u = function () {
            var a = this,
                D = b.ab(a, o),
                h, v, C, B, m, l = 0,
                g, s, p, z, A, i, k, u, t, x, j;

            function y(a) {
                j[a] && j[a].Me(a == l)
            }

            function w(b) {
                a.k(r.Bc, b * s)
            }
            a.Cc = function (a) {
                if (a != m) {
                    var d = l,
                        b = c.cb(a / s);
                    l = b;
                    m = a;
                    y(d);
                    y(b)
                }
            };
            a.Nc = function (a) {
                b.Tb(h, a)
            };
            a.Hc = function (I) {
                b.p(j);
                m = e;
                a.kd();
                x = [];
                j = [];
                b.Vb(h);
                v = c.ub(I / s);
                l = 0;
                var H = u + z,
                    y = t + A,
                    r = c.ub(v / p) - 1;
                C = u + H * (!i ? r : p - 1);
                B = t + y * (i ? r : p - 1);
                b.I(h, C);
                b.F(h, B);
                for (var n = 0; n < v; n++) {
                    var D = b.cg();
                    b.Pf(D, n + 1);
                    var o = b.Ee(k, "numbertemplate", D, d);
                    b.Kb(o, "absolute");
                    var G = n % (r + 1),
                        E = c.cb(n / (r + 1)),
                        F = g.Yb && !i ? r - G : G;
                    b.P(o, (!i ? F : E) * H);
                    b.O(o, (i ? F : E) * y);
                    b.Z(h, o);
                    x[n] = o;
                    g.Ec & 1 && a.a(o, "click", b.db(f, w, n));
                    g.Ec & 2 && a.a(o, "mouseenter", b.db(f, w, n));
                    j[n] = b.Lc(o)
                }
                q.tc(h)
            };
            a.s = function (d, c) {
                a.Q = h = b.Rb(d);
                a.Gc = g = b.jb({
                    Pd: 10,
                    Qd: 10,
                    Ec: 1
                }, c);
                k = b.be(h, "prototype");
                u = b.I(k);
                t = b.F(k);
                b.Pb(k, h);
                s = g.Hd || 1;
                p = g.Td || 1;
                z = g.Pd;
                A = g.Qd;
                i = g.Ce & 2;
                g.lb && q.sc(h, n.Nb, g.lb)
            };
            a.p = function () {
                b.p(j, D)
            };
            b.s(a)
        },
        v = function () {
            var a = this,
                v = b.ab(a, o),
                e, c, g, l, s, k, h, m, j, i;

            function p(b) {
                a.k(r.Bc, b, d)
            }

            function u(a) {
                b.Tb(e, a);
                b.Tb(c, a)
            }

            function t() {
                j.Jc((g.Mb || !l.ne(h)) && k > 1);
                i.Jc((g.Mb || !l.oe(h)) && k > 1)
            }
            a.Cc = function (c, a, b) {
                h = a;
                !b && t()
            };
            a.Nc = u;
            a.Hc = function (g) {
                k = g;
                h = 0;
                if (!s) {
                    a.a(e, "click", b.db(f, p, -m));
                    a.a(c, "click", b.db(f, p, m));
                    j = b.Lc(e);
                    i = b.Lc(c);
                    b.l(e, n.uc, 1);
                    b.l(c, n.uc, 1);
                    s = d
                }
            };
            a.s = function (f, d, h, i) {
                a.Gc = g = b.jb({
                    Hd: 1
                }, h);
                e = f;
                c = d;
                if (g.Yb) {
                    e = d;
                    c = f
                }
                m = g.Hd;
                l = i;
                if (g.lb) {
                    q.sc(e, n.Nb, g.lb);
                    q.sc(c, n.Nb, g.lb)
                }
                q.tc(e);
                q.tc(c)
            };
            a.p = function () {
                b.p(j, i, v)
            };
            b.s(a)
        };

    function p(e, d, c) {
        var a = this;
        b.ab(a, o);
        l.call(a, 0, c.ob);
        a.gc = 0;
        a.Dc = c.ob
    }
    p.Fc = 21;
    p.ic = 24;
    var s = function () {
            var a = this,
                hb = b.ab(a, o);
            l.call(a, 0, 0);
            var e, t, gb = [h.ac, h.Qe, h.Pe, h.Xd, h.nf, h.of, h.Kf, h.Jf, h.If, h.Hf, h.Lf, h.og, h.hf, h.pf, h.qf, h.rf, h.sf, h.tf, h.uf, h.vf, h.wf, h.xf, h.yf, h.Af, h.Bf, h.Cf, h.Df, h.Ef, h.Ff, h.sd, h.dd, h.Gf, h.mf, h.lf],
                P = {},
                S, C, u = new l(0, 0),
                T = [],
                x = [],
                E, q = 0;

            function G(d, c) {
                var a = {};
                b.g(d, function (d, f) {
                    var e = P[f];
                    if (e) {
                        if (b.af(d)) d = G(d, c || f == "e");
                        else if (c)
                            if (b.Ic(d)) d = gb[d];
                        a[e] = d
                    }
                });
                return a
            }

            function I(c, f) {
                var e = [],
                    d = b.n(c, "play");
                if (f && d) {
                    var g = new s(c, t, {
                        ig: d
                    });
                    N.push(g);
                    a.a(g, p.Fc, Z);
                    a.a(g, p.ic, U)
                } else b.g(b.Gb(c), function (a) {
                    e = e.concat(I(a, f + 1))
                });
                if (!f && (!j || j & 16) || f && (!d || !(d & 16))) {
                    var h = S[b.n(c, "t")];
                    h && e.push({
                        Q: c,
                        xc: h
                    })
                }
                return e
            }

            function O(c, e) {
                var a = T[c];
                if (a == f) {
                    a = T[c] = {
                        nb: c,
                        vc: [],
                        Jd: []
                    };
                    var d = 0;
                    !b.g(x, function (a, b) {
                        d = b;
                        return a.nb > c
                    }) && d++;
                    x.splice(d, 0, a)
                }
                return a
            }

            function db(o, p, g) {
                var a, e;
                if (C) {
                    var k = C[b.n(o, "c")];
                    if (k) {
                        a = O(k.r, 0);
                        a.bg = k.e || 0
                    }
                }
                b.g(p, function (h) {
                    var f = b.jb(d, {}, G(h)),
                        i = b.ge(f.ib);
                    delete f.ib;
                    if (f.j) {
                        f.J = f.j;
                        i.J = i.j;
                        delete f.j
                    }
                    if (f.i) {
                        f.A = f.i;
                        i.A = i.i;
                        delete f.i
                    }
                    var m = {
                            ib: i,
                            Db: g.T,
                            Cb: g.R
                        },
                        j = new l(h.b, h.d, m, o, g, f);
                    q = c.C(q, h.b + h.d);
                    if (a) {
                        if (!e) e = new l(h.b, 0);
                        e.N(j)
                    } else {
                        var k = O(h.b, h.b + h.d);
                        k.vc.push(j)
                    }
                    if (f.rb) g.rb = {
                        yd: 0,
                        xd: 0,
                        wd: 0,
                        S: 0
                    };
                    g = b.pe(g, f)
                });
                if (a && e) {
                    e.se();
                    var h = e,
                        i, j = e.Wb(),
                        m = e.kb(),
                        n = c.C(m, a.bg);
                    if (a.nb < m) {
                        if (a.nb > j) {
                            h = new l(j, a.nb - j);
                            h.N(e, d)
                        } else h = f;
                        i = new l(a.nb, n - j, {
                            ec: n - a.nb,
                            ze: d
                        });
                        i.N(e, d)
                    }
                    h && a.vc.push(h);
                    i && a.Jd.push(i)
                }
                return g
            }

            function cb(a) {
                b.g(a, function (d) {
                    var a = d.Q,
                        f = b.I(a),
                        e = b.F(a),
                        c = {
                            j: b.P(a),
                            i: b.O(a),
                            J: 0,
                            A: 0,
                            S: 1,
                            Eb: b.L(a) || 0,
                            bb: 0,
                            mb: 0,
                            fb: 0,
                            V: 1,
                            U: 1,
                            zb: 0,
                            Jb: 0,
                            gb: 0,
                            bc: 0,
                            nc: 0,
                            T: f,
                            R: e,
                            f: {
                                i: 0,
                                v: f,
                                z: e,
                                j: 0
                            }
                        };
                    c.Kd = c.j;
                    c.Vd = c.i;
                    db(a, d.xc, c)
                })
            }

            function fb(f, e, g) {
                var c = f.b - e;
                if (c) {
                    var b = new l(e, c);
                    b.N(u, d);
                    b.Xb(g);
                    a.N(b)
                }
                a.fd(f.d);
                return c
            }

            function eb(e) {
                var c = u.Wb(),
                    d = 0;
                b.g(e, function (e, f) {
                    e = b.jb({
                        d: 3e3
                    }, e);
                    fb(e, c, d);
                    c = e.b;
                    d += e.d;
                    if (!f || e.t == 2) {
                        a.gc = c;
                        a.Dc = c + e.d
                    }
                })
            }

            function B(g, e, d) {
                var f = e.length;
                if (f > 4)
                    for (var j = c.ub(f / 4), a = 0; a < j; a++) {
                        var h = e.slice(a * 4, c.u(a * 4 + 4, f)),
                            i = new l(h[0].nb, 0);
                        B(i, h, d);
                        g.N(i)
                    } else b.g(e, function (a) {
                        b.g(d ? a.Jd : a.vc, function (a) {
                            d && a.fd(q - a.kb());
                            g.N(a)
                        })
                    })
            }
            var j, F, v = 0,
                g, z, K, J, A, N = [],
                H = [],
                r, D, m;

            function y(a) {
                return a & 2 || a & 4 && b.Vc().bd
            }

            function ab() {
                if (!A) {
                    g & 8 && a.a(i, "keydown", Q);
                    if (g & 32) {
                        a.a(i, "mousedown", w);
                        a.a(i, "touchstart", w)
                    }
                    A = d
                }
            }

            function Y() {
                a.M(i, "keydown", Q);
                a.M(i, "mousedown", w);
                a.M(i, "touchstart", w);
                A = k
            }

            function L(b) {
                if (!r || b) {
                    r = d;
                    a.H();
                    b && v && a.D(0);
                    a.ae(1);
                    a.De();
                    ab();
                    a.k(p.Fc, a)
                }
            }

            function n() {
                if (!D && (r || a.m())) {
                    D = d;
                    a.H();
                    a.Cd() > a.gc && a.D(a.gc);
                    a.ae(K || 1);
                    a.Ub(0)
                }
            }

            function V() {
                !m && n()
            }

            function M(c) {
                var b = c;
                if (c < 0 && a.m()) b = 1;
                if (b != v) {
                    v = b;
                    F && a.k(p.ic, a, v)
                }
            }

            function Q(a) {
                g & 8 && b.Md(a) == 27 && n()
            }

            function X(a) {
                if (m && b.Nd(a) !== f) {
                    m = k;
                    g & 16 && b.hc(V, 160)
                }
            }

            function w(a) {
                g & 32 && !b.Ud(e, b.Lb(a)) && n()
            }

            function W(a) {
                if (!m) {
                    m = d;
                    if (j & 1) b.Id(a, e) && L()
                }
            }

            function bb(i) {
                var h = b.Lb(i),
                    a = b.eb(h, f, f, "A"),
                    c = a && (b.Yf(a) || a === e || b.Ud(e, a));
                if (r && y(g)) !c && n();
                else if (y(j)) !c && L(d)
            }

            function Z(b) {
                var c = b.Zf(),
                    a = H[c];
                a !== b && a && a.Wf();
                H[c] = b
            }

            function U(b, c) {
                a.k(p.ic, b, c)
            }
            a.Zf = function () {
                return J || ""
            };
            a.Wf = n;
            a.Tc = function () {
                M(1)
            };
            a.Uc = function () {
                r = k;
                D = k;
                M(-1);
                !a.m() && Y()
            };
            a.Ib = function () {
                !m && z && a.Cd() > a.Dc && n()
            };
            a.s = function (m, i, f) {
                e = m;
                t = i;
                j = f.ig;
                E = f.Uf;
                S = t.yc;
                C = t.Kg;
                var l = {
                    i: "y",
                    j: "x",
                    z: "m",
                    v: "t",
                    bb: "r",
                    mb: "rX",
                    fb: "rY",
                    V: "sX",
                    U: "sY",
                    zb: "tX",
                    Jb: "tY",
                    gb: "tZ",
                    bc: "kX",
                    nc: "kY",
                    S: "o",
                    ib: "e",
                    Eb: "i",
                    f: "c",
                    rb: "bc",
                    yd: "re",
                    xd: "gr",
                    wd: "bl"
                };
                b.g(l, function (b, a) {
                    P[b] = a
                });
                cb(I(e, 0));
                B(u, x);
                if (j) {
                    a.N(u);
                    E = d;
                    z = b.n(e, "idle");
                    g = b.n(e, "rollback");
                    K = b.n(e, "speed", 1);
                    J = b.W(e, "group");
                    (y(j) || y(g)) && a.a(e, "click", bb);
                    if ((j & 1 || z) && !b.Vc().bd) {
                        a.a(e, "mouseenter", W);
                        a.a(e, "mouseleave", X)
                    }
                    F = b.n(e, "pause")
                }
                var k = t.Sf || [],
                    c = k[b.n(e, "b")] || [],
                    h = {
                        b: q,
                        d: c.length ? 0 : f.ob || z || 0
                    };
                c = c.concat([h]);
                eb(c);
                a.kb();
                E && a.fd(1e8);
                q = a.kb();
                B(a, x, d);
                a.D(-1);
                a.D(b.n(e, "initial") || 0)
            };
            a.p = function () {
                b.p(hb, N);
                a.H();
                a.D(-1)
            };
            b.s(a)
        },
        j = (g.module || {}).exports = function () {
            var a = this,
                xc = b.ab(a, o),
                Kb = "data-jssor-slider",
                Cc = "data-jssor-thumb",
                t, m, R, Hb, cb, tb, Z, M, K, P, Ub, zc = 1,
                qc = 1,
                Gc = 1,
                hc = 1,
                cc = {},
                w, U, Vb, Zb, Yb, Ib, Gb, Fb, gb, E = [],
                fc, u = -1,
                jc, q, I, H, L, kb, lb, F, J, hb, S, A, W, jb, Y = [],
                lc, nc, dc, s, sb, Cb, nb, eb, X, ic, Bb, Mb, Nb, G, ac = 0,
                bb = 0,
                Q = Number.MAX_VALUE,
                N = Number.MIN_VALUE,
                C, ib, db, T = 1,
                Sb = 0,
                mb, B, Ab, zb, O, xb, yb, z, V, ob, y, Jb, Xb = b.Vc(),
                Qb = Xb.bd,
                x = [],
                D, ub, ab, bc, Ac, Ic, vb;

            function Eb() {
                return !T && X & 12
            }

            function Bc() {
                return Sb || !T && X & 3
            }

            function Db() {
                return !B && !Eb() && !y.cd()
            }

            function Rc() {
                return !Bc() && Db()
            }

            function Ec() {
                return A || R
            }

            function Kc() {
                return Ec() & 2 ? lb : kb
            }

            function Hc(a, c, d) {
                b.P(a, c);
                b.O(a, d)
            }

            function vc(c, b) {
                var a = Ec(),
                    d = (kb * b + ac) * (a & 1),
                    e = (lb * b + ac) * (a & 2) / 2;
                Hc(c, d, e)
            }

            function sc(b, f) {
                if (B && !(C & 1)) {
                    var e = b,
                        d;
                    if (b < N) {
                        e = N;
                        d = -1
                    }
                    if (b > Q) {
                        e = Q;
                        d = 1
                    }
                    if (d) {
                        var a = b - e;
                        if (f) {
                            a = c.Te(a) * 2 / c.B;
                            a = c.q(a * d, 1.6)
                        } else {
                            a = c.q(a * d, .625);
                            a = c.pd(a * c.B / 2)
                        }
                        b = e + a * d
                    }
                }
                return b
            }

            function Mc(a) {
                return sc(a, d)
            }

            function dd(a) {
                return sc(a)
            }

            function wb(a, b) {
                if (!(C & 1)) {
                    var c = a - Q + (b || 0),
                        d = N - a + (b || 0);
                    if (c > 0 && c > d) a = Q;
                    else if (d > 0) a = N
                }
                return a
            }

            function oc(a) {
                return !(C & 1) && a - N < .0001
            }

            function mc(a) {
                return !(C & 1) && Q - a < .0001
            }

            function pb(a) {
                return !(C & 1) && (a - N < .0001 || Q - a < .0001)
            }

            function Ob(c, a, d) {
                !vb && b.g(Y, function (b) {
                    b.Cc(c, a, d)
                })
            }

            function uc(b) {
                var a = b,
                    d = pb(b);
                if (d) a = wb(a);
                else {
                    b = v(b);
                    a = b
                }
                a = c.cb(a);
                a = c.C(a, 0);
                return a
            }

            function ad(a) {
                x[u];
                fc = u;
                u = a;
                jc = x[u]
            }

            function Pc() {
                A = 0;
                var b = z.m(),
                    d = uc(b);
                Ob(d, b);
                if (pb(b) || b == c.cb(b)) {
                    if (s & 2 && (eb > 0 && d == q - 1 || eb < 0 && !d)) s = 0;
                    ad(d);
                    a.k(j.Qf, u, fc)
                }
            }

            function ec(a, b) {
                if (q && (!b || !y.cd())) {
                    y.H();
                    y.wc(a, a)
                }
            }

            function rb(a) {
                if (q) {
                    a = v(a);
                    a = wb(a);
                    ec(a)
                } else Ob(0, 0)
            }

            function Uc() {
                var b = j.Zd || 0,
                    a = ib;
                j.Zd |= a;
                return W = a & ~b
            }

            function Qc() {
                if (W) {
                    j.Zd &= ~ib;
                    W = 0
                }
            }

            function Tb(c) {
                var a = b.Mc();
                b.Y(a, gb);
                c && b.Pc(a, "hidden");
                return a
            }

            function v(b, a) {
                a = a || q || 1;
                return (b % a + a) % a
            }

            function wc(c, a, b) {
                s & 8 && (s = 0);
                qb(c, Bb, a, b)
            }

            function Pb() {
                b.g(Y, function (a) {
                    a.Nc(a.Gc.Lg <= T)
                })
            }

            function cd(c) {
                if (!T && (b.Nd(c) || !b.Id(c, t))) {
                    T = 1;
                    Pb();
                    if (!B) {
                        X & 12 && Dc();
                        x[u] && x[u].Zb()
                    }
                    a.k(j.zg)
                }
            }

            function bd() {
                if (T) {
                    T = 0;
                    Pb();
                    B || !(X & 12) || Fc()
                }
                a.k(j.Hg)
            }

            function Jc() {
                b.Y(U, gb)
            }

            function Rb(b, a) {
                qb(b, a, d)
            }

            function qb(g, h, l, p) {
                if (q && (!B || m.Ld) && !Eb() && !isNaN(g)) {
                    var f = z.m(),
                        a = g;
                    if (l) {
                        a = f + g;
                        if (C & 2) {
                            if (oc(f) && g < 0) a = Q;
                            if (mc(f) && g > 0) a = N
                        }
                    }
                    if (!(C & 1))
                        if (p) a = v(a);
                        else a = wb(a, .5);
                    if (l && !pb(a)) a = c.G(a);
                    var j = (a - f) % q;
                    a = f + j;
                    if (h == e) h = Bb;
                    var b = c.tb(j),
                        i = 0;
                    if (b) {
                        if (b < 1) b = c.q(b, .5);
                        if (b > 1) {
                            var o = Kc(),
                                n = (R & 1 ? Gb : Fb) / o;
                            b = c.u(b, n * 1.5)
                        }
                        i = h * b
                    }
                    vb = d;
                    y.H();
                    vb = k;
                    y.wc(f, a, i)
                }
            }

            function Nc(e, h, o) {
                var l = this,
                    i = {
                        i: 2,
                        v: 1,
                        z: 2,
                        j: 1
                    },
                    m = {
                        i: "top",
                        v: "right",
                        z: "bottom",
                        j: "left"
                    },
                    g, a, f, j, k = {};
                l.Q = e;
                l.fc = function (q, l, u) {
                    var p, s = q,
                        r = l;
                    if (!f) {
                        f = b.Fe(e);
                        g = e.parentNode;
                        j = {
                            Oc: b.n(e, n.ke, 1),
                            lb: b.n(e, n.Nb)
                        };
                        b.g(m, function (c, a) {
                            k[a] = b.n(e, "data-scale-" + c, 1)
                        });
                        a = e;
                        if (h) {
                            a = b.pc(g, d);
                            b.Y(a, {
                                i: 0,
                                j: 0
                            });
                            b.Z(a, e);
                            b.Z(g, a)
                        }
                    }
                    if (o) {
                        p = c.C(q, l);
                        if (h)
                            if (u >= 0 && u < 1) {
                                var w = c.u(q, l);
                                p = c.u(p / w, 1 / (1 - u)) * w
                            }
                    } else s = r = p = c.q(K < P ? l : q, j.Oc);
                    var x = h ? 1.001 : 1,
                        t = p * x;
                    h && (hc = t);
                    b.Sd(a, t);
                    b.I(g, f.T * s);
                    b.F(g, f.R * r);
                    var v = b.Yd() && b.Xc() < 9 ? t : 1,
                        y = (s - v) * f.T / 2,
                        z = (r - v) * f.R / 2;
                    b.P(a, y);
                    b.O(a, z);
                    b.g(f, function (d, a) {
                        if (i[a] && d) {
                            var e = (i[a] & 1) * c.q(q, k[a]) * d + (i[a] & 2) * c.q(l, k[a]) * d / 2;
                            b.we[a](g, e)
                        }
                    });
                    b.td(g, j.lb)
                }
            }

            function Yc() {
                var a = this;
                l.call(a, 0, 0, {
                    ec: q
                });
                b.g(x, function (b) {
                    a.rc(b);
                    b.Xb(G / F)
                })
            }

            function Xc() {
                var a = this,
                    b = Jb.Q;
                l.call(a, -1, 2, {
                    ib: h.ac,
                    Je: {
                        wb: vc
                    },
                    ec: q,
                    Rc: Cb
                }, b, {
                    wb: 1
                }, {
                    wb: -2
                })
            }

            function Zc() {
                var b = this;
                l.call(b, -1e8, 2e8);
                b.Ib = function (e, b) {
                    if (c.tb(b - e) > 1e-5) {
                        var g = b,
                            f = b;
                        if (c.cb(b) != b && b > e && (C & 1 || b > bb)) f++;
                        var h = uc(f);
                        Ob(h, g, d);
                        a.k(j.sg, v(g), v(e), b, e)
                    }
                }
            }

            function Oc(o, n) {
                var b = this,
                    g, i, e, c, h;
                l.call(b, -1e8, 2e8, {
                    qd: 100
                });
                b.Tc = function () {
                    mb = d;
                    a.k(j.rg, v(z.m()), V.m())
                };
                b.Uc = function () {
                    mb = k;
                    c = k;
                    a.k(j.yg, v(z.m()), V.m());
                    !B && Pc()
                };
                b.Ib = function (f, b) {
                    var a = b;
                    if (c) a = h;
                    else if (e) {
                        var d = b / e;
                        a = m.Wd(d) * (i - g) + g
                    }
                    a = Mc(a);
                    V.D(a)
                };
                b.wc = function (a, c, h, f) {
                    B = k;
                    e = h || 1;
                    g = a;
                    i = c;
                    vb = d;
                    V.D(a);
                    vb = k;
                    b.D(0);
                    b.Ub(e, f)
                };
                b.qg = function () {
                    c = d;
                    c && b.Dd(f, f, d)
                };
                b.Cg = function (a) {
                    h = a
                };
                V = new Zc;
                V.N(o);
                Nb && V.N(n)
            }

            function Lc() {
                var c = this,
                    a = Tb();
                b.L(a, 0);
                c.Q = a;
                c.jc = function () {
                    b.vb(a);
                    b.Vb(a)
                }
            }

            function Wc(w, h) {
                var g = this,
                    ib = b.ab(g, o),
                    y, G = 0,
                    P, t, F, B, K, i, E = [],
                    V, N, R, n, r, A, S;
                l.call(g, -J, J + 1, {
                    ec: C & 1 ? q : e,
                    Rc: Cb
                });

                function L() {
                    y && y.p();
                    Sb -= G;
                    G = 0;
                    y = new cb.X(t, cb, {
                        ob: b.n(t, "idle", ic),
                        Uf: !s
                    });
                    y.Sc(p.ic, Y)
                }

                function Z() {
                    y.Yc < cb.Yc && L()
                }

                function Y(b, a) {
                    G += a;
                    Sb += a;
                    if (h == u) !G && g.Zb()
                }

                function Q(p, s, o) {
                    if (!N) {
                        N = d;
                        if (i && o) {
                            var q = b.n(i, "data-expand", 0) * 2,
                                f = o.width,
                                e = o.height,
                                m = f,
                                l = e;
                            if (f && e) {
                                if (B) {
                                    if (B & 3 && (!(B & 4) || f > I || e > H)) {
                                        var n = k,
                                            r = I / H * e / f;
                                        if (B & 1) n = r > 1;
                                        else if (B & 2) n = r < 1;
                                        m = n ? f * H / e : I;
                                        l = n ? H : e * I / f
                                    }
                                    b.I(i, m);
                                    b.F(i, l);
                                    b.O(i, (H - l) / 2);
                                    b.P(i, (I - m) / 2)
                                }
                                b.Sd(i, c.C((m + q) / m, (l + q) / l))
                            }
                            b.Kb(i, "absolute")
                        }
                        a.k(j.Eg, h)
                    }
                    s.fe(k);
                    p && p(g)
                }

                function X(f, b, c, e) {
                    if (e == A && u == h && s && Db() && !g.Wc()) {
                        var a = v(f);
                        D.qe(a, h, b, g, c, H / I);
                        b.ng();
                        ob.Xb(a - ob.Wb() - 1);
                        ob.D(a);
                        ec(a, d)
                    }
                }

                function bb(b) {
                    if (b == A && u == h && Db() && !g.Wc()) {
                        if (!n) {
                            var a = f;
                            if (D)
                                if (D.Ob == h) a = D.xe();
                                else D.jc();
                            Z();
                            n = new Vc(w, h, a, y);
                            n.Mf(r)
                        }!n.cd() && n.Qc()
                    }
                }

                function M(a, d, j) {
                    if (a == h) {
                        if (a != d) x[d] && x[d].od();
                        else !j && n && n.zf();
                        r && r.Jc();
                        A = b.Qb();
                        g.yb(b.db(f, bb, A))
                    } else {
                        var i = c.u(h, a),
                            e = c.C(h, a),
                            l = c.u(e - i, i + q - e),
                            k = J + m.kf - 1;
                        (!R || l <= k) && g.yb()
                    }
                }

                function fb() {
                    if (u == h && n) {
                        n.H();
                        r && r.jf();
                        r && r.Nf();
                        n.nd()
                    }
                }

                function hb() {
                    u == h && n && n.H()
                }

                function ab(b) {
                    !db && a.k(j.gf, h, b)
                }
                g.fe = function (a) {
                    if (S != a) {
                        S = a;
                        a && b.Z(w, K);
                        !a && b.Pb(K)
                    }
                };
                g.yb = function (e, c) {
                    c = c || g;
                    if (E.length && !N) {
                        c.fe(d);
                        if (!V) {
                            V = d;
                            a.k(j.ff, h);
                            b.g(E, function (a) {
                                if (!b.l(a, "src")) {
                                    var c = b.W(a, "src") || b.W(a, "src2") || "";
                                    if (c) {
                                        a.src = c;
                                        b.qb(a, b.l(a, "data-display"))
                                    }
                                }
                            })
                        }
                        b.Ge(E, i, b.db(f, Q, e, c))
                    } else Q(e, c)
                };
                g.ef = function () {
                    if (Rc())
                        if (q == 1) {
                            g.od();
                            M(h, h)
                        } else {
                            var a;
                            if (D) a = D.ue(q);
                            if (a) {
                                A = b.Qb();
                                var c = h + eb,
                                    d = x[v(c)];
                                return d.yb(b.db(f, X, c, d, a, A), g)
                            } else(C || !pb(z.m()) || !pb(z.m() + eb)) && Rb(eb)
                        }
                };
                g.Zb = function () {
                    M(h, h, d)
                };
                g.od = function () {
                    r && r.jf();
                    r && r.Nf();
                    g.id();
                    n && n.df();
                    n = f;
                    L()
                };
                g.ng = function () {
                    b.vb(w)
                };
                g.id = function () {
                    b.Tb(w)
                };

                function T(a, k, e) {
                    if (b.l(a, Kb)) return;
                    if (e) {
                        if (!t) {
                            t = a;
                            F = Tb(d);
                            var c = "background";
                            b.E(F, c + "Color", b.E(t, c + "Color"));
                            b.E(F, c + "Image", b.E(t, c + "Image"));
                            b.E(t, c, f);
                            b.pb(F, t)
                        }
                        b.l(a, "data-events", b.kc(a));
                        b.l(a, "data-display", b.qb(a));
                        b.Ag(a, b.W(a, "data-to"));
                        b.Fg(a, b.W(a, "data-bf"));
                        e > 1 && b.cc(a, b.l(a, "data-ts"));
                        b.Gg(a, b.n(a, "data-p"));
                        b.ug(a, b.W(a, "po"));
                        if (a.tagName == "IMG") {
                            E.push(a);
                            if (!b.l(a, "src")) {
                                R = d;
                                b.vb(a)
                            }
                        }
                        var g = b.Ae(a);
                        if (g) {
                            var h = new Image;
                            b.l(h, "src", g);
                            E.push(h)
                        }
                        e && b.L(a, (b.L(a) || 0) + 1)
                    }
                    var j = b.Gb(a);
                    b.g(j, function (c) {
                        if (e < 3 && !i)
                            if (b.W(c, "u") == "image") {
                                i = c;
                                i.border = 0;
                                b.Y(i, gb);
                                b.Y(a, gb);
                                b.E(i, "maxWidth", "10000px");
                                b.Z(F, i)
                            } T(c, k, e + 1)
                    })
                }
                g.Zc = function (c, b) {
                    var a = J - b;
                    vc(P, a)
                };
                g.Ob = h;
                T(w, d, 0);
                B = b.n(t, "data-fillmode", m.cf);
                var O = b.be(t, "thumb", d);
                if (O) {
                    b.pc(O);
                    b.vb(O)
                }
                b.Tb(w);
                K = b.pc(U);
                b.L(K, 1e3);
                g.a(w, "click", ab);
                L(d);
                P = w;
                g.a(a, 203, M);
                g.a(a, 28, hb);
                g.a(a, 24, fb);
                g.p = function () {
                    b.p(ib, y, n)
                }
            }

            function Vc(F, h, q, r) {
                var c = this,
                    E = b.ab(c, o),
                    i = 0,
                    t = 0,
                    g, m, f, e, n, w, v, y = x[h];
                l.call(c, 0, 0);

                function A() {
                    c.Qc()
                }

                function C(a) {
                    v = a;
                    c.H();
                    c.Qc()
                }

                function z() {}
                c.Qc = function () {
                    if (!B && !mb && !v && u == h && !c.Wc()) {
                        var k = c.m();
                        if (!k)
                            if (g && !n) {
                                n = d;
                                c.nd(d);
                                a.k(j.bf, h, t, i, t, g, e)
                            } a.k(j.ld, h, k, i, m, f, e);
                        if (!Eb()) {
                            var l;
                            if (k == e) s && b.hc(y.ef, 20);
                            else {
                                if (k == f) l = e;
                                else if (!k) l = f;
                                else l = c.Bd();
                                (k != f || !Bc()) && c.Ub(l, A)
                            }
                        }
                    }
                };
                c.zf = function () {
                    f == e && f == c.m() && c.D(m)
                };
                c.df = function () {
                    D && D.Ob == h && D.jc();
                    var b = c.m();
                    b < e && a.k(j.ld, h, -b - 1, i, m, f, e)
                };
                c.nd = function (a) {
                    q && b.Pc(S, a && q.xc.Mg ? "" : "hidden")
                };
                c.Zc = function (c, b) {
                    if (n && b >= g) {
                        n = k;
                        y.id();
                        D.jc();
                        a.k(j.Ze, h, g, i, t, g, e)
                    }
                    a.k(j.Ye, h, b, i, m, f, e)
                };
                c.Mf = function (a) {
                    if (a && !w) {
                        w = a;
                        a.Sc($JssorPlayer$.te, C)
                    }
                };
                c.a(r, p.Fc, z);
                q && c.rc(q);
                g = c.kb();
                c.rc(r);
                m = g + r.gc;
                e = c.kb();
                f = s ? g + r.Dc : e;
                c.p = function () {
                    E.p();
                    c.H()
                }
            }

            function gc() {
                bc = mb;
                Ac = y.Bd();
                ab = z.m();
                ub = dd(ab)
            }

            function Fc() {
                gc();
                if (B || Eb()) {
                    y.H();
                    a.k(j.Xe)
                }
            }

            function Dc(f) {
                if (Db()) {
                    var b = z.m(),
                        a = ub,
                        e = 0;
                    if (f && c.tb(O) >= m.ie) {
                        a = b;
                        e = yb
                    }
                    a = c.ub(a);
                    a = wb(a + e, .5);
                    var d = c.tb(a - b);
                    if (d < 1 && m.Wd != h.ac) d = c.q(d, .5);
                    if ((!db || !f) && bc) y.Ub(Ac);
                    else if (b == a) jc.Zb();
                    else y.wc(b, a, d * Bb)
                }
            }

            function yc(a) {
                !b.eb(b.Lb(a), e, n.Od) && b.Sb(a)
            }

            function pc(b) {
                Ab = k;
                B = d;
                Fc();
                if (!bc) A = 0;
                a.k(j.We, v(ab), ab, b)
            }

            function Tc(a) {
                tc(a, 1)
            }

            function tc(c, d) {
                O = 0;
                xb = 0;
                yb = 0;
                Gc = hc;
                if (d) {
                    var h = c.touches[0];
                    zb = {
                        x: h.clientX,
                        y: h.clientY
                    }
                } else zb = b.ed(c);
                var f = b.Lb(c),
                    g = b.eb(f, "1", Cc);
                if ((!g || g === t) && !W && (!d || c.touches.length == 1)) {
                    jb = b.eb(f, e, n.Od) || !ib || !Uc();
                    a.a(i, d ? "touchmove" : "mousemove", Wb);
                    Ab = !jb && b.eb(f, e, n.uc);
                    !Ab && !jb && pc(c, d)
                }
            }

            function Wb(a) {
                var e, f;
                a = b.vg(a);
                if (a.type != "mousemove")
                    if (a.touches.length == 1) {
                        f = a.touches[0];
                        e = {
                            x: f.clientX,
                            y: f.clientY
                        }
                    } else fb();
                else e = b.ed(a);
                if (e) {
                    var i = e.x - zb.x,
                        j = e.y - zb.y,
                        g = c.tb(i),
                        h = c.tb(j);
                    if (A || g > 1.5 || h > 1.5)
                        if (Ab) pc(a, f);
                        else {
                            if (c.cb(ub) != ub) A = A || R & W;
                            if ((i || j) && !A) {
                                if (W == 3)
                                    if (h > g) A = 2;
                                    else A = 1;
                                else A = W;
                                if (Qb && A == 1 && h > g * 2.4) jb = d
                            }
                            var l = i,
                                k = kb;
                            if (A == 2) {
                                l = j;
                                k = lb
                            }(O - xb) * nb < -1.5 && (yb = 0);
                            (O - xb) * nb > 1.5 && (yb = -1);
                            xb = O;
                            O = l;
                            Ic = ub - O * nb / k / Gc * m.Oe;
                            if (O && A && !jb) {
                                b.Sb(a);
                                y.qg(d);
                                y.Cg(Ic)
                            }
                        }
                }
            }

            function fb() {
                Qc();
                a.M(i, "mousemove", Wb);
                a.M(i, "touchmove", Wb);
                db = O;
                if (B) {
                    db && s & 8 && (s = 0);
                    y.H();
                    B = k;
                    var b = z.m();
                    a.k(j.Ue, v(b), b, v(ab), ab);
                    X & 12 && gc();
                    Dc(d)
                }
            }

            function ed(c) {
                var f = b.Lb(c),
                    a = b.eb(f, "1", Kb);
                if (t === a)
                    if (db) {
                        b.mg(c);
                        a = b.eb(f, e, "data-jssor-button", "A");
                        a && b.Sb(c)
                    } else {
                        s & 4 && (s = 0);
                        a = b.eb(f, e, "data-jssor-click");
                        if (a) {
                            b.Sb(c);
                            hitValues = (b.l(a, "data-jssor-click") || "").split(":");
                            var g = b.ve(hitValues[1]);
                            hitValues[0] == "to" && qb(g - 1);
                            hitValues[0] == "next" && qb(g, e, d)
                        }
                    }
            }
            a.xb = function (a) {
                if (a == e) return s;
                if (a != s) {
                    s = a;
                    s && x[u] && x[u].Zb()
                }
            };
            a.Db = function () {
                return K
            };
            a.Cb = function () {
                return P
            };
            a.Re = function (b) {
                if (b == e) return Ub || K;
                a.fc(b, b / K * P)
            };
            a.fc = function (c, a, d) {
                b.I(t, c);
                b.F(t, a);
                zc = c / K;
                qc = a / P;
                b.g(cc, function (a) {
                    a.fc(zc, qc, d)
                });
                if (!Ub) {
                    b.pb(S, w);
                    b.O(S, 0);
                    b.P(S, 0)
                }
                Ub = c
            };
            a.ne = oc;
            a.oe = mc;
            a.Dd = function () {
                a.xb(s || 1)
            };

            function Sc() {
                Xb.de && b.E(w, Xb.de, ([f, "pan-y", "pan-x", "auto"])[ib] || "");
                a.a(t, "click", ed, d);
                a.a(t, "mouseleave", cd);
                a.a(t, "mouseenter", bd);
                a.a(t, "mousedown", tc);
                a.a(t, "touchstart", Tc);
                a.a(t, "dragstart", yc);
                a.a(t, "selectstart", yc);
                a.a(g, "mouseup", fb);
                a.a(i, "mouseup", fb);
                a.a(i, "touchend", fb);
                a.a(i, "touchcancel", fb);
                a.a(g, "blur", fb);
                m.dc && a.a(i, "keydown", function (c) {
                    var a = b.Md(c);
                    if (a == 37 || a == 39) {
                        s & 8 && (s = 0);
                        wc(m.dc * (a - 38) * nb, d)
                    }
                })
            }

            function kc(g) {
                xc.kd();
                E = [];
                x = [];
                var h = b.Gb(w),
                    k = b.jd(["DIV", "A", "LI"]);
                b.g(h, function (a) {
                    var c = a;
                    if (k[a.tagName.toUpperCase()] && !b.W(a, "u") && b.qb(a) != "none") {
                        var c = Tb(d);
                        b.Y(a, gb);
                        b.pb(c, a);
                        b.Z(c, a);
                        b.cc(c, "flat");
                        b.cc(a, "preserve-3d");
                        b.vb(c);
                        E.push(c)
                    }
                    b.L(c, (b.L(c) || 0) + 1)
                });
                q = E.length;
                if (q) {
                    var a = R & 1 ? Gb : Fb;
                    Jc();
                    G = m.Se;
                    if (G == e) G = (a - F + L) / 2;
                    hb = a / F;
                    J = c.u(q, m.Kc || q, c.ub(hb));
                    C = J < q ? m.Mb : 0;
                    if (q * F - L <= a) {
                        hb = q - L / F;
                        G = (a - F + L) / 2;
                        ac = (a - F * q + L) / 2
                    }
                    if (Hb) {
                        Mb = Hb.X;
                        Nb = !G && J == 1 && q > 1 && Mb && (!b.Yd() || b.Xc() >= 9)
                    }
                    if (!(C & 1)) {
                        bb = G / F;
                        if (bb > q - 1) {
                            bb = q - 1;
                            G = bb * F
                        }
                        N = bb;
                        Q = N + q - hb - L / F
                    }
                    ib = (J > 1 || G ? R : -1) & m.zc;
                    if (Nb) D = new Mb(Jb, I, H, Hb, Qb, Hc);
                    for (var f = 0; f < E.length; f++) {
                        var i = E[f],
                            j = new Wc(i, f);
                        x.push(j)
                    }
                    ob = new Xc;
                    z = new Yc;
                    y = new Oc(z, ob);
                    Sc()
                }
                b.g(Y, function (a) {
                    a.Hc(q, x);
                    g && a.Sc(r.Bc, wc)
                })
            }
            a.s = function (c, g) {
                a.Q = t = b.Rb(c);
                K = b.I(t);
                P = b.F(t);
                m = b.jb({
                    cf: 0,
                    kf: 1,
                    dc: 1,
                    Ac: 0,
                    xb: 0,
                    Mb: 1,
                    Ad: d,
                    Ld: d,
                    Dg: 1,
                    vd: 3e3,
                    ee: 1,
                    he: 500,
                    Wd: h.Xd,
                    ie: 20,
                    Oe: 1,
                    je: 0,
                    Of: 1,
                    ce: 1,
                    zc: 1
                }, g);
                m.Ad = m.Ad && b.xg();
                if (m.ob != e) m.vd = m.ob;
                if (m.Rf != e) m.Kc = m.Rf;
                if (m.Tf != e) m.Se = m.Tf;
                s = m.xb & 63;
                !m.Of;
                eb = m.Dg;
                X = m.ee;
                X &= Qb ? 10 : 5;
                ic = m.vd;
                Bb = m.he;
                R = m.ce & 3;
                sb = b.hg(b.l(t, "dir")) == "rtl";
                Cb = sb && (R == 1 || m.zc & 1);
                nb = Cb ? -1 : 1;
                Hb = m.Jg;
                cb = b.jb({
                    X: p
                }, m.Vf);
                tb = m.dg;
                Z = m.fg;
                M = m.Og;
                var f = b.Gb(t);
                b.g(f, function (a, d) {
                    var c = b.W(a, "u");
                    if (c == "loading") U = a;
                    else {
                        if (c == "slides") {
                            w = a;
                            b.E(w, "margin", 0);
                            b.E(w, "padding", 0);
                            b.cc(w, "flat")
                        }
                        if (c == "navigator") Vb = a;
                        if (c == "arrowleft") Zb = a;
                        if (c == "arrowright") Yb = a;
                        if (c == "thumbnavigator") Ib = a;
                        if (a.tagName != "STYLE" && a.tagName != "SCRIPT") cc[c || d] = new Nc(a, c == "slides", b.jd(["slides", "thumbnavigator"])[c])
                    }
                });
                U && b.Pb(U);
                U = U || b.Mc(i);
                Gb = b.I(w);
                Fb = b.F(w);
                I = m.He || Gb;
                H = m.Ke || Fb;
                gb = {
                    T: I,
                    R: H,
                    i: 0,
                    j: 0,
                    Fd: "block",
                    wb: "absolute"
                };
                L = m.je;
                kb = I + L;
                lb = H + L;
                F = R & 1 ? kb : lb;
                Jb = new Lc;
                b.l(t, Kb, "1");
                b.L(w, b.L(w) || 0);
                b.Kb(w, "absolute");
                S = b.pc(w, d);
                b.E(S, "pointerEvents", "none");
                b.pb(S, w);
                b.Z(S, Jb.Q);
                b.Pc(w, "hidden");
                if (Vb && tb) {
                    tb.Yb = sb;
                    lc = new tb.X(Vb, tb, K, P);
                    Y.push(lc)
                }
                if (Z && Zb && Yb) {
                    Z.Yb = sb;
                    Z.Mb = m.Mb;
                    nc = new Z.X(Zb, Yb, Z, a);
                    Y.push(nc)
                }
                if (Ib && M) {
                    M.Ac = m.Ac;
                    M.dc = M.dc || 0;
                    M.Yb = sb;
                    dc = new M.X(Ib, M, U);
                    !M.Ig && b.l(Ib, Cc, "1");
                    Y.push(dc)
                }
                kc(d);
                a.fc(K, P);
                Pb();
                rb(v(m.Ac));
                b.E(t, "visibility", "visible")
            };
            a.p = function () {
                s = 0;
                b.p(x, Y, xc);
                b.Vb(t)
            };
            b.s(a)
        };
    j.gf = 21;
    j.We = 22;
    j.Ue = 23;
    j.rg = 24;
    j.yg = 25;
    j.ff = 26;
    j.Eg = 27;
    j.Xe = 28;
    j.Hg = 31;
    j.zg = 32;
    j.sg = 202;
    j.Qf = 203;
    j.bf = 206;
    j.Ze = 207;
    j.Ye = 208;
    j.ld = 209;
    jssor_1_slider_init = function () {
        var d = [
                [{
                    b: 0,
                    d: 600,
                    y: -290,
                    e: {
                        y: 27
                    }
                }],
                [{
                    b: 0,
                    d: 1e3,
                    y: 185
                }, {
                    b: 1e3,
                    d: 500,
                    o: -1
                }, {
                    b: 1500,
                    d: 500,
                    o: 1
                }, {
                    b: 2e3,
                    d: 1500,
                    r: 360
                }, {
                    b: 3500,
                    d: 1e3,
                    rX: 30
                }, {
                    b: 4500,
                    d: 500,
                    rX: -30
                }, {
                    b: 5e3,
                    d: 1e3,
                    rY: 30
                }, {
                    b: 6e3,
                    d: 500,
                    rY: -30
                }, {
                    b: 6500,
                    d: 500,
                    sX: 1
                }, {
                    b: 7e3,
                    d: 500,
                    sX: -1
                }, {
                    b: 7500,
                    d: 500,
                    sY: 1
                }, {
                    b: 8e3,
                    d: 500,
                    sY: -1
                }, {
                    b: 8500,
                    d: 500,
                    kX: 30
                }, {
                    b: 9e3,
                    d: 500,
                    kX: -30
                }, {
                    b: 9500,
                    d: 500,
                    kY: 30
                }, {
                    b: 1e4,
                    d: 500,
                    kY: -30
                }, {
                    b: 10500,
                    d: 500,
                    c: {
                        x: 125,
                        t: -125
                    }
                }, {
                    b: 11e3,
                    d: 500,
                    c: {
                        x: -125,
                        t: 125
                    }
                }],
                [{
                    b: 0,
                    d: 600,
                    x: 535,
                    e: {
                        x: 27
                    }
                }],
                [{
                    b: -1,
                    d: 1,
                    o: -1
                }, {
                    b: 0,
                    d: 600,
                    o: 1,
                    e: {
                        o: 5
                    }
                }],
                [{
                    b: -1,
                    d: 1,
                    c: {
                        x: 250,
                        t: -250
                    }
                }, {
                    b: 0,
                    d: 800,
                    c: {
                        x: -250,
                        t: 250
                    },
                    e: {
                        c: {
                            x: 7,
                            t: 7
                        }
                    }
                }],
                [{
                    b: -1,
                    d: 1,
                    o: -1
                }, {
                    b: 0,
                    d: 600,
                    x: -570,
                    o: 1,
                    e: {
                        x: 6
                    }
                }],
                [{
                    b: -1,
                    d: 1,
                    o: -1,
                    r: -180
                }, {
                    b: 0,
                    d: 800,
                    o: 1,
                    r: 180,
                    e: {
                        r: 7
                    }
                }],
                [{
                    b: 0,
                    d: 1e3,
                    y: 80,
                    e: {
                        y: 24
                    }
                }, {
                    b: 1e3,
                    d: 1100,
                    x: 570,
                    y: 170,
                    o: -1,
                    r: 30,
                    sX: 9,
                    sY: 9,
                    e: {
                        x: 2,
                        y: 6,
                        r: 1,
                        sX: 5,
                        sY: 5
                    }
                }],
                [{
                    b: 2e3,
                    d: 600,
                    rY: 30
                }],
                [{
                    b: 0,
                    d: 500,
                    x: -105
                }, {
                    b: 500,
                    d: 500,
                    x: 230
                }, {
                    b: 1e3,
                    d: 500,
                    y: -120
                }, {
                    b: 1500,
                    d: 500,
                    x: -70,
                    y: 120
                }, {
                    b: 2600,
                    d: 500,
                    y: -80
                }, {
                    b: 3100,
                    d: 900,
                    y: 160,
                    e: {
                        y: 24
                    }
                }],
                [{
                    b: 0,
                    d: 1e3,
                    o: -.4,
                    rX: 2,
                    rY: 1
                }, {
                    b: 1e3,
                    d: 1e3,
                    rY: 1
                }, {
                    b: 2e3,
                    d: 1e3,
                    rX: -1
                }, {
                    b: 3e3,
                    d: 1e3,
                    rY: -1
                }, {
                    b: 4e3,
                    d: 1e3,
                    o: .4,
                    rX: -1,
                    rY: -1
                }]
            ],
            e = {
                xb: 1,
                ob: 2e3,
                Vf: {
                    X: s,
                    yc: d,
                    Sf: [
                        [{
                            d: 2e3,
                            b: 1e3
                        }]
                    ]
                },
                fg: {
                    X: v
                },
                dg: {
                    X: u
                }
            },
            c = new j("jssor_1", e),
            f = 980;

        function a() {
            var d = c.Q.parentNode,
                b = d.clientWidth;
            if (b) {
                var e = m.min(f || b, b);
                c.Re(e)
            } else g.setTimeout(a, 30)
        }
        a();
        b.hb(g, "load", a);
        b.hb(g, "resize", a);
        b.hb(g, "orientationchange", a)
    }
}(window, document, Math, null, true, false)