(()=>{
        var mo = Object.defineProperty;
        var ho = (e,t)=>{
                for (var r in t)
                    mo(e, r, {
                        get: t[r],
                        enumerable: !0
                    })
            }
        ;
        var go = 1e3
            , Te = "transitionend"
            , vo = e=>e == null ? `${e}` : {}.toString.call(e).match(/\s([a-z]+)/i)[1].toLowerCase();
        var mr = e=>{
            let t = e.getAttribute("data-bs-target");
            if (!t || t === "#") {
                let r = e.getAttribute("href");
                if (!r || !r.includes("#") && !r.startsWith("."))
                    return null;
                r.includes("#") && !r.startsWith("#") && (r = `#${r.split("#")[1]}`),
                    t = r && r !== "#" ? r.trim() : null
            }
            return t
        }
            , Yt = e=>{
            let t = mr(e);
            return t && document.querySelector(t) ? t : null
        }
            , Ft = e=>{
            let t = mr(e);
            return t ? document.querySelector(t) : null
        }
            , Eo = e=>{
            if (!e)
                return 0;
            let {transitionDuration: t, transitionDelay: r} = window.getComputedStyle(e)
                , o = Number.parseFloat(t)
                , n = Number.parseFloat(r);
            return !o && !n ? 0 : (t = t.split(",")[0],
                r = r.split(",")[0],
            (Number.parseFloat(t) + Number.parseFloat(r)) * go)
        }
            , _o = e=>{
            e.dispatchEvent(new Event(Te))
        }
            , xt = e=>!e || typeof e != "object" ? !1 : (typeof e.jquery != "undefined" && (e = e[0]),
        typeof e.nodeType != "undefined")
            , Z = e=>xt(e) ? e.jquery ? e[0] : e : typeof e == "string" && e.length > 0 ? document.querySelector(e) : null
            , Ot = (e,t,r)=>{
            Object.keys(r).forEach(o=>{
                    let n = r[o]
                        , i = t[o]
                        , a = i && xt(i) ? "element" : vo(i);
                    if (!new RegExp(n).test(a))
                        throw new TypeError(`${e.toUpperCase()}: Option "${o}" provided type "${a}" but expected type "${n}".`)
                }
            )
        }
            , se = e=>!xt(e) || e.getClientRects().length === 0 ? !1 : getComputedStyle(e).getPropertyValue("visibility") === "visible"
            , At = e=>!e || e.nodeType !== Node.ELEMENT_NODE || e.classList.contains("disabled") ? !0 : typeof e.disabled != "undefined" ? e.disabled : e.hasAttribute("disabled") && e.getAttribute("disabled") !== "false";
        var Se = ()=>{}
            , dr = e=>{
                e.offsetHeight
            }
            , Ne = ()=>{
                let {jQuery: e} = window;
                return e && !document.body.hasAttribute("data-bs-no-jquery") ? e : null
            }
            , Ce = []
            , yo = e=>{
                document.readyState === "loading" ? (Ce.length || document.addEventListener("DOMContentLoaded", ()=>{
                        Ce.forEach(t=>t())
                    }
                ),
                    Ce.push(e)) : e()
            }
            , pt = ()=>document.documentElement.dir === "rtl"
            , Tt = e=>{
                yo(()=>{
                        let t = Ne();
                        if (t) {
                            let r = e.NAME
                                , o = t.fn[r];
                            t.fn[r] = e.jQueryInterface,
                                t.fn[r].Constructor = e,
                                t.fn[r].noConflict = ()=>(t.fn[r] = o,
                                    e.jQueryInterface)
                        }
                    }
                )
            }
            , hr = e=>{
                typeof e == "function" && e()
            }
            , gr = (e,t,r=!0)=>{
                if (!r) {
                    hr(e);
                    return
                }
                let o = 5
                    , n = Eo(t) + o
                    , i = !1
                    , a = ({target: s})=>{
                        s === t && (i = !0,
                            t.removeEventListener(Te, a),
                            hr(e))
                    }
                ;
                t.addEventListener(Te, a),
                    setTimeout(()=>{
                            i || _o(t)
                        }
                        , n)
            }
            , vr = (e,t,r,o)=>{
                let n = e.indexOf(t);
                if (n === -1)
                    return e[!r && o ? e.length - 1 : 0];
                let i = e.length;
                return n += r ? 1 : -1,
                o && (n = (n + i) % i),
                    e[Math.max(0, Math.min(n, i - 1))]
            }
        ;
        var nt = new Map
            , St = {
            set(e, t, r) {
                nt.has(e) || nt.set(e, new Map);
                let o = nt.get(e);
                if (!o.has(t) && o.size !== 0) {
                    console.error(`Bootstrap doesn't allow more than one instance per element. Bound instance: ${Array.from(o.keys())[0]}.`);
                    return
                }
                o.set(t, r)
            },
            get(e, t) {
                return nt.has(e) && nt.get(e).get(t) || null
            },
            remove(e, t) {
                if (!nt.has(e))
                    return;
                let r = nt.get(e);
                r.delete(t),
                r.size === 0 && nt.delete(e)
            }
        };
        var wo = /[^.]*(?=\..*)\.|.*/
            , bo = /\..*/
            , xo = /::\d+$/
            , Pe = {}
            , Er = 1
            , Oo = {
            mouseenter: "mouseover",
            mouseleave: "mouseout"
        }
            , Ao = /^(mouseenter|mouseleave)/i
            , _r = new Set(["click", "dblclick", "mouseup", "mousedown", "contextmenu", "mousewheel", "DOMMouseScroll", "mouseover", "mouseout", "mousemove", "selectstart", "selectend", "keydown", "keypress", "keyup", "orientationchange", "touchstart", "touchmove", "touchend", "touchcancel", "pointerdown", "pointermove", "pointerup", "pointerleave", "pointercancel", "gesturestart", "gesturechange", "gestureend", "focus", "blur", "change", "reset", "select", "submit", "focusin", "focusout", "load", "unload", "beforeunload", "resize", "move", "DOMContentLoaded", "readystatechange", "error", "abort", "scroll"]);
        function yr(e, t) {
            return t && `${t}::${Er++}` || e.uidEvent || Er++
        }
        function wr(e) {
            let t = yr(e);
            return e.uidEvent = t,
                Pe[t] = Pe[t] || {},
                Pe[t]
        }
        function To(e, t) {
            return function r(o) {
                return o.delegateTarget = e,
                r.oneOff && Le.off(e, o.type, t),
                    t.apply(e, [o])
            }
        }
        function So(e, t, r) {
            return function o(n) {
                let i = e.querySelectorAll(t);
                for (let {target: a} = n; a && a !== this; a = a.parentNode)
                    for (let s = i.length; s--; )
                        if (i[s] === a)
                            return n.delegateTarget = a,
                            o.oneOff && Le.off(e, n.type, t, r),
                                r.apply(a, [n]);
                return null
            }
        }
        function br(e, t, r=null) {
            let o = Object.keys(e);
            for (let n = 0, i = o.length; n < i; n++) {
                let a = e[o[n]];
                if (a.originalHandler === t && a.delegationSelector === r)
                    return a
            }
            return null
        }
        function xr(e, t, r) {
            let o = typeof t == "string"
                , n = o ? r : t
                , i = Ar(e);
            return _r.has(i) || (i = e),
                [o, n, i]
        }
        function Or(e, t, r, o, n) {
            if (typeof t != "string" || !e)
                return;
            if (r || (r = o,
                o = null),
                Ao.test(t)) {
                let c = E=>function(m) {
                        if (!m.relatedTarget || m.relatedTarget !== m.delegateTarget && !m.delegateTarget.contains(m.relatedTarget))
                            return E.call(this, m)
                    }
                ;
                o ? o = c(o) : r = c(r)
            }
            let[i,a,s] = xr(t, r, o)
                , f = wr(e)
                , p = f[s] || (f[s] = {})
                , l = br(p, a, i ? r : null);
            if (l) {
                l.oneOff = l.oneOff && n;
                return
            }
            let u = yr(a, t.replace(wo, ""))
                , d = i ? So(e, r, o) : To(e, r);
            d.delegationSelector = i ? r : null,
                d.originalHandler = a,
                d.oneOff = n,
                d.uidEvent = u,
                p[u] = d,
                e.addEventListener(s, d, i)
        }
        function De(e, t, r, o, n) {
            let i = br(t[r], o, n);
            !i || (e.removeEventListener(r, i, Boolean(n)),
                delete t[r][i.uidEvent])
        }
        function No(e, t, r, o) {
            let n = t[r] || {};
            Object.keys(n).forEach(i=>{
                    if (i.includes(o)) {
                        let a = n[i];
                        De(e, t, r, a.originalHandler, a.delegationSelector)
                    }
                }
            )
        }
        function Ar(e) {
            return e = e.replace(bo, ""),
            Oo[e] || e
        }
        var Le = {
            on(e, t, r, o) {
                Or(e, t, r, o, !1)
            },
            one(e, t, r, o) {
                Or(e, t, r, o, !0)
            },
            off(e, t, r, o) {
                if (typeof t != "string" || !e)
                    return;
                let[n,i,a] = xr(t, r, o)
                    , s = a !== t
                    , f = wr(e)
                    , p = t.startsWith(".");
                if (typeof i != "undefined") {
                    if (!f || !f[a])
                        return;
                    De(e, f, a, i, n ? r : null);
                    return
                }
                p && Object.keys(f).forEach(u=>{
                        No(e, f, u, t.slice(1))
                    }
                );
                let l = f[a] || {};
                Object.keys(l).forEach(u=>{
                        let d = u.replace(xo, "");
                        if (!s || t.includes(d)) {
                            let c = l[u];
                            De(e, f, a, c.originalHandler, c.delegationSelector)
                        }
                    }
                )
            },
            trigger(e, t, r) {
                if (typeof t != "string" || !e)
                    return null;
                let o = Ne(), n = Ar(t), i = t !== n, a = _r.has(n), s, f = !0, p = !0, l = !1, u = null;
                return i && o && (s = o.Event(t, r),
                    o(e).trigger(s),
                    f = !s.isPropagationStopped(),
                    p = !s.isImmediatePropagationStopped(),
                    l = s.isDefaultPrevented()),
                    a ? (u = document.createEvent("HTMLEvents"),
                        u.initEvent(n, f, !0)) : u = new CustomEvent(t,{
                        bubbles: f,
                        cancelable: !0
                    }),
                typeof r != "undefined" && Object.keys(r).forEach(d=>{
                        Object.defineProperty(u, d, {
                            get() {
                                return r[d]
                            }
                        })
                    }
                ),
                l && u.preventDefault(),
                p && e.dispatchEvent(u),
                u.defaultPrevented && typeof s != "undefined" && s.preventDefault(),
                    u
            }
        }
            , b = Le;
        function Tr(e) {
            return e === "true" ? !0 : e === "false" ? !1 : e === Number(e).toString() ? Number(e) : e === "" || e === "null" ? null : e
        }
        function Me(e) {
            return e.replace(/[A-Z]/g, t=>`-${t.toLowerCase()}`)
        }
        var Co = {
            setDataAttribute(e, t, r) {
                e.setAttribute(`data-bs-${Me(t)}`, r)
            },
            removeDataAttribute(e, t) {
                e.removeAttribute(`data-bs-${Me(t)}`)
            },
            getDataAttributes(e) {
                if (!e)
                    return {};
                let t = {};
                return Object.keys(e.dataset).filter(r=>r.startsWith("bs")).forEach(r=>{
                        let o = r.replace(/^bs/, "");
                        o = o.charAt(0).toLowerCase() + o.slice(1, o.length),
                            t[o] = Tr(e.dataset[r])
                    }
                ),
                    t
            },
            getDataAttribute(e, t) {
                return Tr(e.getAttribute(`data-bs-${Me(t)}`))
            },
            offset(e) {
                let t = e.getBoundingClientRect();
                return {
                    top: t.top + window.pageYOffset,
                    left: t.left + window.pageXOffset
                }
            },
            position(e) {
                return {
                    top: e.offsetTop,
                    left: e.offsetLeft
                }
            }
        }
            , G = Co;
        var Po = 3
            , Do = {
            find(e, t=document.documentElement) {
                return [].concat(...Element.prototype.querySelectorAll.call(t, e))
            },
            findOne(e, t=document.documentElement) {
                return Element.prototype.querySelector.call(t, e)
            },
            children(e, t) {
                return [].concat(...e.children).filter(r=>r.matches(t))
            },
            parents(e, t) {
                let r = []
                    , o = e.parentNode;
                for (; o && o.nodeType === Node.ELEMENT_NODE && o.nodeType !== Po; )
                    o.matches(t) && r.push(o),
                        o = o.parentNode;
                return r
            },
            prev(e, t) {
                let r = e.previousElementSibling;
                for (; r; ) {
                    if (r.matches(t))
                        return [r];
                    r = r.previousElementSibling
                }
                return []
            },
            next(e, t) {
                let r = e.nextElementSibling;
                for (; r; ) {
                    if (r.matches(t))
                        return [r];
                    r = r.nextElementSibling
                }
                return []
            },
            focusableChildren(e) {
                let t = ["a", "button", "input", "textarea", "select", "details", "[tabindex]", '[contenteditable="true"]'].map(r=>`${r}:not([tabindex^="-"])`).join(", ");
                return this.find(t, e).filter(r=>!At(r) && se(r))
            }
        }
            , x = Do;
        var Lo = "5.1.3"
            , Sr = class {
            constructor(t) {
                t = Z(t),
                !!t && (this._element = t,
                    St.set(this._element, this.constructor.DATA_KEY, this))
            }
            dispose() {
                St.remove(this._element, this.constructor.DATA_KEY),
                    b.off(this._element, this.constructor.EVENT_KEY),
                    Object.getOwnPropertyNames(this).forEach(t=>{
                            this[t] = null
                        }
                    )
            }
            _queueCallback(t, r, o=!0) {
                gr(t, r, o)
            }
            static getInstance(t) {
                return St.get(Z(t), this.DATA_KEY)
            }
            static getOrCreateInstance(t, r={}) {
                return this.getInstance(t) || new this(t,typeof r == "object" ? r : null)
            }
            static get VERSION() {
                return Lo
            }
            static get NAME() {
                throw new Error('You have to implement the static method "NAME", for each component!')
            }
            static get DATA_KEY() {
                return `bs.${this.NAME}`
            }
            static get EVENT_KEY() {
                return `.${this.DATA_KEY}`
            }
        }
            , Nt = Sr;
        var Nr = "collapse"
            , Cr = "bs.collapse"
            , Kt = `.${Cr}`
            , Mo = ".data-api"
            , Pr = {
                toggle: !0,
                parent: null
            }
            , Ro = {
                toggle: "boolean",
                parent: "(null|element)"
            }
            , Io = `show${Kt}`
            , Ho = `shown${Kt}`
            , $o = `hide${Kt}`
            , Bo = `hidden${Kt}`
            , Vo = `click${Kt}${Mo}`
            , Re = "show"
            , Ct = "collapse"
            , fe = "collapsing"
            , Dr = "collapsed"
            , Lr = `:scope .${Ct} .${Ct}`
            , ko = "collapse-horizontal"
            , jo = "width"
            , Wo = "height"
            , Yo = ".collapse.show, .collapse.collapsing"
            , Ie = '[data-bs-toggle="collapse"]'
            , ct = class extends Nt {
                constructor(t, r) {
                    super(t);
                    this._isTransitioning = !1,
                        this._config = this._getConfig(r),
                        this._triggerArray = [];
                    let o = x.find(Ie);
                    for (let n = 0, i = o.length; n < i; n++) {
                        let a = o[n]
                            , s = Yt(a)
                            , f = x.find(s).filter(p=>p === this._element);
                        s !== null && f.length && (this._selector = s,
                            this._triggerArray.push(a))
                    }
                    this._initializeChildren(),
                    this._config.parent || this._addAriaAndCollapsedClass(this._triggerArray, this._isShown()),
                    this._config.toggle && this.toggle()
                }
                static get Default() {
                    return Pr
                }
                static get NAME() {
                    return Nr
                }
                toggle() {
                    this._isShown() ? this.hide() : this.show()
                }
                show() {
                    if (this._isTransitioning || this._isShown())
                        return;
                    let t = [], r;
                    if (this._config.parent) {
                        let p = x.find(Lr, this._config.parent);
                        t = x.find(Yo, this._config.parent).filter(l=>!p.includes(l))
                    }
                    let o = x.findOne(this._selector);
                    if (t.length) {
                        let p = t.find(l=>o !== l);
                        if (r = p ? ct.getInstance(p) : null,
                        r && r._isTransitioning)
                            return
                    }
                    if (b.trigger(this._element, Io).defaultPrevented)
                        return;
                    t.forEach(p=>{
                            o !== p && ct.getOrCreateInstance(p, {
                                toggle: !1
                            }).hide(),
                            r || St.set(p, Cr, null)
                        }
                    );
                    let i = this._getDimension();
                    this._element.classList.remove(Ct),
                        this._element.classList.add(fe),
                        this._element.style[i] = 0,
                        this._addAriaAndCollapsedClass(this._triggerArray, !0),
                        this._isTransitioning = !0;
                    let a = ()=>{
                        this._isTransitioning = !1,
                            this._element.classList.remove(fe),
                            this._element.classList.add(Ct, Re),
                            this._element.style[i] = "",
                            b.trigger(this._element, Ho)
                    }
                        , f = `scroll${i[0].toUpperCase() + i.slice(1)}`;
                    this._queueCallback(a, this._element, !0),
                        this._element.style[i] = `${this._element[f]}px`
                }
                hide() {
                    if (this._isTransitioning || !this._isShown() || b.trigger(this._element, $o).defaultPrevented)
                        return;
                    let r = this._getDimension();
                    this._element.style[r] = `${this._element.getBoundingClientRect()[r]}px`,
                        dr(this._element),
                        this._element.classList.add(fe),
                        this._element.classList.remove(Ct, Re);
                    let o = this._triggerArray.length;
                    for (let i = 0; i < o; i++) {
                        let a = this._triggerArray[i]
                            , s = Ft(a);
                        s && !this._isShown(s) && this._addAriaAndCollapsedClass([a], !1)
                    }
                    this._isTransitioning = !0;
                    let n = ()=>{
                            this._isTransitioning = !1,
                                this._element.classList.remove(fe),
                                this._element.classList.add(Ct),
                                b.trigger(this._element, Bo)
                        }
                    ;
                    this._element.style[r] = "",
                        this._queueCallback(n, this._element, !0)
                }
                _isShown(t=this._element) {
                    return t.classList.contains(Re)
                }
                _getConfig(t) {
                    return t = {
                        ...Pr,
                        ...G.getDataAttributes(this._element),
                        ...t
                    },
                        t.toggle = Boolean(t.toggle),
                        t.parent = Z(t.parent),
                        Ot(Nr, t, Ro),
                        t
                }
                _getDimension() {
                    return this._element.classList.contains(ko) ? jo : Wo
                }
                _initializeChildren() {
                    if (!this._config.parent)
                        return;
                    let t = x.find(Lr, this._config.parent);
                    x.find(Ie, this._config.parent).filter(r=>!t.includes(r)).forEach(r=>{
                            let o = Ft(r);
                            o && this._addAriaAndCollapsedClass([r], this._isShown(o))
                        }
                    )
                }
                _addAriaAndCollapsedClass(t, r) {
                    !t.length || t.forEach(o=>{
                            r ? o.classList.remove(Dr) : o.classList.add(Dr),
                                o.setAttribute("aria-expanded", r)
                        }
                    )
                }
                static jQueryInterface(t) {
                    return this.each(function() {
                        let r = {};
                        typeof t == "string" && /show|hide/.test(t) && (r.toggle = !1);
                        let o = ct.getOrCreateInstance(this, r);
                        if (typeof t == "string") {
                            if (typeof o[t] == "undefined")
                                throw new TypeError(`No method named "${t}"`);
                            o[t]()
                        }
                    })
                }
            }
        ;
        b.on(document, Vo, Ie, function(e) {
            (e.target.tagName === "A" || e.delegateTarget && e.delegateTarget.tagName === "A") && e.preventDefault();
            let t = Yt(this);
            x.find(t).forEach(o=>{
                    ct.getOrCreateInstance(o, {
                        toggle: !1
                    }).toggle()
                }
            )
        });
        Tt(ct);
        var Qe = {};
        ho(Qe, {
            afterMain: ()=>Br,
            afterRead: ()=>Ir,
            afterWrite: ()=>jr,
            applyStyles: ()=>Dt,
            arrow: ()=>pe,
            auto: ()=>qt,
            basePlacements: ()=>tt,
            beforeMain: ()=>Hr,
            beforeRead: ()=>Mr,
            beforeWrite: ()=>Vr,
            bottom: ()=>L,
            clippingParents: ()=>He,
            computeStyles: ()=>Lt,
            createPopper: ()=>Ee,
            createPopperBase: ()=>Gr,
            createPopperLite: ()=>Qr,
            detectOverflow: ()=>k,
            end: ()=>it,
            eventListeners: ()=>Mt,
            flip: ()=>de,
            hide: ()=>he,
            left: ()=>N,
            main: ()=>$r,
            modifierPhases: ()=>Be,
            offset: ()=>ge,
            placements: ()=>zt,
            popper: ()=>ut,
            popperGenerator: ()=>yt,
            popperOffsets: ()=>Ht,
            preventOverflow: ()=>ve,
            read: ()=>Rr,
            reference: ()=>$e,
            right: ()=>P,
            start: ()=>Q,
            top: ()=>S,
            variationPlacements: ()=>le,
            viewport: ()=>Ut,
            write: ()=>kr
        });
        var S = "top"
            , L = "bottom"
            , P = "right"
            , N = "left"
            , qt = "auto"
            , tt = [S, L, P, N]
            , Q = "start"
            , it = "end"
            , He = "clippingParents"
            , Ut = "viewport"
            , ut = "popper"
            , $e = "reference"
            , le = tt.reduce(function(e, t) {
            return e.concat([t + "-" + Q, t + "-" + it])
        }, [])
            , zt = [].concat(tt, [qt]).reduce(function(e, t) {
            return e.concat([t, t + "-" + Q, t + "-" + it])
        }, [])
            , Mr = "beforeRead"
            , Rr = "read"
            , Ir = "afterRead"
            , Hr = "beforeMain"
            , $r = "main"
            , Br = "afterMain"
            , Vr = "beforeWrite"
            , kr = "write"
            , jr = "afterWrite"
            , Be = [Mr, Rr, Ir, Hr, $r, Br, Vr, kr, jr];
        function R(e) {
            return e ? (e.nodeName || "").toLowerCase() : null
        }
        function D(e) {
            if (e == null)
                return window;
            if (e.toString() !== "[object Window]") {
                var t = e.ownerDocument;
                return t && t.defaultView || window
            }
            return e
        }
        function J(e) {
            var t = D(e).Element;
            return e instanceof t || e instanceof Element
        }
        function M(e) {
            var t = D(e).HTMLElement;
            return e instanceof t || e instanceof HTMLElement
        }
        function Pt(e) {
            if (typeof ShadowRoot == "undefined")
                return !1;
            var t = D(e).ShadowRoot;
            return e instanceof t || e instanceof ShadowRoot
        }
        function Fo(e) {
            var t = e.state;
            Object.keys(t.elements).forEach(function(r) {
                var o = t.styles[r] || {}
                    , n = t.attributes[r] || {}
                    , i = t.elements[r];
                !M(i) || !R(i) || (Object.assign(i.style, o),
                    Object.keys(n).forEach(function(a) {
                        var s = n[a];
                        s === !1 ? i.removeAttribute(a) : i.setAttribute(a, s === !0 ? "" : s)
                    }))
            })
        }
        function Ko(e) {
            var t = e.state
                , r = {
                popper: {
                    position: t.options.strategy,
                    left: "0",
                    top: "0",
                    margin: "0"
                },
                arrow: {
                    position: "absolute"
                },
                reference: {}
            };
            return Object.assign(t.elements.popper.style, r.popper),
                t.styles = r,
            t.elements.arrow && Object.assign(t.elements.arrow.style, r.arrow),
                function() {
                    Object.keys(t.elements).forEach(function(o) {
                        var n = t.elements[o]
                            , i = t.attributes[o] || {}
                            , a = Object.keys(t.styles.hasOwnProperty(o) ? t.styles[o] : r[o])
                            , s = a.reduce(function(f, p) {
                            return f[p] = "",
                                f
                        }, {});
                        !M(n) || !R(n) || (Object.assign(n.style, s),
                            Object.keys(i).forEach(function(f) {
                                n.removeAttribute(f)
                            }))
                    })
                }
        }
        var Dt = {
            name: "applyStyles",
            enabled: !0,
            phase: "write",
            fn: Fo,
            effect: Ko,
            requires: ["computeStyles"]
        };
        function I(e) {
            return e.split("-")[0]
        }
        var z = Math.max
            , mt = Math.min
            , et = Math.round;
        function K(e, t) {
            t === void 0 && (t = !1);
            var r = e.getBoundingClientRect()
                , o = 1
                , n = 1;
            if (M(e) && t) {
                var i = e.offsetHeight
                    , a = e.offsetWidth;
                a > 0 && (o = et(r.width) / a || 1),
                i > 0 && (n = et(r.height) / i || 1)
            }
            return {
                width: r.width / o,
                height: r.height / n,
                top: r.top / n,
                right: r.right / o,
                bottom: r.bottom / n,
                left: r.left / o,
                x: r.left / o,
                y: r.top / n
            }
        }
        function dt(e) {
            var t = K(e)
                , r = e.offsetWidth
                , o = e.offsetHeight;
            return Math.abs(t.width - r) <= 1 && (r = t.width),
            Math.abs(t.height - o) <= 1 && (o = t.height),
                {
                    x: e.offsetLeft,
                    y: e.offsetTop,
                    width: r,
                    height: o
                }
        }
        function Xt(e, t) {
            var r = t.getRootNode && t.getRootNode();
            if (e.contains(t))
                return !0;
            if (r && Pt(r)) {
                var o = t;
                do {
                    if (o && e.isSameNode(o))
                        return !0;
                    o = o.parentNode || o.host
                } while (o)
            }
            return !1
        }
        function V(e) {
            return D(e).getComputedStyle(e)
        }
        function Ve(e) {
            return ["table", "td", "th"].indexOf(R(e)) >= 0
        }
        function B(e) {
            return ((J(e) ? e.ownerDocument : e.document) || window.document).documentElement
        }
        function rt(e) {
            return R(e) === "html" ? e : e.assignedSlot || e.parentNode || (Pt(e) ? e.host : null) || B(e)
        }
        function Wr(e) {
            return !M(e) || V(e).position === "fixed" ? null : e.offsetParent
        }
        function qo(e) {
            var t = navigator.userAgent.toLowerCase().indexOf("firefox") !== -1
                , r = navigator.userAgent.indexOf("Trident") !== -1;
            if (r && M(e)) {
                var o = V(e);
                if (o.position === "fixed")
                    return null
            }
            var n = rt(e);
            for (Pt(n) && (n = n.host); M(n) && ["html", "body"].indexOf(R(n)) < 0; ) {
                var i = V(n);
                if (i.transform !== "none" || i.perspective !== "none" || i.contain === "paint" || ["transform", "perspective"].indexOf(i.willChange) !== -1 || t && i.willChange === "filter" || t && i.filter && i.filter !== "none")
                    return n;
                n = n.parentNode
            }
            return null
        }
        function X(e) {
            for (var t = D(e), r = Wr(e); r && Ve(r) && V(r).position === "static"; )
                r = Wr(r);
            return r && (R(r) === "html" || R(r) === "body" && V(r).position === "static") ? t : r || qo(e) || t
        }
        function ht(e) {
            return ["top", "bottom"].indexOf(e) >= 0 ? "x" : "y"
        }
        function gt(e, t, r) {
            return z(e, mt(t, r))
        }
        function Yr(e, t, r) {
            var o = gt(e, t, r);
            return o > r ? r : o
        }
        function Gt() {
            return {
                top: 0,
                right: 0,
                bottom: 0,
                left: 0
            }
        }
        function Qt(e) {
            return Object.assign({}, Gt(), e)
        }
        function Jt(e, t) {
            return t.reduce(function(r, o) {
                return r[o] = e,
                    r
            }, {})
        }
        var Uo = function(t, r) {
            return t = typeof t == "function" ? t(Object.assign({}, r.rects, {
                placement: r.placement
            })) : t,
                Qt(typeof t != "number" ? t : Jt(t, tt))
        };
        function zo(e) {
            var t, r = e.state, o = e.name, n = e.options, i = r.elements.arrow, a = r.modifiersData.popperOffsets, s = I(r.placement), f = ht(s), p = [N, P].indexOf(s) >= 0, l = p ? "height" : "width";
            if (!(!i || !a)) {
                var u = Uo(n.padding, r)
                    , d = dt(i)
                    , c = f === "y" ? S : N
                    , E = f === "y" ? L : P
                    , m = r.rects.reference[l] + r.rects.reference[f] - a[f] - r.rects.popper[l]
                    , h = a[f] - r.rects.reference[f]
                    , O = X(i)
                    , A = O ? f === "y" ? O.clientHeight || 0 : O.clientWidth || 0 : 0
                    , T = m / 2 - h / 2
                    , g = u[c]
                    , _ = A - d[l] - u[E]
                    , v = A / 2 - d[l] / 2 + T
                    , y = gt(g, v, _)
                    , C = f;
                r.modifiersData[o] = (t = {},
                    t[C] = y,
                    t.centerOffset = y - v,
                    t)
            }
        }
        function Xo(e) {
            var t = e.state
                , r = e.options
                , o = r.element
                , n = o === void 0 ? "[data-popper-arrow]" : o;
            n != null && (typeof n == "string" && (n = t.elements.popper.querySelector(n),
                !n) || !Xt(t.elements.popper, n) || (t.elements.arrow = n))
        }
        var pe = {
            name: "arrow",
            enabled: !0,
            phase: "main",
            fn: zo,
            effect: Xo,
            requires: ["popperOffsets"],
            requiresIfExists: ["preventOverflow"]
        };
        function q(e) {
            return e.split("-")[1]
        }
        var Go = {
            top: "auto",
            right: "auto",
            bottom: "auto",
            left: "auto"
        };
        function Qo(e) {
            var t = e.x
                , r = e.y
                , o = window
                , n = o.devicePixelRatio || 1;
            return {
                x: et(t * n) / n || 0,
                y: et(r * n) / n || 0
            }
        }
        function Fr(e) {
            var t, r = e.popper, o = e.popperRect, n = e.placement, i = e.variation, a = e.offsets, s = e.position, f = e.gpuAcceleration, p = e.adaptive, l = e.roundOffsets, u = e.isFixed, d = a.x, c = d === void 0 ? 0 : d, E = a.y, m = E === void 0 ? 0 : E, h = typeof l == "function" ? l({
                x: c,
                y: m
            }) : {
                x: c,
                y: m
            };
            c = h.x,
                m = h.y;
            var O = a.hasOwnProperty("x")
                , A = a.hasOwnProperty("y")
                , T = N
                , g = S
                , _ = window;
            if (p) {
                var v = X(r)
                    , y = "clientHeight"
                    , C = "clientWidth";
                if (v === D(r) && (v = B(r),
                V(v).position !== "static" && s === "absolute" && (y = "scrollHeight",
                    C = "scrollWidth")),
                    v = v,
                n === S || (n === N || n === P) && i === it) {
                    g = L;
                    var H = u && v === _ && _.visualViewport ? _.visualViewport.height : v[y];
                    m -= H - o.height,
                        m *= f ? 1 : -1
                }
                if (n === N || (n === S || n === L) && i === it) {
                    T = P;
                    var $ = u && v === _ && _.visualViewport ? _.visualViewport.width : v[C];
                    c -= $ - o.width,
                        c *= f ? 1 : -1
                }
            }
            var w = Object.assign({
                position: s
            }, p && Go)
                , j = l === !0 ? Qo({
                x: c,
                y: m
            }) : {
                x: c,
                y: m
            };
            if (c = j.x,
                m = j.y,
                f) {
                var W;
                return Object.assign({}, w, (W = {},
                    W[g] = A ? "0" : "",
                    W[T] = O ? "0" : "",
                    W.transform = (_.devicePixelRatio || 1) <= 1 ? "translate(" + c + "px, " + m + "px)" : "translate3d(" + c + "px, " + m + "px, 0)",
                    W))
            }
            return Object.assign({}, w, (t = {},
                t[g] = A ? m + "px" : "",
                t[T] = O ? c + "px" : "",
                t.transform = "",
                t))
        }
        function Jo(e) {
            var t = e.state
                , r = e.options
                , o = r.gpuAcceleration
                , n = o === void 0 ? !0 : o
                , i = r.adaptive
                , a = i === void 0 ? !0 : i
                , s = r.roundOffsets
                , f = s === void 0 ? !0 : s;
            if (!1)
                var p;
            var l = {
                placement: I(t.placement),
                variation: q(t.placement),
                popper: t.elements.popper,
                popperRect: t.rects.popper,
                gpuAcceleration: n,
                isFixed: t.options.strategy === "fixed"
            };
            t.modifiersData.popperOffsets != null && (t.styles.popper = Object.assign({}, t.styles.popper, Fr(Object.assign({}, l, {
                offsets: t.modifiersData.popperOffsets,
                position: t.options.strategy,
                adaptive: a,
                roundOffsets: f
            })))),
            t.modifiersData.arrow != null && (t.styles.arrow = Object.assign({}, t.styles.arrow, Fr(Object.assign({}, l, {
                offsets: t.modifiersData.arrow,
                position: "absolute",
                adaptive: !1,
                roundOffsets: f
            })))),
                t.attributes.popper = Object.assign({}, t.attributes.popper, {
                    "data-popper-placement": t.placement
                })
        }
        var Lt = {
            name: "computeStyles",
            enabled: !0,
            phase: "beforeWrite",
            fn: Jo,
            data: {}
        };
        var ce = {
            passive: !0
        };
        function Zo(e) {
            var t = e.state
                , r = e.instance
                , o = e.options
                , n = o.scroll
                , i = n === void 0 ? !0 : n
                , a = o.resize
                , s = a === void 0 ? !0 : a
                , f = D(t.elements.popper)
                , p = [].concat(t.scrollParents.reference, t.scrollParents.popper);
            return i && p.forEach(function(l) {
                l.addEventListener("scroll", r.update, ce)
            }),
            s && f.addEventListener("resize", r.update, ce),
                function() {
                    i && p.forEach(function(l) {
                        l.removeEventListener("scroll", r.update, ce)
                    }),
                    s && f.removeEventListener("resize", r.update, ce)
                }
        }
        var Mt = {
            name: "eventListeners",
            enabled: !0,
            phase: "write",
            fn: function() {},
            effect: Zo,
            data: {}
        };
        var tn = {
            left: "right",
            right: "left",
            bottom: "top",
            top: "bottom"
        };
        function Rt(e) {
            return e.replace(/left|right|bottom|top/g, function(t) {
                return tn[t]
            })
        }
        var en = {
            start: "end",
            end: "start"
        };
        function ue(e) {
            return e.replace(/start|end/g, function(t) {
                return en[t]
            })
        }
        function vt(e) {
            var t = D(e)
                , r = t.pageXOffset
                , o = t.pageYOffset;
            return {
                scrollLeft: r,
                scrollTop: o
            }
        }
        function Et(e) {
            return K(B(e)).left + vt(e).scrollLeft
        }
        function ke(e) {
            var t = D(e)
                , r = B(e)
                , o = t.visualViewport
                , n = r.clientWidth
                , i = r.clientHeight
                , a = 0
                , s = 0;
            return o && (n = o.width,
                i = o.height,
            /^((?!chrome|android).)*safari/i.test(navigator.userAgent) || (a = o.offsetLeft,
                s = o.offsetTop)),
                {
                    width: n,
                    height: i,
                    x: a + Et(e),
                    y: s
                }
        }
        function je(e) {
            var t, r = B(e), o = vt(e), n = (t = e.ownerDocument) == null ? void 0 : t.body, i = z(r.scrollWidth, r.clientWidth, n ? n.scrollWidth : 0, n ? n.clientWidth : 0), a = z(r.scrollHeight, r.clientHeight, n ? n.scrollHeight : 0, n ? n.clientHeight : 0), s = -o.scrollLeft + Et(e), f = -o.scrollTop;
            return V(n || r).direction === "rtl" && (s += z(r.clientWidth, n ? n.clientWidth : 0) - i),
                {
                    width: i,
                    height: a,
                    x: s,
                    y: f
                }
        }
        function _t(e) {
            var t = V(e)
                , r = t.overflow
                , o = t.overflowX
                , n = t.overflowY;
            return /auto|scroll|overlay|hidden/.test(r + n + o)
        }
        function me(e) {
            return ["html", "body", "#document"].indexOf(R(e)) >= 0 ? e.ownerDocument.body : M(e) && _t(e) ? e : me(rt(e))
        }
        function at(e, t) {
            var r;
            t === void 0 && (t = []);
            var o = me(e)
                , n = o === ((r = e.ownerDocument) == null ? void 0 : r.body)
                , i = D(o)
                , a = n ? [i].concat(i.visualViewport || [], _t(o) ? o : []) : o
                , s = t.concat(a);
            return n ? s : s.concat(at(rt(a)))
        }
        function It(e) {
            return Object.assign({}, e, {
                left: e.x,
                top: e.y,
                right: e.x + e.width,
                bottom: e.y + e.height
            })
        }
        function rn(e) {
            var t = K(e);
            return t.top = t.top + e.clientTop,
                t.left = t.left + e.clientLeft,
                t.bottom = t.top + e.clientHeight,
                t.right = t.left + e.clientWidth,
                t.width = e.clientWidth,
                t.height = e.clientHeight,
                t.x = t.left,
                t.y = t.top,
                t
        }
        function Kr(e, t) {
            return t === Ut ? It(ke(e)) : J(t) ? rn(t) : It(je(B(e)))
        }
        function on(e) {
            var t = at(rt(e))
                , r = ["absolute", "fixed"].indexOf(V(e).position) >= 0
                , o = r && M(e) ? X(e) : e;
            return J(o) ? t.filter(function(n) {
                return J(n) && Xt(n, o) && R(n) !== "body"
            }) : []
        }
        function We(e, t, r) {
            var o = t === "clippingParents" ? on(e) : [].concat(t)
                , n = [].concat(o, [r])
                , i = n[0]
                , a = n.reduce(function(s, f) {
                var p = Kr(e, f);
                return s.top = z(p.top, s.top),
                    s.right = mt(p.right, s.right),
                    s.bottom = mt(p.bottom, s.bottom),
                    s.left = z(p.left, s.left),
                    s
            }, Kr(e, i));
            return a.width = a.right - a.left,
                a.height = a.bottom - a.top,
                a.x = a.left,
                a.y = a.top,
                a
        }
        function Zt(e) {
            var t = e.reference, r = e.element, o = e.placement, n = o ? I(o) : null, i = o ? q(o) : null, a = t.x + t.width / 2 - r.width / 2, s = t.y + t.height / 2 - r.height / 2, f;
            switch (n) {
                case S:
                    f = {
                        x: a,
                        y: t.y - r.height
                    };
                    break;
                case L:
                    f = {
                        x: a,
                        y: t.y + t.height
                    };
                    break;
                case P:
                    f = {
                        x: t.x + t.width,
                        y: s
                    };
                    break;
                case N:
                    f = {
                        x: t.x - r.width,
                        y: s
                    };
                    break;
                default:
                    f = {
                        x: t.x,
                        y: t.y
                    }
            }
            var p = n ? ht(n) : null;
            if (p != null) {
                var l = p === "y" ? "height" : "width";
                switch (i) {
                    case Q:
                        f[p] = f[p] - (t[l] / 2 - r[l] / 2);
                        break;
                    case it:
                        f[p] = f[p] + (t[l] / 2 - r[l] / 2);
                        break;
                    default:
                }
            }
            return f
        }
        function k(e, t) {
            t === void 0 && (t = {});
            var r = t
                , o = r.placement
                , n = o === void 0 ? e.placement : o
                , i = r.boundary
                , a = i === void 0 ? He : i
                , s = r.rootBoundary
                , f = s === void 0 ? Ut : s
                , p = r.elementContext
                , l = p === void 0 ? ut : p
                , u = r.altBoundary
                , d = u === void 0 ? !1 : u
                , c = r.padding
                , E = c === void 0 ? 0 : c
                , m = Qt(typeof E != "number" ? E : Jt(E, tt))
                , h = l === ut ? $e : ut
                , O = e.rects.popper
                , A = e.elements[d ? h : l]
                , T = We(J(A) ? A : A.contextElement || B(e.elements.popper), a, f)
                , g = K(e.elements.reference)
                , _ = Zt({
                reference: g,
                element: O,
                strategy: "absolute",
                placement: n
            })
                , v = It(Object.assign({}, O, _))
                , y = l === ut ? v : g
                , C = {
                top: T.top - y.top + m.top,
                bottom: y.bottom - T.bottom + m.bottom,
                left: T.left - y.left + m.left,
                right: y.right - T.right + m.right
            }
                , H = e.modifiersData.offset;
            if (l === ut && H) {
                var $ = H[n];
                Object.keys(C).forEach(function(w) {
                    var j = [P, L].indexOf(w) >= 0 ? 1 : -1
                        , W = [S, L].indexOf(w) >= 0 ? "y" : "x";
                    C[w] += $[W] * j
                })
            }
            return C
        }
        function Ye(e, t) {
            t === void 0 && (t = {});
            var r = t
                , o = r.placement
                , n = r.boundary
                , i = r.rootBoundary
                , a = r.padding
                , s = r.flipVariations
                , f = r.allowedAutoPlacements
                , p = f === void 0 ? zt : f
                , l = q(o)
                , u = l ? s ? le : le.filter(function(E) {
                return q(E) === l
            }) : tt
                , d = u.filter(function(E) {
                return p.indexOf(E) >= 0
            });
            d.length === 0 && (d = u);
            var c = d.reduce(function(E, m) {
                return E[m] = k(e, {
                    placement: m,
                    boundary: n,
                    rootBoundary: i,
                    padding: a
                })[I(m)],
                    E
            }, {});
            return Object.keys(c).sort(function(E, m) {
                return c[E] - c[m]
            })
        }
        function nn(e) {
            if (I(e) === qt)
                return [];
            var t = Rt(e);
            return [ue(e), t, ue(t)]
        }
        function an(e) {
            var t = e.state
                , r = e.options
                , o = e.name;
            if (!t.modifiersData[o]._skip) {
                for (var n = r.mainAxis, i = n === void 0 ? !0 : n, a = r.altAxis, s = a === void 0 ? !0 : a, f = r.fallbackPlacements, p = r.padding, l = r.boundary, u = r.rootBoundary, d = r.altBoundary, c = r.flipVariations, E = c === void 0 ? !0 : c, m = r.allowedAutoPlacements, h = t.options.placement, O = I(h), A = O === h, T = f || (A || !E ? [Rt(h)] : nn(h)), g = [h].concat(T).reduce(function(bt, ot) {
                    return bt.concat(I(ot) === qt ? Ye(t, {
                        placement: ot,
                        boundary: l,
                        rootBoundary: u,
                        padding: p,
                        flipVariations: E,
                        allowedAutoPlacements: m
                    }) : ot)
                }, []), _ = t.rects.reference, v = t.rects.popper, y = new Map, C = !0, H = g[0], $ = 0; $ < g.length; $++) {
                    var w = g[$]
                        , j = I(w)
                        , W = q(w) === Q
                        , Vt = [S, L].indexOf(j) >= 0
                        , kt = Vt ? "width" : "height"
                        , Y = k(t, {
                        placement: w,
                        boundary: l,
                        rootBoundary: u,
                        altBoundary: d,
                        padding: p
                    })
                        , U = Vt ? W ? P : N : W ? L : S;
                    _[kt] > v[kt] && (U = Rt(U));
                    var re = Rt(U)
                        , st = [];
                    if (i && st.push(Y[j] <= 0),
                    s && st.push(Y[U] <= 0, Y[re] <= 0),
                        st.every(function(bt) {
                            return bt
                        })) {
                        H = w,
                            C = !1;
                        break
                    }
                    y.set(w, st)
                }
                if (C)
                    for (var oe = E ? 3 : 1, be = function(ot) {
                        var Wt = g.find(function(ie) {
                            var ft = y.get(ie);
                            if (ft)
                                return ft.slice(0, ot).every(function(xe) {
                                    return xe
                                })
                        });
                        if (Wt)
                            return H = Wt,
                                "break"
                    }, jt = oe; jt > 0; jt--) {
                        var ne = be(jt);
                        if (ne === "break")
                            break
                    }
                t.placement !== H && (t.modifiersData[o]._skip = !0,
                    t.placement = H,
                    t.reset = !0)
            }
        }
        var de = {
            name: "flip",
            enabled: !0,
            phase: "main",
            fn: an,
            requiresIfExists: ["offset"],
            data: {
                _skip: !1
            }
        };
        function qr(e, t, r) {
            return r === void 0 && (r = {
                x: 0,
                y: 0
            }),
                {
                    top: e.top - t.height - r.y,
                    right: e.right - t.width + r.x,
                    bottom: e.bottom - t.height + r.y,
                    left: e.left - t.width - r.x
                }
        }
        function Ur(e) {
            return [S, P, L, N].some(function(t) {
                return e[t] >= 0
            })
        }
        function sn(e) {
            var t = e.state
                , r = e.name
                , o = t.rects.reference
                , n = t.rects.popper
                , i = t.modifiersData.preventOverflow
                , a = k(t, {
                elementContext: "reference"
            })
                , s = k(t, {
                altBoundary: !0
            })
                , f = qr(a, o)
                , p = qr(s, n, i)
                , l = Ur(f)
                , u = Ur(p);
            t.modifiersData[r] = {
                referenceClippingOffsets: f,
                popperEscapeOffsets: p,
                isReferenceHidden: l,
                hasPopperEscaped: u
            },
                t.attributes.popper = Object.assign({}, t.attributes.popper, {
                    "data-popper-reference-hidden": l,
                    "data-popper-escaped": u
                })
        }
        var he = {
            name: "hide",
            enabled: !0,
            phase: "main",
            requiresIfExists: ["preventOverflow"],
            fn: sn
        };
        function fn(e, t, r) {
            var o = I(e)
                , n = [N, S].indexOf(o) >= 0 ? -1 : 1
                , i = typeof r == "function" ? r(Object.assign({}, t, {
                placement: e
            })) : r
                , a = i[0]
                , s = i[1];
            return a = a || 0,
                s = (s || 0) * n,
                [N, P].indexOf(o) >= 0 ? {
                    x: s,
                    y: a
                } : {
                    x: a,
                    y: s
                }
        }
        function ln(e) {
            var t = e.state
                , r = e.options
                , o = e.name
                , n = r.offset
                , i = n === void 0 ? [0, 0] : n
                , a = zt.reduce(function(l, u) {
                return l[u] = fn(u, t.rects, i),
                    l
            }, {})
                , s = a[t.placement]
                , f = s.x
                , p = s.y;
            t.modifiersData.popperOffsets != null && (t.modifiersData.popperOffsets.x += f,
                t.modifiersData.popperOffsets.y += p),
                t.modifiersData[o] = a
        }
        var ge = {
            name: "offset",
            enabled: !0,
            phase: "main",
            requires: ["popperOffsets"],
            fn: ln
        };
        function pn(e) {
            var t = e.state
                , r = e.name;
            t.modifiersData[r] = Zt({
                reference: t.rects.reference,
                element: t.rects.popper,
                strategy: "absolute",
                placement: t.placement
            })
        }
        var Ht = {
            name: "popperOffsets",
            enabled: !0,
            phase: "read",
            fn: pn,
            data: {}
        };
        function Fe(e) {
            return e === "x" ? "y" : "x"
        }
        function cn(e) {
            var t = e.state
                , r = e.options
                , o = e.name
                , n = r.mainAxis
                , i = n === void 0 ? !0 : n
                , a = r.altAxis
                , s = a === void 0 ? !1 : a
                , f = r.boundary
                , p = r.rootBoundary
                , l = r.altBoundary
                , u = r.padding
                , d = r.tether
                , c = d === void 0 ? !0 : d
                , E = r.tetherOffset
                , m = E === void 0 ? 0 : E
                , h = k(t, {
                boundary: f,
                rootBoundary: p,
                padding: u,
                altBoundary: l
            })
                , O = I(t.placement)
                , A = q(t.placement)
                , T = !A
                , g = ht(O)
                , _ = Fe(g)
                , v = t.modifiersData.popperOffsets
                , y = t.rects.reference
                , C = t.rects.popper
                , H = typeof m == "function" ? m(Object.assign({}, t.rects, {
                placement: t.placement
            })) : m
                , $ = typeof H == "number" ? {
                mainAxis: H,
                altAxis: H
            } : Object.assign({
                mainAxis: 0,
                altAxis: 0
            }, H)
                , w = t.modifiersData.offset ? t.modifiersData.offset[t.placement] : null
                , j = {
                x: 0,
                y: 0
            };
            if (!!v) {
                if (i) {
                    var W, Vt = g === "y" ? S : N, kt = g === "y" ? L : P, Y = g === "y" ? "height" : "width", U = v[g], re = U + h[Vt], st = U - h[kt], oe = c ? -C[Y] / 2 : 0, be = A === Q ? y[Y] : C[Y], jt = A === Q ? -C[Y] : -y[Y], ne = t.elements.arrow, bt = c && ne ? dt(ne) : {
                        width: 0,
                        height: 0
                    }, ot = t.modifiersData["arrow#persistent"] ? t.modifiersData["arrow#persistent"].padding : Gt(), Wt = ot[Vt], ie = ot[kt], ft = gt(0, y[Y], bt[Y]), xe = T ? y[Y] / 2 - oe - ft - Wt - $.mainAxis : be - ft - Wt - $.mainAxis, so = T ? -y[Y] / 2 + oe + ft + ie + $.mainAxis : jt + ft + ie + $.mainAxis, Oe = t.elements.arrow && X(t.elements.arrow), fo = Oe ? g === "y" ? Oe.clientTop || 0 : Oe.clientLeft || 0 : 0, nr = (W = w == null ? void 0 : w[g]) != null ? W : 0, lo = U + xe - nr - fo, po = U + so - nr, ir = gt(c ? mt(re, lo) : re, U, c ? z(st, po) : st);
                    v[g] = ir,
                        j[g] = ir - U
                }
                if (s) {
                    var ar, co = g === "x" ? S : N, uo = g === "x" ? L : P, lt = v[_], ae = _ === "y" ? "height" : "width", sr = lt + h[co], fr = lt - h[uo], Ae = [S, N].indexOf(O) !== -1, lr = (ar = w == null ? void 0 : w[_]) != null ? ar : 0, pr = Ae ? sr : lt - y[ae] - C[ae] - lr + $.altAxis, cr = Ae ? lt + y[ae] + C[ae] - lr - $.altAxis : fr, ur = c && Ae ? Yr(pr, lt, cr) : gt(c ? pr : sr, lt, c ? cr : fr);
                    v[_] = ur,
                        j[_] = ur - lt
                }
                t.modifiersData[o] = j
            }
        }
        var ve = {
            name: "preventOverflow",
            enabled: !0,
            phase: "main",
            fn: cn,
            requiresIfExists: ["offset"]
        };
        function Ke(e) {
            return {
                scrollLeft: e.scrollLeft,
                scrollTop: e.scrollTop
            }
        }
        function qe(e) {
            return e === D(e) || !M(e) ? vt(e) : Ke(e)
        }
        function un(e) {
            var t = e.getBoundingClientRect()
                , r = et(t.width) / e.offsetWidth || 1
                , o = et(t.height) / e.offsetHeight || 1;
            return r !== 1 || o !== 1
        }
        function Ue(e, t, r) {
            r === void 0 && (r = !1);
            var o = M(t)
                , n = M(t) && un(t)
                , i = B(t)
                , a = K(e, n)
                , s = {
                scrollLeft: 0,
                scrollTop: 0
            }
                , f = {
                x: 0,
                y: 0
            };
            return (o || !o && !r) && ((R(t) !== "body" || _t(i)) && (s = qe(t)),
                M(t) ? (f = K(t, !0),
                    f.x += t.clientLeft,
                    f.y += t.clientTop) : i && (f.x = Et(i))),
                {
                    x: a.left + s.scrollLeft - f.x,
                    y: a.top + s.scrollTop - f.y,
                    width: a.width,
                    height: a.height
                }
        }
        function mn(e) {
            var t = new Map
                , r = new Set
                , o = [];
            e.forEach(function(i) {
                t.set(i.name, i)
            });
            function n(i) {
                r.add(i.name);
                var a = [].concat(i.requires || [], i.requiresIfExists || []);
                a.forEach(function(s) {
                    if (!r.has(s)) {
                        var f = t.get(s);
                        f && n(f)
                    }
                }),
                    o.push(i)
            }
            return e.forEach(function(i) {
                r.has(i.name) || n(i)
            }),
                o
        }
        function ze(e) {
            var t = mn(e);
            return Be.reduce(function(r, o) {
                return r.concat(t.filter(function(n) {
                    return n.phase === o
                }))
            }, [])
        }
        function Xe(e) {
            var t;
            return function() {
                return t || (t = new Promise(function(r) {
                        Promise.resolve().then(function() {
                            t = void 0,
                                r(e())
                        })
                    }
                )),
                    t
            }
        }
        function Ge(e) {
            var t = e.reduce(function(r, o) {
                var n = r[o.name];
                return r[o.name] = n ? Object.assign({}, n, o, {
                    options: Object.assign({}, n.options, o.options),
                    data: Object.assign({}, n.data, o.data)
                }) : o,
                    r
            }, {});
            return Object.keys(t).map(function(r) {
                return t[r]
            })
        }
        var zr = {
            placement: "bottom",
            modifiers: [],
            strategy: "absolute"
        };
        function Xr() {
            for (var e = arguments.length, t = new Array(e), r = 0; r < e; r++)
                t[r] = arguments[r];
            return !t.some(function(o) {
                return !(o && typeof o.getBoundingClientRect == "function")
            })
        }
        function yt(e) {
            e === void 0 && (e = {});
            var t = e
                , r = t.defaultModifiers
                , o = r === void 0 ? [] : r
                , n = t.defaultOptions
                , i = n === void 0 ? zr : n;
            return function(s, f, p) {
                p === void 0 && (p = i);
                var l = {
                    placement: "bottom",
                    orderedModifiers: [],
                    options: Object.assign({}, zr, i),
                    modifiersData: {},
                    elements: {
                        reference: s,
                        popper: f
                    },
                    attributes: {},
                    styles: {}
                }
                    , u = []
                    , d = !1
                    , c = {
                    state: l,
                    setOptions: function(O) {
                        var A = typeof O == "function" ? O(l.options) : O;
                        m(),
                            l.options = Object.assign({}, i, l.options, A),
                            l.scrollParents = {
                                reference: J(s) ? at(s) : s.contextElement ? at(s.contextElement) : [],
                                popper: at(f)
                            };
                        var T = ze(Ge([].concat(o, l.options.modifiers)));
                        if (l.orderedModifiers = T.filter(function(w) {
                            return w.enabled
                        }),
                            !1) {
                            var g;
                            if (getBasePlacement(l.options.placement) === auto)
                                var _;
                            var v, y, C, H, $
                        }
                        return E(),
                            c.update()
                    },
                    forceUpdate: function() {
                        if (!d) {
                            var O = l.elements
                                , A = O.reference
                                , T = O.popper;
                            if (!!Xr(A, T)) {
                                l.rects = {
                                    reference: Ue(A, X(T), l.options.strategy === "fixed"),
                                    popper: dt(T)
                                },
                                    l.reset = !1,
                                    l.placement = l.options.placement,
                                    l.orderedModifiers.forEach(function(w) {
                                        return l.modifiersData[w.name] = Object.assign({}, w.data)
                                    });
                                for (var g = 0, _ = 0; _ < l.orderedModifiers.length; _++) {
                                    if (l.reset === !0) {
                                        l.reset = !1,
                                            _ = -1;
                                        continue
                                    }
                                    var v = l.orderedModifiers[_]
                                        , y = v.fn
                                        , C = v.options
                                        , H = C === void 0 ? {} : C
                                        , $ = v.name;
                                    typeof y == "function" && (l = y({
                                        state: l,
                                        options: H,
                                        name: $,
                                        instance: c
                                    }) || l)
                                }
                            }
                        }
                    },
                    update: Xe(function() {
                        return new Promise(function(h) {
                                c.forceUpdate(),
                                    h(l)
                            }
                        )
                    }),
                    destroy: function() {
                        m(),
                            d = !0
                    }
                };
                if (!Xr(s, f))
                    return c;
                c.setOptions(p).then(function(h) {
                    !d && p.onFirstUpdate && p.onFirstUpdate(h)
                });
                function E() {
                    l.orderedModifiers.forEach(function(h) {
                        var O = h.name
                            , A = h.options
                            , T = A === void 0 ? {} : A
                            , g = h.effect;
                        if (typeof g == "function") {
                            var _ = g({
                                state: l,
                                name: O,
                                instance: c,
                                options: T
                            })
                                , v = function() {};
                            u.push(_ || v)
                        }
                    })
                }
                function m() {
                    u.forEach(function(h) {
                        return h()
                    }),
                        u = []
                }
                return c
            }
        }
        var Gr = yt();
        var dn = [Mt, Ht, Lt, Dt]
            , Qr = yt({
            defaultModifiers: dn
        });
        var hn = [Mt, Ht, Lt, Dt, ge, de, ve, pe, he]
            , Ee = yt({
            defaultModifiers: hn
        });
        var Je = "dropdown"
            , gn = "bs.dropdown"
            , wt = `.${gn}`
            , Ze = ".data-api"
            , _e = "Escape"
            , Jr = "Space"
            , Zr = "Tab"
            , tr = "ArrowUp"
            , ye = "ArrowDown"
            , vn = 2
            , En = new RegExp(`${tr}|${ye}|${_e}`)
            , _n = `hide${wt}`
            , yn = `hidden${wt}`
            , wn = `show${wt}`
            , bn = `shown${wt}`
            , to = `click${wt}${Ze}`
            , eo = `keydown${wt}${Ze}`
            , xn = `keyup${wt}${Ze}`
            , $t = "show"
            , On = "dropup"
            , An = "dropend"
            , Tn = "dropstart"
            , Sn = "navbar"
            , te = '[data-bs-toggle="dropdown"]'
            , er = ".dropdown-menu"
            , Nn = ".navbar-nav"
            , Cn = ".dropdown-menu .dropdown-item:not(.disabled):not(:disabled)"
            , Pn = pt() ? "top-end" : "top-start"
            , Dn = pt() ? "top-start" : "top-end"
            , Ln = pt() ? "bottom-end" : "bottom-start"
            , Mn = pt() ? "bottom-start" : "bottom-end"
            , Rn = pt() ? "left-start" : "right-start"
            , In = pt() ? "right-start" : "left-start"
            , Hn = {
                offset: [0, 2],
                boundary: "clippingParents",
                reference: "toggle",
                display: "dynamic",
                popperConfig: null,
                autoClose: !0
            }
            , $n = {
                offset: "(array|string|function)",
                boundary: "(string|element)",
                reference: "(string|element|object)",
                display: "string",
                popperConfig: "(null|object|function)",
                autoClose: "(boolean|string)"
            }
            , F = class extends Nt {
                constructor(t, r) {
                    super(t);
                    this._popper = null,
                        this._config = this._getConfig(r),
                        this._menu = this._getMenuElement(),
                        this._inNavbar = this._detectNavbar()
                }
                static get Default() {
                    return Hn
                }
                static get DefaultType() {
                    return $n
                }
                static get NAME() {
                    return Je
                }
                toggle() {
                    return this._isShown() ? this.hide() : this.show()
                }
                show() {
                    if (At(this._element) || this._isShown(this._menu))
                        return;
                    let t = {
                        relatedTarget: this._element
                    };
                    if (b.trigger(this._element, wn, t).defaultPrevented)
                        return;
                    let o = F.getParentFromElement(this._element);
                    this._inNavbar ? G.setDataAttribute(this._menu, "popper", "none") : this._createPopper(o),
                    "ontouchstart"in document.documentElement && !o.closest(Nn) && [].concat(...document.body.children).forEach(n=>b.on(n, "mouseover", Se)),
                        this._element.focus(),
                        this._element.setAttribute("aria-expanded", !0),
                        this._menu.classList.add($t),
                        this._element.classList.add($t),
                        b.trigger(this._element, bn, t)
                }
                hide() {
                    if (At(this._element) || !this._isShown(this._menu))
                        return;
                    let t = {
                        relatedTarget: this._element
                    };
                    this._completeHide(t)
                }
                dispose() {
                    this._popper && this._popper.destroy(),
                        super.dispose()
                }
                update() {
                    this._inNavbar = this._detectNavbar(),
                    this._popper && this._popper.update()
                }
                _completeHide(t) {
                    b.trigger(this._element, _n, t).defaultPrevented || ("ontouchstart"in document.documentElement && [].concat(...document.body.children).forEach(o=>b.off(o, "mouseover", Se)),
                    this._popper && this._popper.destroy(),
                        this._menu.classList.remove($t),
                        this._element.classList.remove($t),
                        this._element.setAttribute("aria-expanded", "false"),
                        G.removeDataAttribute(this._menu, "popper"),
                        b.trigger(this._element, yn, t))
                }
                _getConfig(t) {
                    if (t = {
                        ...this.constructor.Default,
                        ...G.getDataAttributes(this._element),
                        ...t
                    },
                        Ot(Je, t, this.constructor.DefaultType),
                    typeof t.reference == "object" && !xt(t.reference) && typeof t.reference.getBoundingClientRect != "function")
                        throw new TypeError(`${Je.toUpperCase()}: Option "reference" provided type "object" without a required "getBoundingClientRect" method.`);
                    return t
                }
                _createPopper(t) {
                    if (typeof Qe == "undefined")
                        throw new TypeError("Bootstrap's dropdowns require Popper (https://popper.js.org)");
                    let r = this._element;
                    this._config.reference === "parent" ? r = t : xt(this._config.reference) ? r = Z(this._config.reference) : typeof this._config.reference == "object" && (r = this._config.reference);
                    let o = this._getPopperConfig()
                        , n = o.modifiers.find(i=>i.name === "applyStyles" && i.enabled === !1);
                    this._popper = Ee(r, this._menu, o),
                    n && G.setDataAttribute(this._menu, "popper", "static")
                }
                _isShown(t=this._element) {
                    return t.classList.contains($t)
                }
                _getMenuElement() {
                    return x.next(this._element, er)[0]
                }
                _getPlacement() {
                    let t = this._element.parentNode;
                    if (t.classList.contains(An))
                        return Rn;
                    if (t.classList.contains(Tn))
                        return In;
                    let r = getComputedStyle(this._menu).getPropertyValue("--bs-position").trim() === "end";
                    return t.classList.contains(On) ? r ? Dn : Pn : r ? Mn : Ln
                }
                _detectNavbar() {
                    return this._element.closest(`.${Sn}`) !== null
                }
                _getOffset() {
                    let {offset: t} = this._config;
                    return typeof t == "string" ? t.split(",").map(r=>Number.parseInt(r, 10)) : typeof t == "function" ? r=>t(r, this._element) : t
                }
                _getPopperConfig() {
                    let t = {
                        placement: this._getPlacement(),
                        modifiers: [{
                            name: "preventOverflow",
                            options: {
                                boundary: this._config.boundary
                            }
                        }, {
                            name: "offset",
                            options: {
                                offset: this._getOffset()
                            }
                        }]
                    };
                    return this._config.display === "static" && (t.modifiers = [{
                        name: "applyStyles",
                        enabled: !1
                    }]),
                        {
                            ...t,
                            ...typeof this._config.popperConfig == "function" ? this._config.popperConfig(t) : this._config.popperConfig
                        }
                }
                _selectMenuItem({key: t, target: r}) {
                    let o = x.find(Cn, this._menu).filter(se);
                    !o.length || vr(o, r, t === ye, !o.includes(r)).focus()
                }
                static jQueryInterface(t) {
                    return this.each(function() {
                        let r = F.getOrCreateInstance(this, t);
                        if (typeof t == "string") {
                            if (typeof r[t] == "undefined")
                                throw new TypeError(`No method named "${t}"`);
                            r[t]()
                        }
                    })
                }
                static clearMenus(t) {
                    if (t && (t.button === vn || t.type === "keyup" && t.key !== Zr))
                        return;
                    let r = x.find(te);
                    for (let o = 0, n = r.length; o < n; o++) {
                        let i = F.getInstance(r[o]);
                        if (!i || i._config.autoClose === !1 || !i._isShown())
                            continue;
                        let a = {
                            relatedTarget: i._element
                        };
                        if (t) {
                            let s = t.composedPath()
                                , f = s.includes(i._menu);
                            if (s.includes(i._element) || i._config.autoClose === "inside" && !f || i._config.autoClose === "outside" && f || i._menu.contains(t.target) && (t.type === "keyup" && t.key === Zr || /input|select|option|textarea|form/i.test(t.target.tagName)))
                                continue;
                            t.type === "click" && (a.clickEvent = t)
                        }
                        i._completeHide(a)
                    }
                }
                static getParentFromElement(t) {
                    return Ft(t) || t.parentNode
                }
                static dataApiKeydownHandler(t) {
                    if (/input|textarea/i.test(t.target.tagName) ? t.key === Jr || t.key !== _e && (t.key !== ye && t.key !== tr || t.target.closest(er)) : !En.test(t.key))
                        return;
                    let r = this.classList.contains($t);
                    if (!r && t.key === _e || (t.preventDefault(),
                        t.stopPropagation(),
                        At(this)))
                        return;
                    let o = this.matches(te) ? this : x.prev(this, te)[0]
                        , n = F.getOrCreateInstance(o);
                    if (t.key === _e) {
                        n.hide();
                        return
                    }
                    if (t.key === tr || t.key === ye) {
                        r || n.show(),
                            n._selectMenuItem(t);
                        return
                    }
                    (!r || t.key === Jr) && F.clearMenus()
                }
            }
        ;
        b.on(document, eo, te, F.dataApiKeydownHandler);
        b.on(document, eo, er, F.dataApiKeydownHandler);
        b.on(document, to, F.clearMenus);
        b.on(document, xn, F.clearMenus);
        b.on(document, to, te, function(e) {
            e.preventDefault(),
                F.getOrCreateInstance(this).toggle()
        });
        Tt(F);
        var ro = "scrollspy"
            , Bn = "bs.scrollspy"
            , we = `.${Bn}`
            , Vn = ".data-api"
            , oo = {
                offset: 10,
                method: "auto",
                target: ""
            }
            , kn = {
                offset: "number",
                method: "string",
                target: "(string|element)"
            }
            , jn = `activate${we}`
            , Wn = `scroll${we}`
            , Yn = `load${we}${Vn}`
            , no = "dropdown-item"
            , Bt = "active"
            , Fn = '[data-bs-spy="scroll"]'
            , Kn = ".nav, .list-group"
            , rr = ".nav-link"
            , qn = ".nav-item"
            , io = ".list-group-item"
            , or = `${rr}, ${io}, .${no}`
            , Un = ".dropdown"
            , zn = ".dropdown-toggle"
            , Xn = "offset"
            , ao = "position"
            , ee = class extends Nt {
                constructor(t, r) {
                    super(t);
                    this._scrollElement = this._element.tagName === "BODY" ? window : this._element,
                        this._config = this._getConfig(r),
                        this._offsets = [],
                        this._targets = [],
                        this._activeTarget = null,
                        this._scrollHeight = 0,
                        b.on(this._scrollElement, Wn, ()=>this._process()),
                        this.refresh(),
                        this._process()
                }
                static get Default() {
                    return oo
                }
                static get NAME() {
                    return ro
                }
                refresh() {
                    let t = this._scrollElement === this._scrollElement.window ? Xn : ao
                        , r = this._config.method === "auto" ? t : this._config.method
                        , o = r === ao ? this._getScrollTop() : 0;
                    this._offsets = [],
                        this._targets = [],
                        this._scrollHeight = this._getScrollHeight(),
                        x.find(or, this._config.target).map(i=>{
                                let a = Yt(i)
                                    , s = a ? x.findOne(a) : null;
                                if (s) {
                                    let f = s.getBoundingClientRect();
                                    if (f.width || f.height)
                                        return [G[r](s).top + o, a]
                                }
                                return null
                            }
                        ).filter(i=>i).sort((i,a)=>i[0] - a[0]).forEach(i=>{
                                this._offsets.push(i[0]),
                                    this._targets.push(i[1])
                            }
                        )
                }
                dispose() {
                    b.off(this._scrollElement, we),
                        super.dispose()
                }
                _getConfig(t) {
                    return t = {
                        ...oo,
                        ...G.getDataAttributes(this._element),
                        ...typeof t == "object" && t ? t : {}
                    },
                        t.target = Z(t.target) || document.documentElement,
                        Ot(ro, t, kn),
                        t
                }
                _getScrollTop() {
                    return this._scrollElement === window ? this._scrollElement.pageYOffset : this._scrollElement.scrollTop
                }
                _getScrollHeight() {
                    return this._scrollElement.scrollHeight || Math.max(document.body.scrollHeight, document.documentElement.scrollHeight)
                }
                _getOffsetHeight() {
                    return this._scrollElement === window ? window.innerHeight : this._scrollElement.getBoundingClientRect().height
                }
                _process() {
                    let t = this._getScrollTop() + this._config.offset
                        , r = this._getScrollHeight()
                        , o = this._config.offset + r - this._getOffsetHeight();
                    if (this._scrollHeight !== r && this.refresh(),
                    t >= o) {
                        let n = this._targets[this._targets.length - 1];
                        this._activeTarget !== n && this._activate(n);
                        return
                    }
                    if (this._activeTarget && t < this._offsets[0] && this._offsets[0] > 0) {
                        this._activeTarget = null,
                            this._clear();
                        return
                    }
                    for (let n = this._offsets.length; n--; )
                        this._activeTarget !== this._targets[n] && t >= this._offsets[n] && (typeof this._offsets[n + 1] == "undefined" || t < this._offsets[n + 1]) && this._activate(this._targets[n])
                }
                _activate(t) {
                    this._activeTarget = t,
                        this._clear();
                    let r = or.split(",").map(n=>`${n}[data-bs-target="${t}"],${n}[href="${t}"]`)
                        , o = x.findOne(r.join(","), this._config.target);
                    o.classList.add(Bt),
                        o.classList.contains(no) ? x.findOne(zn, o.closest(Un)).classList.add(Bt) : x.parents(o, Kn).forEach(n=>{
                                x.prev(n, `${rr}, ${io}`).forEach(i=>i.classList.add(Bt)),
                                    x.prev(n, qn).forEach(i=>{
                                            x.children(i, rr).forEach(a=>a.classList.add(Bt))
                                        }
                                    )
                            }
                        ),
                        b.trigger(this._scrollElement, jn, {
                            relatedTarget: t
                        })
                }
                _clear() {
                    x.find(or, this._config.target).filter(t=>t.classList.contains(Bt)).forEach(t=>t.classList.remove(Bt))
                }
                static jQueryInterface(t) {
                    return this.each(function() {
                        let r = ee.getOrCreateInstance(this, t);
                        if (typeof t == "string") {
                            if (typeof r[t] == "undefined")
                                throw new TypeError(`No method named "${t}"`);
                            r[t]()
                        }
                    })
                }
            }
        ;
        b.on(window, Yn, ()=>{
                x.find(Fn).forEach(e=>new ee(e))
            }
        );
        Tt(ee);
    }
)();