function CI(t,e){for(var n=0;n<e.length;n++){const r=e[n];if(typeof r!="string"&&!Array.isArray(r)){for(const i in r)if(i!=="default"&&!(i in t)){const s=Object.getOwnPropertyDescriptor(r,i);s&&Object.defineProperty(t,i,s.get?s:{enumerable:!0,get:()=>r[i]})}}}return Object.freeze(Object.defineProperty(t,Symbol.toStringTag,{value:"Module"}))}(function(){const e=document.createElement("link").relList;if(e&&e.supports&&e.supports("modulepreload"))return;for(const i of document.querySelectorAll('link[rel="modulepreload"]'))r(i);new MutationObserver(i=>{for(const s of i)if(s.type==="childList")for(const o of s.addedNodes)o.tagName==="LINK"&&o.rel==="modulepreload"&&r(o)}).observe(document,{childList:!0,subtree:!0});function n(i){const s={};return i.integrity&&(s.integrity=i.integrity),i.referrerPolicy&&(s.referrerPolicy=i.referrerPolicy),i.crossOrigin==="use-credentials"?s.credentials="include":i.crossOrigin==="anonymous"?s.credentials="omit":s.credentials="same-origin",s}function r(i){if(i.ep)return;i.ep=!0;const s=n(i);fetch(i.href,s)}})();function _0(t){return t&&t.__esModule&&Object.prototype.hasOwnProperty.call(t,"default")?t.default:t}var w0={exports:{}},nc={},E0={exports:{}},te={};/**
 * @license React
 * react.production.min.js
 *
 * Copyright (c) Facebook, Inc. and its affiliates.
 *
 * This source code is licensed under the MIT license found in the
 * LICENSE file in the root directory of this source tree.
 */var Ea=Symbol.for("react.element"),NI=Symbol.for("react.portal"),bI=Symbol.for("react.fragment"),OI=Symbol.for("react.strict_mode"),DI=Symbol.for("react.profiler"),MI=Symbol.for("react.provider"),LI=Symbol.for("react.context"),jI=Symbol.for("react.forward_ref"),VI=Symbol.for("react.suspense"),FI=Symbol.for("react.memo"),UI=Symbol.for("react.lazy"),ug=Symbol.iterator;function $I(t){return t===null||typeof t!="object"?null:(t=ug&&t[ug]||t["@@iterator"],typeof t=="function"?t:null)}var T0={isMounted:function(){return!1},enqueueForceUpdate:function(){},enqueueReplaceState:function(){},enqueueSetState:function(){}},I0=Object.assign,S0={};function Ds(t,e,n){this.props=t,this.context=e,this.refs=S0,this.updater=n||T0}Ds.prototype.isReactComponent={};Ds.prototype.setState=function(t,e){if(typeof t!="object"&&typeof t!="function"&&t!=null)throw Error("setState(...): takes an object of state variables to update or a function which returns an object of state variables.");this.updater.enqueueSetState(this,t,e,"setState")};Ds.prototype.forceUpdate=function(t){this.updater.enqueueForceUpdate(this,t,"forceUpdate")};function k0(){}k0.prototype=Ds.prototype;function Hd(t,e,n){this.props=t,this.context=e,this.refs=S0,this.updater=n||T0}var Wd=Hd.prototype=new k0;Wd.constructor=Hd;I0(Wd,Ds.prototype);Wd.isPureReactComponent=!0;var cg=Array.isArray,A0=Object.prototype.hasOwnProperty,Kd={current:null},x0={key:!0,ref:!0,__self:!0,__source:!0};function P0(t,e,n){var r,i={},s=null,o=null;if(e!=null)for(r in e.ref!==void 0&&(o=e.ref),e.key!==void 0&&(s=""+e.key),e)A0.call(e,r)&&!x0.hasOwnProperty(r)&&(i[r]=e[r]);var a=arguments.length-2;if(a===1)i.children=n;else if(1<a){for(var l=Array(a),u=0;u<a;u++)l[u]=arguments[u+2];i.children=l}if(t&&t.defaultProps)for(r in a=t.defaultProps,a)i[r]===void 0&&(i[r]=a[r]);return{$$typeof:Ea,type:t,key:s,ref:o,props:i,_owner:Kd.current}}function zI(t,e){return{$$typeof:Ea,type:t.type,key:e,ref:t.ref,props:t.props,_owner:t._owner}}function Gd(t){return typeof t=="object"&&t!==null&&t.$$typeof===Ea}function BI(t){var e={"=":"=0",":":"=2"};return"$"+t.replace(/[=:]/g,function(n){return e[n]})}var fg=/\/+/g;function af(t,e){return typeof t=="object"&&t!==null&&t.key!=null?BI(""+t.key):e.toString(36)}function jl(t,e,n,r,i){var s=typeof t;(s==="undefined"||s==="boolean")&&(t=null);var o=!1;if(t===null)o=!0;else switch(s){case"string":case"number":o=!0;break;case"object":switch(t.$$typeof){case Ea:case NI:o=!0}}if(o)return o=t,i=i(o),t=r===""?"."+af(o,0):r,cg(i)?(n="",t!=null&&(n=t.replace(fg,"$&/")+"/"),jl(i,e,n,"",function(u){return u})):i!=null&&(Gd(i)&&(i=zI(i,n+(!i.key||o&&o.key===i.key?"":(""+i.key).replace(fg,"$&/")+"/")+t)),e.push(i)),1;if(o=0,r=r===""?".":r+":",cg(t))for(var a=0;a<t.length;a++){s=t[a];var l=r+af(s,a);o+=jl(s,e,n,l,i)}else if(l=$I(t),typeof l=="function")for(t=l.call(t),a=0;!(s=t.next()).done;)s=s.value,l=r+af(s,a++),o+=jl(s,e,n,l,i);else if(s==="object")throw e=String(t),Error("Objects are not valid as a React child (found: "+(e==="[object Object]"?"object with keys {"+Object.keys(t).join(", ")+"}":e)+"). If you meant to render a collection of children, use an array instead.");return o}function el(t,e,n){if(t==null)return t;var r=[],i=0;return jl(t,r,"","",function(s){return e.call(n,s,i++)}),r}function HI(t){if(t._status===-1){var e=t._result;e=e(),e.then(function(n){(t._status===0||t._status===-1)&&(t._status=1,t._result=n)},function(n){(t._status===0||t._status===-1)&&(t._status=2,t._result=n)}),t._status===-1&&(t._status=0,t._result=e)}if(t._status===1)return t._result.default;throw t._result}var St={current:null},Vl={transition:null},WI={ReactCurrentDispatcher:St,ReactCurrentBatchConfig:Vl,ReactCurrentOwner:Kd};te.Children={map:el,forEach:function(t,e,n){el(t,function(){e.apply(this,arguments)},n)},count:function(t){var e=0;return el(t,function(){e++}),e},toArray:function(t){return el(t,function(e){return e})||[]},only:function(t){if(!Gd(t))throw Error("React.Children.only expected to receive a single React element child.");return t}};te.Component=Ds;te.Fragment=bI;te.Profiler=DI;te.PureComponent=Hd;te.StrictMode=OI;te.Suspense=VI;te.__SECRET_INTERNALS_DO_NOT_USE_OR_YOU_WILL_BE_FIRED=WI;te.cloneElement=function(t,e,n){if(t==null)throw Error("React.cloneElement(...): The argument must be a React element, but you passed "+t+".");var r=I0({},t.props),i=t.key,s=t.ref,o=t._owner;if(e!=null){if(e.ref!==void 0&&(s=e.ref,o=Kd.current),e.key!==void 0&&(i=""+e.key),t.type&&t.type.defaultProps)var a=t.type.defaultProps;for(l in e)A0.call(e,l)&&!x0.hasOwnProperty(l)&&(r[l]=e[l]===void 0&&a!==void 0?a[l]:e[l])}var l=arguments.length-2;if(l===1)r.children=n;else if(1<l){a=Array(l);for(var u=0;u<l;u++)a[u]=arguments[u+2];r.children=a}return{$$typeof:Ea,type:t.type,key:i,ref:s,props:r,_owner:o}};te.createContext=function(t){return t={$$typeof:LI,_currentValue:t,_currentValue2:t,_threadCount:0,Provider:null,Consumer:null,_defaultValue:null,_globalName:null},t.Provider={$$typeof:MI,_context:t},t.Consumer=t};te.createElement=P0;te.createFactory=function(t){var e=P0.bind(null,t);return e.type=t,e};te.createRef=function(){return{current:null}};te.forwardRef=function(t){return{$$typeof:jI,render:t}};te.isValidElement=Gd;te.lazy=function(t){return{$$typeof:UI,_payload:{_status:-1,_result:t},_init:HI}};te.memo=function(t,e){return{$$typeof:FI,type:t,compare:e===void 0?null:e}};te.startTransition=function(t){var e=Vl.transition;Vl.transition={};try{t()}finally{Vl.transition=e}};te.unstable_act=function(){throw Error("act(...) is not supported in production builds of React.")};te.useCallback=function(t,e){return St.current.useCallback(t,e)};te.useContext=function(t){return St.current.useContext(t)};te.useDebugValue=function(){};te.useDeferredValue=function(t){return St.current.useDeferredValue(t)};te.useEffect=function(t,e){return St.current.useEffect(t,e)};te.useId=function(){return St.current.useId()};te.useImperativeHandle=function(t,e,n){return St.current.useImperativeHandle(t,e,n)};te.useInsertionEffect=function(t,e){return St.current.useInsertionEffect(t,e)};te.useLayoutEffect=function(t,e){return St.current.useLayoutEffect(t,e)};te.useMemo=function(t,e){return St.current.useMemo(t,e)};te.useReducer=function(t,e,n){return St.current.useReducer(t,e,n)};te.useRef=function(t){return St.current.useRef(t)};te.useState=function(t){return St.current.useState(t)};te.useSyncExternalStore=function(t,e,n){return St.current.useSyncExternalStore(t,e,n)};te.useTransition=function(){return St.current.useTransition()};te.version="18.2.0";E0.exports=te;var N=E0.exports;const rc=_0(N),KI=CI({__proto__:null,default:rc},[N]);/**
 * @license React
 * react-jsx-runtime.production.min.js
 *
 * Copyright (c) Facebook, Inc. and its affiliates.
 *
 * This source code is licensed under the MIT license found in the
 * LICENSE file in the root directory of this source tree.
 */var GI=N,qI=Symbol.for("react.element"),QI=Symbol.for("react.fragment"),YI=Object.prototype.hasOwnProperty,XI=GI.__SECRET_INTERNALS_DO_NOT_USE_OR_YOU_WILL_BE_FIRED.ReactCurrentOwner,JI={key:!0,ref:!0,__self:!0,__source:!0};function R0(t,e,n){var r,i={},s=null,o=null;n!==void 0&&(s=""+n),e.key!==void 0&&(s=""+e.key),e.ref!==void 0&&(o=e.ref);for(r in e)YI.call(e,r)&&!JI.hasOwnProperty(r)&&(i[r]=e[r]);if(t&&t.defaultProps)for(r in e=t.defaultProps,e)i[r]===void 0&&(i[r]=e[r]);return{$$typeof:qI,type:t,key:s,ref:o,props:i,_owner:XI.current}}nc.Fragment=QI;nc.jsx=R0;nc.jsxs=R0;w0.exports=nc;var f=w0.exports,sh={},C0={exports:{}},Ft={},N0={exports:{}},b0={};/**
 * @license React
 * scheduler.production.min.js
 *
 * Copyright (c) Facebook, Inc. and its affiliates.
 *
 * This source code is licensed under the MIT license found in the
 * LICENSE file in the root directory of this source tree.
 */(function(t){function e(D,H){var X=D.length;D.push(H);e:for(;0<X;){var Ie=X-1>>>1,Oe=D[Ie];if(0<i(Oe,H))D[Ie]=H,D[X]=Oe,X=Ie;else break e}}function n(D){return D.length===0?null:D[0]}function r(D){if(D.length===0)return null;var H=D[0],X=D.pop();if(X!==H){D[0]=X;e:for(var Ie=0,Oe=D.length,bi=Oe>>>1;Ie<bi;){var fe=2*(Ie+1)-1,Pn=D[fe],dn=fe+1,Oi=D[dn];if(0>i(Pn,X))dn<Oe&&0>i(Oi,Pn)?(D[Ie]=Oi,D[dn]=X,Ie=dn):(D[Ie]=Pn,D[fe]=X,Ie=fe);else if(dn<Oe&&0>i(Oi,X))D[Ie]=Oi,D[dn]=X,Ie=dn;else break e}}return H}function i(D,H){var X=D.sortIndex-H.sortIndex;return X!==0?X:D.id-H.id}if(typeof performance=="object"&&typeof performance.now=="function"){var s=performance;t.unstable_now=function(){return s.now()}}else{var o=Date,a=o.now();t.unstable_now=function(){return o.now()-a}}var l=[],u=[],c=1,h=null,d=3,y=!1,_=!1,w=!1,T=typeof setTimeout=="function"?setTimeout:null,g=typeof clearTimeout=="function"?clearTimeout:null,p=typeof setImmediate<"u"?setImmediate:null;typeof navigator<"u"&&navigator.scheduling!==void 0&&navigator.scheduling.isInputPending!==void 0&&navigator.scheduling.isInputPending.bind(navigator.scheduling);function v(D){for(var H=n(u);H!==null;){if(H.callback===null)r(u);else if(H.startTime<=D)r(u),H.sortIndex=H.expirationTime,e(l,H);else break;H=n(u)}}function m(D){if(w=!1,v(D),!_)if(n(l)!==null)_=!0,Ot(S);else{var H=n(u);H!==null&&hn(m,H.startTime-D)}}function S(D,H){_=!1,w&&(w=!1,g(O),O=-1),y=!0;var X=d;try{for(v(H),h=n(l);h!==null&&(!(h.expirationTime>H)||D&&!He());){var Ie=h.callback;if(typeof Ie=="function"){h.callback=null,d=h.priorityLevel;var Oe=Ie(h.expirationTime<=H);H=t.unstable_now(),typeof Oe=="function"?h.callback=Oe:h===n(l)&&r(l),v(H)}else r(l);h=n(l)}if(h!==null)var bi=!0;else{var fe=n(u);fe!==null&&hn(m,fe.startTime-H),bi=!1}return bi}finally{h=null,d=X,y=!1}}var P=!1,A=null,O=-1,B=5,U=-1;function He(){return!(t.unstable_now()-U<B)}function ce(){if(A!==null){var D=t.unstable_now();U=D;var H=!0;try{H=A(!0,D)}finally{H?fn():(P=!1,A=null)}}else P=!1}var fn;if(typeof p=="function")fn=function(){p(ce)};else if(typeof MessageChannel<"u"){var rr=new MessageChannel,ir=rr.port2;rr.port1.onmessage=ce,fn=function(){ir.postMessage(null)}}else fn=function(){T(ce,0)};function Ot(D){A=D,P||(P=!0,fn())}function hn(D,H){O=T(function(){D(t.unstable_now())},H)}t.unstable_IdlePriority=5,t.unstable_ImmediatePriority=1,t.unstable_LowPriority=4,t.unstable_NormalPriority=3,t.unstable_Profiling=null,t.unstable_UserBlockingPriority=2,t.unstable_cancelCallback=function(D){D.callback=null},t.unstable_continueExecution=function(){_||y||(_=!0,Ot(S))},t.unstable_forceFrameRate=function(D){0>D||125<D?console.error("forceFrameRate takes a positive int between 0 and 125, forcing frame rates higher than 125 fps is not supported"):B=0<D?Math.floor(1e3/D):5},t.unstable_getCurrentPriorityLevel=function(){return d},t.unstable_getFirstCallbackNode=function(){return n(l)},t.unstable_next=function(D){switch(d){case 1:case 2:case 3:var H=3;break;default:H=d}var X=d;d=H;try{return D()}finally{d=X}},t.unstable_pauseExecution=function(){},t.unstable_requestPaint=function(){},t.unstable_runWithPriority=function(D,H){switch(D){case 1:case 2:case 3:case 4:case 5:break;default:D=3}var X=d;d=D;try{return H()}finally{d=X}},t.unstable_scheduleCallback=function(D,H,X){var Ie=t.unstable_now();switch(typeof X=="object"&&X!==null?(X=X.delay,X=typeof X=="number"&&0<X?Ie+X:Ie):X=Ie,D){case 1:var Oe=-1;break;case 2:Oe=250;break;case 5:Oe=1073741823;break;case 4:Oe=1e4;break;default:Oe=5e3}return Oe=X+Oe,D={id:c++,callback:H,priorityLevel:D,startTime:X,expirationTime:Oe,sortIndex:-1},X>Ie?(D.sortIndex=X,e(u,D),n(l)===null&&D===n(u)&&(w?(g(O),O=-1):w=!0,hn(m,X-Ie))):(D.sortIndex=Oe,e(l,D),_||y||(_=!0,Ot(S))),D},t.unstable_shouldYield=He,t.unstable_wrapCallback=function(D){var H=d;return function(){var X=d;d=H;try{return D.apply(this,arguments)}finally{d=X}}}})(b0);N0.exports=b0;var ZI=N0.exports;/**
 * @license React
 * react-dom.production.min.js
 *
 * Copyright (c) Facebook, Inc. and its affiliates.
 *
 * This source code is licensed under the MIT license found in the
 * LICENSE file in the root directory of this source tree.
 */var O0=N,jt=ZI;function x(t){for(var e="https://reactjs.org/docs/error-decoder.html?invariant="+t,n=1;n<arguments.length;n++)e+="&args[]="+encodeURIComponent(arguments[n]);return"Minified React error #"+t+"; visit "+e+" for the full message or use the non-minified dev environment for full errors and additional helpful warnings."}var D0=new Set,Oo={};function xi(t,e){gs(t,e),gs(t+"Capture",e)}function gs(t,e){for(Oo[t]=e,t=0;t<e.length;t++)D0.add(e[t])}var zn=!(typeof window>"u"||typeof window.document>"u"||typeof window.document.createElement>"u"),oh=Object.prototype.hasOwnProperty,e2=/^[:A-Z_a-z\u00C0-\u00D6\u00D8-\u00F6\u00F8-\u02FF\u0370-\u037D\u037F-\u1FFF\u200C-\u200D\u2070-\u218F\u2C00-\u2FEF\u3001-\uD7FF\uF900-\uFDCF\uFDF0-\uFFFD][:A-Z_a-z\u00C0-\u00D6\u00D8-\u00F6\u00F8-\u02FF\u0370-\u037D\u037F-\u1FFF\u200C-\u200D\u2070-\u218F\u2C00-\u2FEF\u3001-\uD7FF\uF900-\uFDCF\uFDF0-\uFFFD\-.0-9\u00B7\u0300-\u036F\u203F-\u2040]*$/,hg={},dg={};function t2(t){return oh.call(dg,t)?!0:oh.call(hg,t)?!1:e2.test(t)?dg[t]=!0:(hg[t]=!0,!1)}function n2(t,e,n,r){if(n!==null&&n.type===0)return!1;switch(typeof e){case"function":case"symbol":return!0;case"boolean":return r?!1:n!==null?!n.acceptsBooleans:(t=t.toLowerCase().slice(0,5),t!=="data-"&&t!=="aria-");default:return!1}}function r2(t,e,n,r){if(e===null||typeof e>"u"||n2(t,e,n,r))return!0;if(r)return!1;if(n!==null)switch(n.type){case 3:return!e;case 4:return e===!1;case 5:return isNaN(e);case 6:return isNaN(e)||1>e}return!1}function kt(t,e,n,r,i,s,o){this.acceptsBooleans=e===2||e===3||e===4,this.attributeName=r,this.attributeNamespace=i,this.mustUseProperty=n,this.propertyName=t,this.type=e,this.sanitizeURL=s,this.removeEmptyString=o}var at={};"children dangerouslySetInnerHTML defaultValue defaultChecked innerHTML suppressContentEditableWarning suppressHydrationWarning style".split(" ").forEach(function(t){at[t]=new kt(t,0,!1,t,null,!1,!1)});[["acceptCharset","accept-charset"],["className","class"],["htmlFor","for"],["httpEquiv","http-equiv"]].forEach(function(t){var e=t[0];at[e]=new kt(e,1,!1,t[1],null,!1,!1)});["contentEditable","draggable","spellCheck","value"].forEach(function(t){at[t]=new kt(t,2,!1,t.toLowerCase(),null,!1,!1)});["autoReverse","externalResourcesRequired","focusable","preserveAlpha"].forEach(function(t){at[t]=new kt(t,2,!1,t,null,!1,!1)});"allowFullScreen async autoFocus autoPlay controls default defer disabled disablePictureInPicture disableRemotePlayback formNoValidate hidden loop noModule noValidate open playsInline readOnly required reversed scoped seamless itemScope".split(" ").forEach(function(t){at[t]=new kt(t,3,!1,t.toLowerCase(),null,!1,!1)});["checked","multiple","muted","selected"].forEach(function(t){at[t]=new kt(t,3,!0,t,null,!1,!1)});["capture","download"].forEach(function(t){at[t]=new kt(t,4,!1,t,null,!1,!1)});["cols","rows","size","span"].forEach(function(t){at[t]=new kt(t,6,!1,t,null,!1,!1)});["rowSpan","start"].forEach(function(t){at[t]=new kt(t,5,!1,t.toLowerCase(),null,!1,!1)});var qd=/[\-:]([a-z])/g;function Qd(t){return t[1].toUpperCase()}"accent-height alignment-baseline arabic-form baseline-shift cap-height clip-path clip-rule color-interpolation color-interpolation-filters color-profile color-rendering dominant-baseline enable-background fill-opacity fill-rule flood-color flood-opacity font-family font-size font-size-adjust font-stretch font-style font-variant font-weight glyph-name glyph-orientation-horizontal glyph-orientation-vertical horiz-adv-x horiz-origin-x image-rendering letter-spacing lighting-color marker-end marker-mid marker-start overline-position overline-thickness paint-order panose-1 pointer-events rendering-intent shape-rendering stop-color stop-opacity strikethrough-position strikethrough-thickness stroke-dasharray stroke-dashoffset stroke-linecap stroke-linejoin stroke-miterlimit stroke-opacity stroke-width text-anchor text-decoration text-rendering underline-position underline-thickness unicode-bidi unicode-range units-per-em v-alphabetic v-hanging v-ideographic v-mathematical vector-effect vert-adv-y vert-origin-x vert-origin-y word-spacing writing-mode xmlns:xlink x-height".split(" ").forEach(function(t){var e=t.replace(qd,Qd);at[e]=new kt(e,1,!1,t,null,!1,!1)});"xlink:actuate xlink:arcrole xlink:role xlink:show xlink:title xlink:type".split(" ").forEach(function(t){var e=t.replace(qd,Qd);at[e]=new kt(e,1,!1,t,"http://www.w3.org/1999/xlink",!1,!1)});["xml:base","xml:lang","xml:space"].forEach(function(t){var e=t.replace(qd,Qd);at[e]=new kt(e,1,!1,t,"http://www.w3.org/XML/1998/namespace",!1,!1)});["tabIndex","crossOrigin"].forEach(function(t){at[t]=new kt(t,1,!1,t.toLowerCase(),null,!1,!1)});at.xlinkHref=new kt("xlinkHref",1,!1,"xlink:href","http://www.w3.org/1999/xlink",!0,!1);["src","href","action","formAction"].forEach(function(t){at[t]=new kt(t,1,!1,t.toLowerCase(),null,!0,!0)});function Yd(t,e,n,r){var i=at.hasOwnProperty(e)?at[e]:null;(i!==null?i.type!==0:r||!(2<e.length)||e[0]!=="o"&&e[0]!=="O"||e[1]!=="n"&&e[1]!=="N")&&(r2(e,n,i,r)&&(n=null),r||i===null?t2(e)&&(n===null?t.removeAttribute(e):t.setAttribute(e,""+n)):i.mustUseProperty?t[i.propertyName]=n===null?i.type===3?!1:"":n:(e=i.attributeName,r=i.attributeNamespace,n===null?t.removeAttribute(e):(i=i.type,n=i===3||i===4&&n===!0?"":""+n,r?t.setAttributeNS(r,e,n):t.setAttribute(e,n))))}var Zn=O0.__SECRET_INTERNALS_DO_NOT_USE_OR_YOU_WILL_BE_FIRED,tl=Symbol.for("react.element"),Vi=Symbol.for("react.portal"),Fi=Symbol.for("react.fragment"),Xd=Symbol.for("react.strict_mode"),ah=Symbol.for("react.profiler"),M0=Symbol.for("react.provider"),L0=Symbol.for("react.context"),Jd=Symbol.for("react.forward_ref"),lh=Symbol.for("react.suspense"),uh=Symbol.for("react.suspense_list"),Zd=Symbol.for("react.memo"),fr=Symbol.for("react.lazy"),j0=Symbol.for("react.offscreen"),pg=Symbol.iterator;function Gs(t){return t===null||typeof t!="object"?null:(t=pg&&t[pg]||t["@@iterator"],typeof t=="function"?t:null)}var xe=Object.assign,lf;function ao(t){if(lf===void 0)try{throw Error()}catch(n){var e=n.stack.trim().match(/\n( *(at )?)/);lf=e&&e[1]||""}return`
`+lf+t}var uf=!1;function cf(t,e){if(!t||uf)return"";uf=!0;var n=Error.prepareStackTrace;Error.prepareStackTrace=void 0;try{if(e)if(e=function(){throw Error()},Object.defineProperty(e.prototype,"props",{set:function(){throw Error()}}),typeof Reflect=="object"&&Reflect.construct){try{Reflect.construct(e,[])}catch(u){var r=u}Reflect.construct(t,[],e)}else{try{e.call()}catch(u){r=u}t.call(e.prototype)}else{try{throw Error()}catch(u){r=u}t()}}catch(u){if(u&&r&&typeof u.stack=="string"){for(var i=u.stack.split(`
`),s=r.stack.split(`
`),o=i.length-1,a=s.length-1;1<=o&&0<=a&&i[o]!==s[a];)a--;for(;1<=o&&0<=a;o--,a--)if(i[o]!==s[a]){if(o!==1||a!==1)do if(o--,a--,0>a||i[o]!==s[a]){var l=`
`+i[o].replace(" at new "," at ");return t.displayName&&l.includes("<anonymous>")&&(l=l.replace("<anonymous>",t.displayName)),l}while(1<=o&&0<=a);break}}}finally{uf=!1,Error.prepareStackTrace=n}return(t=t?t.displayName||t.name:"")?ao(t):""}function i2(t){switch(t.tag){case 5:return ao(t.type);case 16:return ao("Lazy");case 13:return ao("Suspense");case 19:return ao("SuspenseList");case 0:case 2:case 15:return t=cf(t.type,!1),t;case 11:return t=cf(t.type.render,!1),t;case 1:return t=cf(t.type,!0),t;default:return""}}function ch(t){if(t==null)return null;if(typeof t=="function")return t.displayName||t.name||null;if(typeof t=="string")return t;switch(t){case Fi:return"Fragment";case Vi:return"Portal";case ah:return"Profiler";case Xd:return"StrictMode";case lh:return"Suspense";case uh:return"SuspenseList"}if(typeof t=="object")switch(t.$$typeof){case L0:return(t.displayName||"Context")+".Consumer";case M0:return(t._context.displayName||"Context")+".Provider";case Jd:var e=t.render;return t=t.displayName,t||(t=e.displayName||e.name||"",t=t!==""?"ForwardRef("+t+")":"ForwardRef"),t;case Zd:return e=t.displayName||null,e!==null?e:ch(t.type)||"Memo";case fr:e=t._payload,t=t._init;try{return ch(t(e))}catch{}}return null}function s2(t){var e=t.type;switch(t.tag){case 24:return"Cache";case 9:return(e.displayName||"Context")+".Consumer";case 10:return(e._context.displayName||"Context")+".Provider";case 18:return"DehydratedFragment";case 11:return t=e.render,t=t.displayName||t.name||"",e.displayName||(t!==""?"ForwardRef("+t+")":"ForwardRef");case 7:return"Fragment";case 5:return e;case 4:return"Portal";case 3:return"Root";case 6:return"Text";case 16:return ch(e);case 8:return e===Xd?"StrictMode":"Mode";case 22:return"Offscreen";case 12:return"Profiler";case 21:return"Scope";case 13:return"Suspense";case 19:return"SuspenseList";case 25:return"TracingMarker";case 1:case 0:case 17:case 2:case 14:case 15:if(typeof e=="function")return e.displayName||e.name||null;if(typeof e=="string")return e}return null}function Or(t){switch(typeof t){case"boolean":case"number":case"string":case"undefined":return t;case"object":return t;default:return""}}function V0(t){var e=t.type;return(t=t.nodeName)&&t.toLowerCase()==="input"&&(e==="checkbox"||e==="radio")}function o2(t){var e=V0(t)?"checked":"value",n=Object.getOwnPropertyDescriptor(t.constructor.prototype,e),r=""+t[e];if(!t.hasOwnProperty(e)&&typeof n<"u"&&typeof n.get=="function"&&typeof n.set=="function"){var i=n.get,s=n.set;return Object.defineProperty(t,e,{configurable:!0,get:function(){return i.call(this)},set:function(o){r=""+o,s.call(this,o)}}),Object.defineProperty(t,e,{enumerable:n.enumerable}),{getValue:function(){return r},setValue:function(o){r=""+o},stopTracking:function(){t._valueTracker=null,delete t[e]}}}}function nl(t){t._valueTracker||(t._valueTracker=o2(t))}function F0(t){if(!t)return!1;var e=t._valueTracker;if(!e)return!0;var n=e.getValue(),r="";return t&&(r=V0(t)?t.checked?"true":"false":t.value),t=r,t!==n?(e.setValue(t),!0):!1}function ru(t){if(t=t||(typeof document<"u"?document:void 0),typeof t>"u")return null;try{return t.activeElement||t.body}catch{return t.body}}function fh(t,e){var n=e.checked;return xe({},e,{defaultChecked:void 0,defaultValue:void 0,value:void 0,checked:n??t._wrapperState.initialChecked})}function mg(t,e){var n=e.defaultValue==null?"":e.defaultValue,r=e.checked!=null?e.checked:e.defaultChecked;n=Or(e.value!=null?e.value:n),t._wrapperState={initialChecked:r,initialValue:n,controlled:e.type==="checkbox"||e.type==="radio"?e.checked!=null:e.value!=null}}function U0(t,e){e=e.checked,e!=null&&Yd(t,"checked",e,!1)}function hh(t,e){U0(t,e);var n=Or(e.value),r=e.type;if(n!=null)r==="number"?(n===0&&t.value===""||t.value!=n)&&(t.value=""+n):t.value!==""+n&&(t.value=""+n);else if(r==="submit"||r==="reset"){t.removeAttribute("value");return}e.hasOwnProperty("value")?dh(t,e.type,n):e.hasOwnProperty("defaultValue")&&dh(t,e.type,Or(e.defaultValue)),e.checked==null&&e.defaultChecked!=null&&(t.defaultChecked=!!e.defaultChecked)}function gg(t,e,n){if(e.hasOwnProperty("value")||e.hasOwnProperty("defaultValue")){var r=e.type;if(!(r!=="submit"&&r!=="reset"||e.value!==void 0&&e.value!==null))return;e=""+t._wrapperState.initialValue,n||e===t.value||(t.value=e),t.defaultValue=e}n=t.name,n!==""&&(t.name=""),t.defaultChecked=!!t._wrapperState.initialChecked,n!==""&&(t.name=n)}function dh(t,e,n){(e!=="number"||ru(t.ownerDocument)!==t)&&(n==null?t.defaultValue=""+t._wrapperState.initialValue:t.defaultValue!==""+n&&(t.defaultValue=""+n))}var lo=Array.isArray;function rs(t,e,n,r){if(t=t.options,e){e={};for(var i=0;i<n.length;i++)e["$"+n[i]]=!0;for(n=0;n<t.length;n++)i=e.hasOwnProperty("$"+t[n].value),t[n].selected!==i&&(t[n].selected=i),i&&r&&(t[n].defaultSelected=!0)}else{for(n=""+Or(n),e=null,i=0;i<t.length;i++){if(t[i].value===n){t[i].selected=!0,r&&(t[i].defaultSelected=!0);return}e!==null||t[i].disabled||(e=t[i])}e!==null&&(e.selected=!0)}}function ph(t,e){if(e.dangerouslySetInnerHTML!=null)throw Error(x(91));return xe({},e,{value:void 0,defaultValue:void 0,children:""+t._wrapperState.initialValue})}function vg(t,e){var n=e.value;if(n==null){if(n=e.children,e=e.defaultValue,n!=null){if(e!=null)throw Error(x(92));if(lo(n)){if(1<n.length)throw Error(x(93));n=n[0]}e=n}e==null&&(e=""),n=e}t._wrapperState={initialValue:Or(n)}}function $0(t,e){var n=Or(e.value),r=Or(e.defaultValue);n!=null&&(n=""+n,n!==t.value&&(t.value=n),e.defaultValue==null&&t.defaultValue!==n&&(t.defaultValue=n)),r!=null&&(t.defaultValue=""+r)}function yg(t){var e=t.textContent;e===t._wrapperState.initialValue&&e!==""&&e!==null&&(t.value=e)}function z0(t){switch(t){case"svg":return"http://www.w3.org/2000/svg";case"math":return"http://www.w3.org/1998/Math/MathML";default:return"http://www.w3.org/1999/xhtml"}}function mh(t,e){return t==null||t==="http://www.w3.org/1999/xhtml"?z0(e):t==="http://www.w3.org/2000/svg"&&e==="foreignObject"?"http://www.w3.org/1999/xhtml":t}var rl,B0=function(t){return typeof MSApp<"u"&&MSApp.execUnsafeLocalFunction?function(e,n,r,i){MSApp.execUnsafeLocalFunction(function(){return t(e,n,r,i)})}:t}(function(t,e){if(t.namespaceURI!=="http://www.w3.org/2000/svg"||"innerHTML"in t)t.innerHTML=e;else{for(rl=rl||document.createElement("div"),rl.innerHTML="<svg>"+e.valueOf().toString()+"</svg>",e=rl.firstChild;t.firstChild;)t.removeChild(t.firstChild);for(;e.firstChild;)t.appendChild(e.firstChild)}});function Do(t,e){if(e){var n=t.firstChild;if(n&&n===t.lastChild&&n.nodeType===3){n.nodeValue=e;return}}t.textContent=e}var mo={animationIterationCount:!0,aspectRatio:!0,borderImageOutset:!0,borderImageSlice:!0,borderImageWidth:!0,boxFlex:!0,boxFlexGroup:!0,boxOrdinalGroup:!0,columnCount:!0,columns:!0,flex:!0,flexGrow:!0,flexPositive:!0,flexShrink:!0,flexNegative:!0,flexOrder:!0,gridArea:!0,gridRow:!0,gridRowEnd:!0,gridRowSpan:!0,gridRowStart:!0,gridColumn:!0,gridColumnEnd:!0,gridColumnSpan:!0,gridColumnStart:!0,fontWeight:!0,lineClamp:!0,lineHeight:!0,opacity:!0,order:!0,orphans:!0,tabSize:!0,widows:!0,zIndex:!0,zoom:!0,fillOpacity:!0,floodOpacity:!0,stopOpacity:!0,strokeDasharray:!0,strokeDashoffset:!0,strokeMiterlimit:!0,strokeOpacity:!0,strokeWidth:!0},a2=["Webkit","ms","Moz","O"];Object.keys(mo).forEach(function(t){a2.forEach(function(e){e=e+t.charAt(0).toUpperCase()+t.substring(1),mo[e]=mo[t]})});function H0(t,e,n){return e==null||typeof e=="boolean"||e===""?"":n||typeof e!="number"||e===0||mo.hasOwnProperty(t)&&mo[t]?(""+e).trim():e+"px"}function W0(t,e){t=t.style;for(var n in e)if(e.hasOwnProperty(n)){var r=n.indexOf("--")===0,i=H0(n,e[n],r);n==="float"&&(n="cssFloat"),r?t.setProperty(n,i):t[n]=i}}var l2=xe({menuitem:!0},{area:!0,base:!0,br:!0,col:!0,embed:!0,hr:!0,img:!0,input:!0,keygen:!0,link:!0,meta:!0,param:!0,source:!0,track:!0,wbr:!0});function gh(t,e){if(e){if(l2[t]&&(e.children!=null||e.dangerouslySetInnerHTML!=null))throw Error(x(137,t));if(e.dangerouslySetInnerHTML!=null){if(e.children!=null)throw Error(x(60));if(typeof e.dangerouslySetInnerHTML!="object"||!("__html"in e.dangerouslySetInnerHTML))throw Error(x(61))}if(e.style!=null&&typeof e.style!="object")throw Error(x(62))}}function vh(t,e){if(t.indexOf("-")===-1)return typeof e.is=="string";switch(t){case"annotation-xml":case"color-profile":case"font-face":case"font-face-src":case"font-face-uri":case"font-face-format":case"font-face-name":case"missing-glyph":return!1;default:return!0}}var yh=null;function ep(t){return t=t.target||t.srcElement||window,t.correspondingUseElement&&(t=t.correspondingUseElement),t.nodeType===3?t.parentNode:t}var _h=null,is=null,ss=null;function _g(t){if(t=Sa(t)){if(typeof _h!="function")throw Error(x(280));var e=t.stateNode;e&&(e=lc(e),_h(t.stateNode,t.type,e))}}function K0(t){is?ss?ss.push(t):ss=[t]:is=t}function G0(){if(is){var t=is,e=ss;if(ss=is=null,_g(t),e)for(t=0;t<e.length;t++)_g(e[t])}}function q0(t,e){return t(e)}function Q0(){}var ff=!1;function Y0(t,e,n){if(ff)return t(e,n);ff=!0;try{return q0(t,e,n)}finally{ff=!1,(is!==null||ss!==null)&&(Q0(),G0())}}function Mo(t,e){var n=t.stateNode;if(n===null)return null;var r=lc(n);if(r===null)return null;n=r[e];e:switch(e){case"onClick":case"onClickCapture":case"onDoubleClick":case"onDoubleClickCapture":case"onMouseDown":case"onMouseDownCapture":case"onMouseMove":case"onMouseMoveCapture":case"onMouseUp":case"onMouseUpCapture":case"onMouseEnter":(r=!r.disabled)||(t=t.type,r=!(t==="button"||t==="input"||t==="select"||t==="textarea")),t=!r;break e;default:t=!1}if(t)return null;if(n&&typeof n!="function")throw Error(x(231,e,typeof n));return n}var wh=!1;if(zn)try{var qs={};Object.defineProperty(qs,"passive",{get:function(){wh=!0}}),window.addEventListener("test",qs,qs),window.removeEventListener("test",qs,qs)}catch{wh=!1}function u2(t,e,n,r,i,s,o,a,l){var u=Array.prototype.slice.call(arguments,3);try{e.apply(n,u)}catch(c){this.onError(c)}}var go=!1,iu=null,su=!1,Eh=null,c2={onError:function(t){go=!0,iu=t}};function f2(t,e,n,r,i,s,o,a,l){go=!1,iu=null,u2.apply(c2,arguments)}function h2(t,e,n,r,i,s,o,a,l){if(f2.apply(this,arguments),go){if(go){var u=iu;go=!1,iu=null}else throw Error(x(198));su||(su=!0,Eh=u)}}function Pi(t){var e=t,n=t;if(t.alternate)for(;e.return;)e=e.return;else{t=e;do e=t,e.flags&4098&&(n=e.return),t=e.return;while(t)}return e.tag===3?n:null}function X0(t){if(t.tag===13){var e=t.memoizedState;if(e===null&&(t=t.alternate,t!==null&&(e=t.memoizedState)),e!==null)return e.dehydrated}return null}function wg(t){if(Pi(t)!==t)throw Error(x(188))}function d2(t){var e=t.alternate;if(!e){if(e=Pi(t),e===null)throw Error(x(188));return e!==t?null:t}for(var n=t,r=e;;){var i=n.return;if(i===null)break;var s=i.alternate;if(s===null){if(r=i.return,r!==null){n=r;continue}break}if(i.child===s.child){for(s=i.child;s;){if(s===n)return wg(i),t;if(s===r)return wg(i),e;s=s.sibling}throw Error(x(188))}if(n.return!==r.return)n=i,r=s;else{for(var o=!1,a=i.child;a;){if(a===n){o=!0,n=i,r=s;break}if(a===r){o=!0,r=i,n=s;break}a=a.sibling}if(!o){for(a=s.child;a;){if(a===n){o=!0,n=s,r=i;break}if(a===r){o=!0,r=s,n=i;break}a=a.sibling}if(!o)throw Error(x(189))}}if(n.alternate!==r)throw Error(x(190))}if(n.tag!==3)throw Error(x(188));return n.stateNode.current===n?t:e}function J0(t){return t=d2(t),t!==null?Z0(t):null}function Z0(t){if(t.tag===5||t.tag===6)return t;for(t=t.child;t!==null;){var e=Z0(t);if(e!==null)return e;t=t.sibling}return null}var e1=jt.unstable_scheduleCallback,Eg=jt.unstable_cancelCallback,p2=jt.unstable_shouldYield,m2=jt.unstable_requestPaint,be=jt.unstable_now,g2=jt.unstable_getCurrentPriorityLevel,tp=jt.unstable_ImmediatePriority,t1=jt.unstable_UserBlockingPriority,ou=jt.unstable_NormalPriority,v2=jt.unstable_LowPriority,n1=jt.unstable_IdlePriority,ic=null,En=null;function y2(t){if(En&&typeof En.onCommitFiberRoot=="function")try{En.onCommitFiberRoot(ic,t,void 0,(t.current.flags&128)===128)}catch{}}var ln=Math.clz32?Math.clz32:E2,_2=Math.log,w2=Math.LN2;function E2(t){return t>>>=0,t===0?32:31-(_2(t)/w2|0)|0}var il=64,sl=4194304;function uo(t){switch(t&-t){case 1:return 1;case 2:return 2;case 4:return 4;case 8:return 8;case 16:return 16;case 32:return 32;case 64:case 128:case 256:case 512:case 1024:case 2048:case 4096:case 8192:case 16384:case 32768:case 65536:case 131072:case 262144:case 524288:case 1048576:case 2097152:return t&4194240;case 4194304:case 8388608:case 16777216:case 33554432:case 67108864:return t&130023424;case 134217728:return 134217728;case 268435456:return 268435456;case 536870912:return 536870912;case 1073741824:return 1073741824;default:return t}}function au(t,e){var n=t.pendingLanes;if(n===0)return 0;var r=0,i=t.suspendedLanes,s=t.pingedLanes,o=n&268435455;if(o!==0){var a=o&~i;a!==0?r=uo(a):(s&=o,s!==0&&(r=uo(s)))}else o=n&~i,o!==0?r=uo(o):s!==0&&(r=uo(s));if(r===0)return 0;if(e!==0&&e!==r&&!(e&i)&&(i=r&-r,s=e&-e,i>=s||i===16&&(s&4194240)!==0))return e;if(r&4&&(r|=n&16),e=t.entangledLanes,e!==0)for(t=t.entanglements,e&=r;0<e;)n=31-ln(e),i=1<<n,r|=t[n],e&=~i;return r}function T2(t,e){switch(t){case 1:case 2:case 4:return e+250;case 8:case 16:case 32:case 64:case 128:case 256:case 512:case 1024:case 2048:case 4096:case 8192:case 16384:case 32768:case 65536:case 131072:case 262144:case 524288:case 1048576:case 2097152:return e+5e3;case 4194304:case 8388608:case 16777216:case 33554432:case 67108864:return-1;case 134217728:case 268435456:case 536870912:case 1073741824:return-1;default:return-1}}function I2(t,e){for(var n=t.suspendedLanes,r=t.pingedLanes,i=t.expirationTimes,s=t.pendingLanes;0<s;){var o=31-ln(s),a=1<<o,l=i[o];l===-1?(!(a&n)||a&r)&&(i[o]=T2(a,e)):l<=e&&(t.expiredLanes|=a),s&=~a}}function Th(t){return t=t.pendingLanes&-1073741825,t!==0?t:t&1073741824?1073741824:0}function r1(){var t=il;return il<<=1,!(il&4194240)&&(il=64),t}function hf(t){for(var e=[],n=0;31>n;n++)e.push(t);return e}function Ta(t,e,n){t.pendingLanes|=e,e!==536870912&&(t.suspendedLanes=0,t.pingedLanes=0),t=t.eventTimes,e=31-ln(e),t[e]=n}function S2(t,e){var n=t.pendingLanes&~e;t.pendingLanes=e,t.suspendedLanes=0,t.pingedLanes=0,t.expiredLanes&=e,t.mutableReadLanes&=e,t.entangledLanes&=e,e=t.entanglements;var r=t.eventTimes;for(t=t.expirationTimes;0<n;){var i=31-ln(n),s=1<<i;e[i]=0,r[i]=-1,t[i]=-1,n&=~s}}function np(t,e){var n=t.entangledLanes|=e;for(t=t.entanglements;n;){var r=31-ln(n),i=1<<r;i&e|t[r]&e&&(t[r]|=e),n&=~i}}var oe=0;function i1(t){return t&=-t,1<t?4<t?t&268435455?16:536870912:4:1}var s1,rp,o1,a1,l1,Ih=!1,ol=[],Tr=null,Ir=null,Sr=null,Lo=new Map,jo=new Map,dr=[],k2="mousedown mouseup touchcancel touchend touchstart auxclick dblclick pointercancel pointerdown pointerup dragend dragstart drop compositionend compositionstart keydown keypress keyup input textInput copy cut paste click change contextmenu reset submit".split(" ");function Tg(t,e){switch(t){case"focusin":case"focusout":Tr=null;break;case"dragenter":case"dragleave":Ir=null;break;case"mouseover":case"mouseout":Sr=null;break;case"pointerover":case"pointerout":Lo.delete(e.pointerId);break;case"gotpointercapture":case"lostpointercapture":jo.delete(e.pointerId)}}function Qs(t,e,n,r,i,s){return t===null||t.nativeEvent!==s?(t={blockedOn:e,domEventName:n,eventSystemFlags:r,nativeEvent:s,targetContainers:[i]},e!==null&&(e=Sa(e),e!==null&&rp(e)),t):(t.eventSystemFlags|=r,e=t.targetContainers,i!==null&&e.indexOf(i)===-1&&e.push(i),t)}function A2(t,e,n,r,i){switch(e){case"focusin":return Tr=Qs(Tr,t,e,n,r,i),!0;case"dragenter":return Ir=Qs(Ir,t,e,n,r,i),!0;case"mouseover":return Sr=Qs(Sr,t,e,n,r,i),!0;case"pointerover":var s=i.pointerId;return Lo.set(s,Qs(Lo.get(s)||null,t,e,n,r,i)),!0;case"gotpointercapture":return s=i.pointerId,jo.set(s,Qs(jo.get(s)||null,t,e,n,r,i)),!0}return!1}function u1(t){var e=Jr(t.target);if(e!==null){var n=Pi(e);if(n!==null){if(e=n.tag,e===13){if(e=X0(n),e!==null){t.blockedOn=e,l1(t.priority,function(){o1(n)});return}}else if(e===3&&n.stateNode.current.memoizedState.isDehydrated){t.blockedOn=n.tag===3?n.stateNode.containerInfo:null;return}}}t.blockedOn=null}function Fl(t){if(t.blockedOn!==null)return!1;for(var e=t.targetContainers;0<e.length;){var n=Sh(t.domEventName,t.eventSystemFlags,e[0],t.nativeEvent);if(n===null){n=t.nativeEvent;var r=new n.constructor(n.type,n);yh=r,n.target.dispatchEvent(r),yh=null}else return e=Sa(n),e!==null&&rp(e),t.blockedOn=n,!1;e.shift()}return!0}function Ig(t,e,n){Fl(t)&&n.delete(e)}function x2(){Ih=!1,Tr!==null&&Fl(Tr)&&(Tr=null),Ir!==null&&Fl(Ir)&&(Ir=null),Sr!==null&&Fl(Sr)&&(Sr=null),Lo.forEach(Ig),jo.forEach(Ig)}function Ys(t,e){t.blockedOn===e&&(t.blockedOn=null,Ih||(Ih=!0,jt.unstable_scheduleCallback(jt.unstable_NormalPriority,x2)))}function Vo(t){function e(i){return Ys(i,t)}if(0<ol.length){Ys(ol[0],t);for(var n=1;n<ol.length;n++){var r=ol[n];r.blockedOn===t&&(r.blockedOn=null)}}for(Tr!==null&&Ys(Tr,t),Ir!==null&&Ys(Ir,t),Sr!==null&&Ys(Sr,t),Lo.forEach(e),jo.forEach(e),n=0;n<dr.length;n++)r=dr[n],r.blockedOn===t&&(r.blockedOn=null);for(;0<dr.length&&(n=dr[0],n.blockedOn===null);)u1(n),n.blockedOn===null&&dr.shift()}var os=Zn.ReactCurrentBatchConfig,lu=!0;function P2(t,e,n,r){var i=oe,s=os.transition;os.transition=null;try{oe=1,ip(t,e,n,r)}finally{oe=i,os.transition=s}}function R2(t,e,n,r){var i=oe,s=os.transition;os.transition=null;try{oe=4,ip(t,e,n,r)}finally{oe=i,os.transition=s}}function ip(t,e,n,r){if(lu){var i=Sh(t,e,n,r);if(i===null)Tf(t,e,r,uu,n),Tg(t,r);else if(A2(i,t,e,n,r))r.stopPropagation();else if(Tg(t,r),e&4&&-1<k2.indexOf(t)){for(;i!==null;){var s=Sa(i);if(s!==null&&s1(s),s=Sh(t,e,n,r),s===null&&Tf(t,e,r,uu,n),s===i)break;i=s}i!==null&&r.stopPropagation()}else Tf(t,e,r,null,n)}}var uu=null;function Sh(t,e,n,r){if(uu=null,t=ep(r),t=Jr(t),t!==null)if(e=Pi(t),e===null)t=null;else if(n=e.tag,n===13){if(t=X0(e),t!==null)return t;t=null}else if(n===3){if(e.stateNode.current.memoizedState.isDehydrated)return e.tag===3?e.stateNode.containerInfo:null;t=null}else e!==t&&(t=null);return uu=t,null}function c1(t){switch(t){case"cancel":case"click":case"close":case"contextmenu":case"copy":case"cut":case"auxclick":case"dblclick":case"dragend":case"dragstart":case"drop":case"focusin":case"focusout":case"input":case"invalid":case"keydown":case"keypress":case"keyup":case"mousedown":case"mouseup":case"paste":case"pause":case"play":case"pointercancel":case"pointerdown":case"pointerup":case"ratechange":case"reset":case"resize":case"seeked":case"submit":case"touchcancel":case"touchend":case"touchstart":case"volumechange":case"change":case"selectionchange":case"textInput":case"compositionstart":case"compositionend":case"compositionupdate":case"beforeblur":case"afterblur":case"beforeinput":case"blur":case"fullscreenchange":case"focus":case"hashchange":case"popstate":case"select":case"selectstart":return 1;case"drag":case"dragenter":case"dragexit":case"dragleave":case"dragover":case"mousemove":case"mouseout":case"mouseover":case"pointermove":case"pointerout":case"pointerover":case"scroll":case"toggle":case"touchmove":case"wheel":case"mouseenter":case"mouseleave":case"pointerenter":case"pointerleave":return 4;case"message":switch(g2()){case tp:return 1;case t1:return 4;case ou:case v2:return 16;case n1:return 536870912;default:return 16}default:return 16}}var _r=null,sp=null,Ul=null;function f1(){if(Ul)return Ul;var t,e=sp,n=e.length,r,i="value"in _r?_r.value:_r.textContent,s=i.length;for(t=0;t<n&&e[t]===i[t];t++);var o=n-t;for(r=1;r<=o&&e[n-r]===i[s-r];r++);return Ul=i.slice(t,1<r?1-r:void 0)}function $l(t){var e=t.keyCode;return"charCode"in t?(t=t.charCode,t===0&&e===13&&(t=13)):t=e,t===10&&(t=13),32<=t||t===13?t:0}function al(){return!0}function Sg(){return!1}function Ut(t){function e(n,r,i,s,o){this._reactName=n,this._targetInst=i,this.type=r,this.nativeEvent=s,this.target=o,this.currentTarget=null;for(var a in t)t.hasOwnProperty(a)&&(n=t[a],this[a]=n?n(s):s[a]);return this.isDefaultPrevented=(s.defaultPrevented!=null?s.defaultPrevented:s.returnValue===!1)?al:Sg,this.isPropagationStopped=Sg,this}return xe(e.prototype,{preventDefault:function(){this.defaultPrevented=!0;var n=this.nativeEvent;n&&(n.preventDefault?n.preventDefault():typeof n.returnValue!="unknown"&&(n.returnValue=!1),this.isDefaultPrevented=al)},stopPropagation:function(){var n=this.nativeEvent;n&&(n.stopPropagation?n.stopPropagation():typeof n.cancelBubble!="unknown"&&(n.cancelBubble=!0),this.isPropagationStopped=al)},persist:function(){},isPersistent:al}),e}var Ms={eventPhase:0,bubbles:0,cancelable:0,timeStamp:function(t){return t.timeStamp||Date.now()},defaultPrevented:0,isTrusted:0},op=Ut(Ms),Ia=xe({},Ms,{view:0,detail:0}),C2=Ut(Ia),df,pf,Xs,sc=xe({},Ia,{screenX:0,screenY:0,clientX:0,clientY:0,pageX:0,pageY:0,ctrlKey:0,shiftKey:0,altKey:0,metaKey:0,getModifierState:ap,button:0,buttons:0,relatedTarget:function(t){return t.relatedTarget===void 0?t.fromElement===t.srcElement?t.toElement:t.fromElement:t.relatedTarget},movementX:function(t){return"movementX"in t?t.movementX:(t!==Xs&&(Xs&&t.type==="mousemove"?(df=t.screenX-Xs.screenX,pf=t.screenY-Xs.screenY):pf=df=0,Xs=t),df)},movementY:function(t){return"movementY"in t?t.movementY:pf}}),kg=Ut(sc),N2=xe({},sc,{dataTransfer:0}),b2=Ut(N2),O2=xe({},Ia,{relatedTarget:0}),mf=Ut(O2),D2=xe({},Ms,{animationName:0,elapsedTime:0,pseudoElement:0}),M2=Ut(D2),L2=xe({},Ms,{clipboardData:function(t){return"clipboardData"in t?t.clipboardData:window.clipboardData}}),j2=Ut(L2),V2=xe({},Ms,{data:0}),Ag=Ut(V2),F2={Esc:"Escape",Spacebar:" ",Left:"ArrowLeft",Up:"ArrowUp",Right:"ArrowRight",Down:"ArrowDown",Del:"Delete",Win:"OS",Menu:"ContextMenu",Apps:"ContextMenu",Scroll:"ScrollLock",MozPrintableKey:"Unidentified"},U2={8:"Backspace",9:"Tab",12:"Clear",13:"Enter",16:"Shift",17:"Control",18:"Alt",19:"Pause",20:"CapsLock",27:"Escape",32:" ",33:"PageUp",34:"PageDown",35:"End",36:"Home",37:"ArrowLeft",38:"ArrowUp",39:"ArrowRight",40:"ArrowDown",45:"Insert",46:"Delete",112:"F1",113:"F2",114:"F3",115:"F4",116:"F5",117:"F6",118:"F7",119:"F8",120:"F9",121:"F10",122:"F11",123:"F12",144:"NumLock",145:"ScrollLock",224:"Meta"},$2={Alt:"altKey",Control:"ctrlKey",Meta:"metaKey",Shift:"shiftKey"};function z2(t){var e=this.nativeEvent;return e.getModifierState?e.getModifierState(t):(t=$2[t])?!!e[t]:!1}function ap(){return z2}var B2=xe({},Ia,{key:function(t){if(t.key){var e=F2[t.key]||t.key;if(e!=="Unidentified")return e}return t.type==="keypress"?(t=$l(t),t===13?"Enter":String.fromCharCode(t)):t.type==="keydown"||t.type==="keyup"?U2[t.keyCode]||"Unidentified":""},code:0,location:0,ctrlKey:0,shiftKey:0,altKey:0,metaKey:0,repeat:0,locale:0,getModifierState:ap,charCode:function(t){return t.type==="keypress"?$l(t):0},keyCode:function(t){return t.type==="keydown"||t.type==="keyup"?t.keyCode:0},which:function(t){return t.type==="keypress"?$l(t):t.type==="keydown"||t.type==="keyup"?t.keyCode:0}}),H2=Ut(B2),W2=xe({},sc,{pointerId:0,width:0,height:0,pressure:0,tangentialPressure:0,tiltX:0,tiltY:0,twist:0,pointerType:0,isPrimary:0}),xg=Ut(W2),K2=xe({},Ia,{touches:0,targetTouches:0,changedTouches:0,altKey:0,metaKey:0,ctrlKey:0,shiftKey:0,getModifierState:ap}),G2=Ut(K2),q2=xe({},Ms,{propertyName:0,elapsedTime:0,pseudoElement:0}),Q2=Ut(q2),Y2=xe({},sc,{deltaX:function(t){return"deltaX"in t?t.deltaX:"wheelDeltaX"in t?-t.wheelDeltaX:0},deltaY:function(t){return"deltaY"in t?t.deltaY:"wheelDeltaY"in t?-t.wheelDeltaY:"wheelDelta"in t?-t.wheelDelta:0},deltaZ:0,deltaMode:0}),X2=Ut(Y2),J2=[9,13,27,32],lp=zn&&"CompositionEvent"in window,vo=null;zn&&"documentMode"in document&&(vo=document.documentMode);var Z2=zn&&"TextEvent"in window&&!vo,h1=zn&&(!lp||vo&&8<vo&&11>=vo),Pg=String.fromCharCode(32),Rg=!1;function d1(t,e){switch(t){case"keyup":return J2.indexOf(e.keyCode)!==-1;case"keydown":return e.keyCode!==229;case"keypress":case"mousedown":case"focusout":return!0;default:return!1}}function p1(t){return t=t.detail,typeof t=="object"&&"data"in t?t.data:null}var Ui=!1;function eS(t,e){switch(t){case"compositionend":return p1(e);case"keypress":return e.which!==32?null:(Rg=!0,Pg);case"textInput":return t=e.data,t===Pg&&Rg?null:t;default:return null}}function tS(t,e){if(Ui)return t==="compositionend"||!lp&&d1(t,e)?(t=f1(),Ul=sp=_r=null,Ui=!1,t):null;switch(t){case"paste":return null;case"keypress":if(!(e.ctrlKey||e.altKey||e.metaKey)||e.ctrlKey&&e.altKey){if(e.char&&1<e.char.length)return e.char;if(e.which)return String.fromCharCode(e.which)}return null;case"compositionend":return h1&&e.locale!=="ko"?null:e.data;default:return null}}var nS={color:!0,date:!0,datetime:!0,"datetime-local":!0,email:!0,month:!0,number:!0,password:!0,range:!0,search:!0,tel:!0,text:!0,time:!0,url:!0,week:!0};function Cg(t){var e=t&&t.nodeName&&t.nodeName.toLowerCase();return e==="input"?!!nS[t.type]:e==="textarea"}function m1(t,e,n,r){K0(r),e=cu(e,"onChange"),0<e.length&&(n=new op("onChange","change",null,n,r),t.push({event:n,listeners:e}))}var yo=null,Fo=null;function rS(t){A1(t,0)}function oc(t){var e=Bi(t);if(F0(e))return t}function iS(t,e){if(t==="change")return e}var g1=!1;if(zn){var gf;if(zn){var vf="oninput"in document;if(!vf){var Ng=document.createElement("div");Ng.setAttribute("oninput","return;"),vf=typeof Ng.oninput=="function"}gf=vf}else gf=!1;g1=gf&&(!document.documentMode||9<document.documentMode)}function bg(){yo&&(yo.detachEvent("onpropertychange",v1),Fo=yo=null)}function v1(t){if(t.propertyName==="value"&&oc(Fo)){var e=[];m1(e,Fo,t,ep(t)),Y0(rS,e)}}function sS(t,e,n){t==="focusin"?(bg(),yo=e,Fo=n,yo.attachEvent("onpropertychange",v1)):t==="focusout"&&bg()}function oS(t){if(t==="selectionchange"||t==="keyup"||t==="keydown")return oc(Fo)}function aS(t,e){if(t==="click")return oc(e)}function lS(t,e){if(t==="input"||t==="change")return oc(e)}function uS(t,e){return t===e&&(t!==0||1/t===1/e)||t!==t&&e!==e}var cn=typeof Object.is=="function"?Object.is:uS;function Uo(t,e){if(cn(t,e))return!0;if(typeof t!="object"||t===null||typeof e!="object"||e===null)return!1;var n=Object.keys(t),r=Object.keys(e);if(n.length!==r.length)return!1;for(r=0;r<n.length;r++){var i=n[r];if(!oh.call(e,i)||!cn(t[i],e[i]))return!1}return!0}function Og(t){for(;t&&t.firstChild;)t=t.firstChild;return t}function Dg(t,e){var n=Og(t);t=0;for(var r;n;){if(n.nodeType===3){if(r=t+n.textContent.length,t<=e&&r>=e)return{node:n,offset:e-t};t=r}e:{for(;n;){if(n.nextSibling){n=n.nextSibling;break e}n=n.parentNode}n=void 0}n=Og(n)}}function y1(t,e){return t&&e?t===e?!0:t&&t.nodeType===3?!1:e&&e.nodeType===3?y1(t,e.parentNode):"contains"in t?t.contains(e):t.compareDocumentPosition?!!(t.compareDocumentPosition(e)&16):!1:!1}function _1(){for(var t=window,e=ru();e instanceof t.HTMLIFrameElement;){try{var n=typeof e.contentWindow.location.href=="string"}catch{n=!1}if(n)t=e.contentWindow;else break;e=ru(t.document)}return e}function up(t){var e=t&&t.nodeName&&t.nodeName.toLowerCase();return e&&(e==="input"&&(t.type==="text"||t.type==="search"||t.type==="tel"||t.type==="url"||t.type==="password")||e==="textarea"||t.contentEditable==="true")}function cS(t){var e=_1(),n=t.focusedElem,r=t.selectionRange;if(e!==n&&n&&n.ownerDocument&&y1(n.ownerDocument.documentElement,n)){if(r!==null&&up(n)){if(e=r.start,t=r.end,t===void 0&&(t=e),"selectionStart"in n)n.selectionStart=e,n.selectionEnd=Math.min(t,n.value.length);else if(t=(e=n.ownerDocument||document)&&e.defaultView||window,t.getSelection){t=t.getSelection();var i=n.textContent.length,s=Math.min(r.start,i);r=r.end===void 0?s:Math.min(r.end,i),!t.extend&&s>r&&(i=r,r=s,s=i),i=Dg(n,s);var o=Dg(n,r);i&&o&&(t.rangeCount!==1||t.anchorNode!==i.node||t.anchorOffset!==i.offset||t.focusNode!==o.node||t.focusOffset!==o.offset)&&(e=e.createRange(),e.setStart(i.node,i.offset),t.removeAllRanges(),s>r?(t.addRange(e),t.extend(o.node,o.offset)):(e.setEnd(o.node,o.offset),t.addRange(e)))}}for(e=[],t=n;t=t.parentNode;)t.nodeType===1&&e.push({element:t,left:t.scrollLeft,top:t.scrollTop});for(typeof n.focus=="function"&&n.focus(),n=0;n<e.length;n++)t=e[n],t.element.scrollLeft=t.left,t.element.scrollTop=t.top}}var fS=zn&&"documentMode"in document&&11>=document.documentMode,$i=null,kh=null,_o=null,Ah=!1;function Mg(t,e,n){var r=n.window===n?n.document:n.nodeType===9?n:n.ownerDocument;Ah||$i==null||$i!==ru(r)||(r=$i,"selectionStart"in r&&up(r)?r={start:r.selectionStart,end:r.selectionEnd}:(r=(r.ownerDocument&&r.ownerDocument.defaultView||window).getSelection(),r={anchorNode:r.anchorNode,anchorOffset:r.anchorOffset,focusNode:r.focusNode,focusOffset:r.focusOffset}),_o&&Uo(_o,r)||(_o=r,r=cu(kh,"onSelect"),0<r.length&&(e=new op("onSelect","select",null,e,n),t.push({event:e,listeners:r}),e.target=$i)))}function ll(t,e){var n={};return n[t.toLowerCase()]=e.toLowerCase(),n["Webkit"+t]="webkit"+e,n["Moz"+t]="moz"+e,n}var zi={animationend:ll("Animation","AnimationEnd"),animationiteration:ll("Animation","AnimationIteration"),animationstart:ll("Animation","AnimationStart"),transitionend:ll("Transition","TransitionEnd")},yf={},w1={};zn&&(w1=document.createElement("div").style,"AnimationEvent"in window||(delete zi.animationend.animation,delete zi.animationiteration.animation,delete zi.animationstart.animation),"TransitionEvent"in window||delete zi.transitionend.transition);function ac(t){if(yf[t])return yf[t];if(!zi[t])return t;var e=zi[t],n;for(n in e)if(e.hasOwnProperty(n)&&n in w1)return yf[t]=e[n];return t}var E1=ac("animationend"),T1=ac("animationiteration"),I1=ac("animationstart"),S1=ac("transitionend"),k1=new Map,Lg="abort auxClick cancel canPlay canPlayThrough click close contextMenu copy cut drag dragEnd dragEnter dragExit dragLeave dragOver dragStart drop durationChange emptied encrypted ended error gotPointerCapture input invalid keyDown keyPress keyUp load loadedData loadedMetadata loadStart lostPointerCapture mouseDown mouseMove mouseOut mouseOver mouseUp paste pause play playing pointerCancel pointerDown pointerMove pointerOut pointerOver pointerUp progress rateChange reset resize seeked seeking stalled submit suspend timeUpdate touchCancel touchEnd touchStart volumeChange scroll toggle touchMove waiting wheel".split(" ");function $r(t,e){k1.set(t,e),xi(e,[t])}for(var _f=0;_f<Lg.length;_f++){var wf=Lg[_f],hS=wf.toLowerCase(),dS=wf[0].toUpperCase()+wf.slice(1);$r(hS,"on"+dS)}$r(E1,"onAnimationEnd");$r(T1,"onAnimationIteration");$r(I1,"onAnimationStart");$r("dblclick","onDoubleClick");$r("focusin","onFocus");$r("focusout","onBlur");$r(S1,"onTransitionEnd");gs("onMouseEnter",["mouseout","mouseover"]);gs("onMouseLeave",["mouseout","mouseover"]);gs("onPointerEnter",["pointerout","pointerover"]);gs("onPointerLeave",["pointerout","pointerover"]);xi("onChange","change click focusin focusout input keydown keyup selectionchange".split(" "));xi("onSelect","focusout contextmenu dragend focusin keydown keyup mousedown mouseup selectionchange".split(" "));xi("onBeforeInput",["compositionend","keypress","textInput","paste"]);xi("onCompositionEnd","compositionend focusout keydown keypress keyup mousedown".split(" "));xi("onCompositionStart","compositionstart focusout keydown keypress keyup mousedown".split(" "));xi("onCompositionUpdate","compositionupdate focusout keydown keypress keyup mousedown".split(" "));var co="abort canplay canplaythrough durationchange emptied encrypted ended error loadeddata loadedmetadata loadstart pause play playing progress ratechange resize seeked seeking stalled suspend timeupdate volumechange waiting".split(" "),pS=new Set("cancel close invalid load scroll toggle".split(" ").concat(co));function jg(t,e,n){var r=t.type||"unknown-event";t.currentTarget=n,h2(r,e,void 0,t),t.currentTarget=null}function A1(t,e){e=(e&4)!==0;for(var n=0;n<t.length;n++){var r=t[n],i=r.event;r=r.listeners;e:{var s=void 0;if(e)for(var o=r.length-1;0<=o;o--){var a=r[o],l=a.instance,u=a.currentTarget;if(a=a.listener,l!==s&&i.isPropagationStopped())break e;jg(i,a,u),s=l}else for(o=0;o<r.length;o++){if(a=r[o],l=a.instance,u=a.currentTarget,a=a.listener,l!==s&&i.isPropagationStopped())break e;jg(i,a,u),s=l}}}if(su)throw t=Eh,su=!1,Eh=null,t}function me(t,e){var n=e[Nh];n===void 0&&(n=e[Nh]=new Set);var r=t+"__bubble";n.has(r)||(x1(e,t,2,!1),n.add(r))}function Ef(t,e,n){var r=0;e&&(r|=4),x1(n,t,r,e)}var ul="_reactListening"+Math.random().toString(36).slice(2);function $o(t){if(!t[ul]){t[ul]=!0,D0.forEach(function(n){n!=="selectionchange"&&(pS.has(n)||Ef(n,!1,t),Ef(n,!0,t))});var e=t.nodeType===9?t:t.ownerDocument;e===null||e[ul]||(e[ul]=!0,Ef("selectionchange",!1,e))}}function x1(t,e,n,r){switch(c1(e)){case 1:var i=P2;break;case 4:i=R2;break;default:i=ip}n=i.bind(null,e,n,t),i=void 0,!wh||e!=="touchstart"&&e!=="touchmove"&&e!=="wheel"||(i=!0),r?i!==void 0?t.addEventListener(e,n,{capture:!0,passive:i}):t.addEventListener(e,n,!0):i!==void 0?t.addEventListener(e,n,{passive:i}):t.addEventListener(e,n,!1)}function Tf(t,e,n,r,i){var s=r;if(!(e&1)&&!(e&2)&&r!==null)e:for(;;){if(r===null)return;var o=r.tag;if(o===3||o===4){var a=r.stateNode.containerInfo;if(a===i||a.nodeType===8&&a.parentNode===i)break;if(o===4)for(o=r.return;o!==null;){var l=o.tag;if((l===3||l===4)&&(l=o.stateNode.containerInfo,l===i||l.nodeType===8&&l.parentNode===i))return;o=o.return}for(;a!==null;){if(o=Jr(a),o===null)return;if(l=o.tag,l===5||l===6){r=s=o;continue e}a=a.parentNode}}r=r.return}Y0(function(){var u=s,c=ep(n),h=[];e:{var d=k1.get(t);if(d!==void 0){var y=op,_=t;switch(t){case"keypress":if($l(n)===0)break e;case"keydown":case"keyup":y=H2;break;case"focusin":_="focus",y=mf;break;case"focusout":_="blur",y=mf;break;case"beforeblur":case"afterblur":y=mf;break;case"click":if(n.button===2)break e;case"auxclick":case"dblclick":case"mousedown":case"mousemove":case"mouseup":case"mouseout":case"mouseover":case"contextmenu":y=kg;break;case"drag":case"dragend":case"dragenter":case"dragexit":case"dragleave":case"dragover":case"dragstart":case"drop":y=b2;break;case"touchcancel":case"touchend":case"touchmove":case"touchstart":y=G2;break;case E1:case T1:case I1:y=M2;break;case S1:y=Q2;break;case"scroll":y=C2;break;case"wheel":y=X2;break;case"copy":case"cut":case"paste":y=j2;break;case"gotpointercapture":case"lostpointercapture":case"pointercancel":case"pointerdown":case"pointermove":case"pointerout":case"pointerover":case"pointerup":y=xg}var w=(e&4)!==0,T=!w&&t==="scroll",g=w?d!==null?d+"Capture":null:d;w=[];for(var p=u,v;p!==null;){v=p;var m=v.stateNode;if(v.tag===5&&m!==null&&(v=m,g!==null&&(m=Mo(p,g),m!=null&&w.push(zo(p,m,v)))),T)break;p=p.return}0<w.length&&(d=new y(d,_,null,n,c),h.push({event:d,listeners:w}))}}if(!(e&7)){e:{if(d=t==="mouseover"||t==="pointerover",y=t==="mouseout"||t==="pointerout",d&&n!==yh&&(_=n.relatedTarget||n.fromElement)&&(Jr(_)||_[Bn]))break e;if((y||d)&&(d=c.window===c?c:(d=c.ownerDocument)?d.defaultView||d.parentWindow:window,y?(_=n.relatedTarget||n.toElement,y=u,_=_?Jr(_):null,_!==null&&(T=Pi(_),_!==T||_.tag!==5&&_.tag!==6)&&(_=null)):(y=null,_=u),y!==_)){if(w=kg,m="onMouseLeave",g="onMouseEnter",p="mouse",(t==="pointerout"||t==="pointerover")&&(w=xg,m="onPointerLeave",g="onPointerEnter",p="pointer"),T=y==null?d:Bi(y),v=_==null?d:Bi(_),d=new w(m,p+"leave",y,n,c),d.target=T,d.relatedTarget=v,m=null,Jr(c)===u&&(w=new w(g,p+"enter",_,n,c),w.target=v,w.relatedTarget=T,m=w),T=m,y&&_)t:{for(w=y,g=_,p=0,v=w;v;v=Di(v))p++;for(v=0,m=g;m;m=Di(m))v++;for(;0<p-v;)w=Di(w),p--;for(;0<v-p;)g=Di(g),v--;for(;p--;){if(w===g||g!==null&&w===g.alternate)break t;w=Di(w),g=Di(g)}w=null}else w=null;y!==null&&Vg(h,d,y,w,!1),_!==null&&T!==null&&Vg(h,T,_,w,!0)}}e:{if(d=u?Bi(u):window,y=d.nodeName&&d.nodeName.toLowerCase(),y==="select"||y==="input"&&d.type==="file")var S=iS;else if(Cg(d))if(g1)S=lS;else{S=oS;var P=sS}else(y=d.nodeName)&&y.toLowerCase()==="input"&&(d.type==="checkbox"||d.type==="radio")&&(S=aS);if(S&&(S=S(t,u))){m1(h,S,n,c);break e}P&&P(t,d,u),t==="focusout"&&(P=d._wrapperState)&&P.controlled&&d.type==="number"&&dh(d,"number",d.value)}switch(P=u?Bi(u):window,t){case"focusin":(Cg(P)||P.contentEditable==="true")&&($i=P,kh=u,_o=null);break;case"focusout":_o=kh=$i=null;break;case"mousedown":Ah=!0;break;case"contextmenu":case"mouseup":case"dragend":Ah=!1,Mg(h,n,c);break;case"selectionchange":if(fS)break;case"keydown":case"keyup":Mg(h,n,c)}var A;if(lp)e:{switch(t){case"compositionstart":var O="onCompositionStart";break e;case"compositionend":O="onCompositionEnd";break e;case"compositionupdate":O="onCompositionUpdate";break e}O=void 0}else Ui?d1(t,n)&&(O="onCompositionEnd"):t==="keydown"&&n.keyCode===229&&(O="onCompositionStart");O&&(h1&&n.locale!=="ko"&&(Ui||O!=="onCompositionStart"?O==="onCompositionEnd"&&Ui&&(A=f1()):(_r=c,sp="value"in _r?_r.value:_r.textContent,Ui=!0)),P=cu(u,O),0<P.length&&(O=new Ag(O,t,null,n,c),h.push({event:O,listeners:P}),A?O.data=A:(A=p1(n),A!==null&&(O.data=A)))),(A=Z2?eS(t,n):tS(t,n))&&(u=cu(u,"onBeforeInput"),0<u.length&&(c=new Ag("onBeforeInput","beforeinput",null,n,c),h.push({event:c,listeners:u}),c.data=A))}A1(h,e)})}function zo(t,e,n){return{instance:t,listener:e,currentTarget:n}}function cu(t,e){for(var n=e+"Capture",r=[];t!==null;){var i=t,s=i.stateNode;i.tag===5&&s!==null&&(i=s,s=Mo(t,n),s!=null&&r.unshift(zo(t,s,i)),s=Mo(t,e),s!=null&&r.push(zo(t,s,i))),t=t.return}return r}function Di(t){if(t===null)return null;do t=t.return;while(t&&t.tag!==5);return t||null}function Vg(t,e,n,r,i){for(var s=e._reactName,o=[];n!==null&&n!==r;){var a=n,l=a.alternate,u=a.stateNode;if(l!==null&&l===r)break;a.tag===5&&u!==null&&(a=u,i?(l=Mo(n,s),l!=null&&o.unshift(zo(n,l,a))):i||(l=Mo(n,s),l!=null&&o.push(zo(n,l,a)))),n=n.return}o.length!==0&&t.push({event:e,listeners:o})}var mS=/\r\n?/g,gS=/\u0000|\uFFFD/g;function Fg(t){return(typeof t=="string"?t:""+t).replace(mS,`
`).replace(gS,"")}function cl(t,e,n){if(e=Fg(e),Fg(t)!==e&&n)throw Error(x(425))}function fu(){}var xh=null,Ph=null;function Rh(t,e){return t==="textarea"||t==="noscript"||typeof e.children=="string"||typeof e.children=="number"||typeof e.dangerouslySetInnerHTML=="object"&&e.dangerouslySetInnerHTML!==null&&e.dangerouslySetInnerHTML.__html!=null}var Ch=typeof setTimeout=="function"?setTimeout:void 0,vS=typeof clearTimeout=="function"?clearTimeout:void 0,Ug=typeof Promise=="function"?Promise:void 0,yS=typeof queueMicrotask=="function"?queueMicrotask:typeof Ug<"u"?function(t){return Ug.resolve(null).then(t).catch(_S)}:Ch;function _S(t){setTimeout(function(){throw t})}function If(t,e){var n=e,r=0;do{var i=n.nextSibling;if(t.removeChild(n),i&&i.nodeType===8)if(n=i.data,n==="/$"){if(r===0){t.removeChild(i),Vo(e);return}r--}else n!=="$"&&n!=="$?"&&n!=="$!"||r++;n=i}while(n);Vo(e)}function kr(t){for(;t!=null;t=t.nextSibling){var e=t.nodeType;if(e===1||e===3)break;if(e===8){if(e=t.data,e==="$"||e==="$!"||e==="$?")break;if(e==="/$")return null}}return t}function $g(t){t=t.previousSibling;for(var e=0;t;){if(t.nodeType===8){var n=t.data;if(n==="$"||n==="$!"||n==="$?"){if(e===0)return t;e--}else n==="/$"&&e++}t=t.previousSibling}return null}var Ls=Math.random().toString(36).slice(2),gn="__reactFiber$"+Ls,Bo="__reactProps$"+Ls,Bn="__reactContainer$"+Ls,Nh="__reactEvents$"+Ls,wS="__reactListeners$"+Ls,ES="__reactHandles$"+Ls;function Jr(t){var e=t[gn];if(e)return e;for(var n=t.parentNode;n;){if(e=n[Bn]||n[gn]){if(n=e.alternate,e.child!==null||n!==null&&n.child!==null)for(t=$g(t);t!==null;){if(n=t[gn])return n;t=$g(t)}return e}t=n,n=t.parentNode}return null}function Sa(t){return t=t[gn]||t[Bn],!t||t.tag!==5&&t.tag!==6&&t.tag!==13&&t.tag!==3?null:t}function Bi(t){if(t.tag===5||t.tag===6)return t.stateNode;throw Error(x(33))}function lc(t){return t[Bo]||null}var bh=[],Hi=-1;function zr(t){return{current:t}}function ye(t){0>Hi||(t.current=bh[Hi],bh[Hi]=null,Hi--)}function pe(t,e){Hi++,bh[Hi]=t.current,t.current=e}var Dr={},yt=zr(Dr),Rt=zr(!1),hi=Dr;function vs(t,e){var n=t.type.contextTypes;if(!n)return Dr;var r=t.stateNode;if(r&&r.__reactInternalMemoizedUnmaskedChildContext===e)return r.__reactInternalMemoizedMaskedChildContext;var i={},s;for(s in n)i[s]=e[s];return r&&(t=t.stateNode,t.__reactInternalMemoizedUnmaskedChildContext=e,t.__reactInternalMemoizedMaskedChildContext=i),i}function Ct(t){return t=t.childContextTypes,t!=null}function hu(){ye(Rt),ye(yt)}function zg(t,e,n){if(yt.current!==Dr)throw Error(x(168));pe(yt,e),pe(Rt,n)}function P1(t,e,n){var r=t.stateNode;if(e=e.childContextTypes,typeof r.getChildContext!="function")return n;r=r.getChildContext();for(var i in r)if(!(i in e))throw Error(x(108,s2(t)||"Unknown",i));return xe({},n,r)}function du(t){return t=(t=t.stateNode)&&t.__reactInternalMemoizedMergedChildContext||Dr,hi=yt.current,pe(yt,t),pe(Rt,Rt.current),!0}function Bg(t,e,n){var r=t.stateNode;if(!r)throw Error(x(169));n?(t=P1(t,e,hi),r.__reactInternalMemoizedMergedChildContext=t,ye(Rt),ye(yt),pe(yt,t)):ye(Rt),pe(Rt,n)}var bn=null,uc=!1,Sf=!1;function R1(t){bn===null?bn=[t]:bn.push(t)}function TS(t){uc=!0,R1(t)}function Br(){if(!Sf&&bn!==null){Sf=!0;var t=0,e=oe;try{var n=bn;for(oe=1;t<n.length;t++){var r=n[t];do r=r(!0);while(r!==null)}bn=null,uc=!1}catch(i){throw bn!==null&&(bn=bn.slice(t+1)),e1(tp,Br),i}finally{oe=e,Sf=!1}}return null}var Wi=[],Ki=0,pu=null,mu=0,Bt=[],Ht=0,di=null,On=1,Dn="";function Qr(t,e){Wi[Ki++]=mu,Wi[Ki++]=pu,pu=t,mu=e}function C1(t,e,n){Bt[Ht++]=On,Bt[Ht++]=Dn,Bt[Ht++]=di,di=t;var r=On;t=Dn;var i=32-ln(r)-1;r&=~(1<<i),n+=1;var s=32-ln(e)+i;if(30<s){var o=i-i%5;s=(r&(1<<o)-1).toString(32),r>>=o,i-=o,On=1<<32-ln(e)+i|n<<i|r,Dn=s+t}else On=1<<s|n<<i|r,Dn=t}function cp(t){t.return!==null&&(Qr(t,1),C1(t,1,0))}function fp(t){for(;t===pu;)pu=Wi[--Ki],Wi[Ki]=null,mu=Wi[--Ki],Wi[Ki]=null;for(;t===di;)di=Bt[--Ht],Bt[Ht]=null,Dn=Bt[--Ht],Bt[Ht]=null,On=Bt[--Ht],Bt[Ht]=null}var Lt=null,Mt=null,Te=!1,nn=null;function N1(t,e){var n=Kt(5,null,null,0);n.elementType="DELETED",n.stateNode=e,n.return=t,e=t.deletions,e===null?(t.deletions=[n],t.flags|=16):e.push(n)}function Hg(t,e){switch(t.tag){case 5:var n=t.type;return e=e.nodeType!==1||n.toLowerCase()!==e.nodeName.toLowerCase()?null:e,e!==null?(t.stateNode=e,Lt=t,Mt=kr(e.firstChild),!0):!1;case 6:return e=t.pendingProps===""||e.nodeType!==3?null:e,e!==null?(t.stateNode=e,Lt=t,Mt=null,!0):!1;case 13:return e=e.nodeType!==8?null:e,e!==null?(n=di!==null?{id:On,overflow:Dn}:null,t.memoizedState={dehydrated:e,treeContext:n,retryLane:1073741824},n=Kt(18,null,null,0),n.stateNode=e,n.return=t,t.child=n,Lt=t,Mt=null,!0):!1;default:return!1}}function Oh(t){return(t.mode&1)!==0&&(t.flags&128)===0}function Dh(t){if(Te){var e=Mt;if(e){var n=e;if(!Hg(t,e)){if(Oh(t))throw Error(x(418));e=kr(n.nextSibling);var r=Lt;e&&Hg(t,e)?N1(r,n):(t.flags=t.flags&-4097|2,Te=!1,Lt=t)}}else{if(Oh(t))throw Error(x(418));t.flags=t.flags&-4097|2,Te=!1,Lt=t}}}function Wg(t){for(t=t.return;t!==null&&t.tag!==5&&t.tag!==3&&t.tag!==13;)t=t.return;Lt=t}function fl(t){if(t!==Lt)return!1;if(!Te)return Wg(t),Te=!0,!1;var e;if((e=t.tag!==3)&&!(e=t.tag!==5)&&(e=t.type,e=e!=="head"&&e!=="body"&&!Rh(t.type,t.memoizedProps)),e&&(e=Mt)){if(Oh(t))throw b1(),Error(x(418));for(;e;)N1(t,e),e=kr(e.nextSibling)}if(Wg(t),t.tag===13){if(t=t.memoizedState,t=t!==null?t.dehydrated:null,!t)throw Error(x(317));e:{for(t=t.nextSibling,e=0;t;){if(t.nodeType===8){var n=t.data;if(n==="/$"){if(e===0){Mt=kr(t.nextSibling);break e}e--}else n!=="$"&&n!=="$!"&&n!=="$?"||e++}t=t.nextSibling}Mt=null}}else Mt=Lt?kr(t.stateNode.nextSibling):null;return!0}function b1(){for(var t=Mt;t;)t=kr(t.nextSibling)}function ys(){Mt=Lt=null,Te=!1}function hp(t){nn===null?nn=[t]:nn.push(t)}var IS=Zn.ReactCurrentBatchConfig;function Jt(t,e){if(t&&t.defaultProps){e=xe({},e),t=t.defaultProps;for(var n in t)e[n]===void 0&&(e[n]=t[n]);return e}return e}var gu=zr(null),vu=null,Gi=null,dp=null;function pp(){dp=Gi=vu=null}function mp(t){var e=gu.current;ye(gu),t._currentValue=e}function Mh(t,e,n){for(;t!==null;){var r=t.alternate;if((t.childLanes&e)!==e?(t.childLanes|=e,r!==null&&(r.childLanes|=e)):r!==null&&(r.childLanes&e)!==e&&(r.childLanes|=e),t===n)break;t=t.return}}function as(t,e){vu=t,dp=Gi=null,t=t.dependencies,t!==null&&t.firstContext!==null&&(t.lanes&e&&(Pt=!0),t.firstContext=null)}function qt(t){var e=t._currentValue;if(dp!==t)if(t={context:t,memoizedValue:e,next:null},Gi===null){if(vu===null)throw Error(x(308));Gi=t,vu.dependencies={lanes:0,firstContext:t}}else Gi=Gi.next=t;return e}var Zr=null;function gp(t){Zr===null?Zr=[t]:Zr.push(t)}function O1(t,e,n,r){var i=e.interleaved;return i===null?(n.next=n,gp(e)):(n.next=i.next,i.next=n),e.interleaved=n,Hn(t,r)}function Hn(t,e){t.lanes|=e;var n=t.alternate;for(n!==null&&(n.lanes|=e),n=t,t=t.return;t!==null;)t.childLanes|=e,n=t.alternate,n!==null&&(n.childLanes|=e),n=t,t=t.return;return n.tag===3?n.stateNode:null}var hr=!1;function vp(t){t.updateQueue={baseState:t.memoizedState,firstBaseUpdate:null,lastBaseUpdate:null,shared:{pending:null,interleaved:null,lanes:0},effects:null}}function D1(t,e){t=t.updateQueue,e.updateQueue===t&&(e.updateQueue={baseState:t.baseState,firstBaseUpdate:t.firstBaseUpdate,lastBaseUpdate:t.lastBaseUpdate,shared:t.shared,effects:t.effects})}function Vn(t,e){return{eventTime:t,lane:e,tag:0,payload:null,callback:null,next:null}}function Ar(t,e,n){var r=t.updateQueue;if(r===null)return null;if(r=r.shared,ie&2){var i=r.pending;return i===null?e.next=e:(e.next=i.next,i.next=e),r.pending=e,Hn(t,n)}return i=r.interleaved,i===null?(e.next=e,gp(r)):(e.next=i.next,i.next=e),r.interleaved=e,Hn(t,n)}function zl(t,e,n){if(e=e.updateQueue,e!==null&&(e=e.shared,(n&4194240)!==0)){var r=e.lanes;r&=t.pendingLanes,n|=r,e.lanes=n,np(t,n)}}function Kg(t,e){var n=t.updateQueue,r=t.alternate;if(r!==null&&(r=r.updateQueue,n===r)){var i=null,s=null;if(n=n.firstBaseUpdate,n!==null){do{var o={eventTime:n.eventTime,lane:n.lane,tag:n.tag,payload:n.payload,callback:n.callback,next:null};s===null?i=s=o:s=s.next=o,n=n.next}while(n!==null);s===null?i=s=e:s=s.next=e}else i=s=e;n={baseState:r.baseState,firstBaseUpdate:i,lastBaseUpdate:s,shared:r.shared,effects:r.effects},t.updateQueue=n;return}t=n.lastBaseUpdate,t===null?n.firstBaseUpdate=e:t.next=e,n.lastBaseUpdate=e}function yu(t,e,n,r){var i=t.updateQueue;hr=!1;var s=i.firstBaseUpdate,o=i.lastBaseUpdate,a=i.shared.pending;if(a!==null){i.shared.pending=null;var l=a,u=l.next;l.next=null,o===null?s=u:o.next=u,o=l;var c=t.alternate;c!==null&&(c=c.updateQueue,a=c.lastBaseUpdate,a!==o&&(a===null?c.firstBaseUpdate=u:a.next=u,c.lastBaseUpdate=l))}if(s!==null){var h=i.baseState;o=0,c=u=l=null,a=s;do{var d=a.lane,y=a.eventTime;if((r&d)===d){c!==null&&(c=c.next={eventTime:y,lane:0,tag:a.tag,payload:a.payload,callback:a.callback,next:null});e:{var _=t,w=a;switch(d=e,y=n,w.tag){case 1:if(_=w.payload,typeof _=="function"){h=_.call(y,h,d);break e}h=_;break e;case 3:_.flags=_.flags&-65537|128;case 0:if(_=w.payload,d=typeof _=="function"?_.call(y,h,d):_,d==null)break e;h=xe({},h,d);break e;case 2:hr=!0}}a.callback!==null&&a.lane!==0&&(t.flags|=64,d=i.effects,d===null?i.effects=[a]:d.push(a))}else y={eventTime:y,lane:d,tag:a.tag,payload:a.payload,callback:a.callback,next:null},c===null?(u=c=y,l=h):c=c.next=y,o|=d;if(a=a.next,a===null){if(a=i.shared.pending,a===null)break;d=a,a=d.next,d.next=null,i.lastBaseUpdate=d,i.shared.pending=null}}while(1);if(c===null&&(l=h),i.baseState=l,i.firstBaseUpdate=u,i.lastBaseUpdate=c,e=i.shared.interleaved,e!==null){i=e;do o|=i.lane,i=i.next;while(i!==e)}else s===null&&(i.shared.lanes=0);mi|=o,t.lanes=o,t.memoizedState=h}}function Gg(t,e,n){if(t=e.effects,e.effects=null,t!==null)for(e=0;e<t.length;e++){var r=t[e],i=r.callback;if(i!==null){if(r.callback=null,r=n,typeof i!="function")throw Error(x(191,i));i.call(r)}}}var M1=new O0.Component().refs;function Lh(t,e,n,r){e=t.memoizedState,n=n(r,e),n=n==null?e:xe({},e,n),t.memoizedState=n,t.lanes===0&&(t.updateQueue.baseState=n)}var cc={isMounted:function(t){return(t=t._reactInternals)?Pi(t)===t:!1},enqueueSetState:function(t,e,n){t=t._reactInternals;var r=Tt(),i=Pr(t),s=Vn(r,i);s.payload=e,n!=null&&(s.callback=n),e=Ar(t,s,i),e!==null&&(un(e,t,i,r),zl(e,t,i))},enqueueReplaceState:function(t,e,n){t=t._reactInternals;var r=Tt(),i=Pr(t),s=Vn(r,i);s.tag=1,s.payload=e,n!=null&&(s.callback=n),e=Ar(t,s,i),e!==null&&(un(e,t,i,r),zl(e,t,i))},enqueueForceUpdate:function(t,e){t=t._reactInternals;var n=Tt(),r=Pr(t),i=Vn(n,r);i.tag=2,e!=null&&(i.callback=e),e=Ar(t,i,r),e!==null&&(un(e,t,r,n),zl(e,t,r))}};function qg(t,e,n,r,i,s,o){return t=t.stateNode,typeof t.shouldComponentUpdate=="function"?t.shouldComponentUpdate(r,s,o):e.prototype&&e.prototype.isPureReactComponent?!Uo(n,r)||!Uo(i,s):!0}function L1(t,e,n){var r=!1,i=Dr,s=e.contextType;return typeof s=="object"&&s!==null?s=qt(s):(i=Ct(e)?hi:yt.current,r=e.contextTypes,s=(r=r!=null)?vs(t,i):Dr),e=new e(n,s),t.memoizedState=e.state!==null&&e.state!==void 0?e.state:null,e.updater=cc,t.stateNode=e,e._reactInternals=t,r&&(t=t.stateNode,t.__reactInternalMemoizedUnmaskedChildContext=i,t.__reactInternalMemoizedMaskedChildContext=s),e}function Qg(t,e,n,r){t=e.state,typeof e.componentWillReceiveProps=="function"&&e.componentWillReceiveProps(n,r),typeof e.UNSAFE_componentWillReceiveProps=="function"&&e.UNSAFE_componentWillReceiveProps(n,r),e.state!==t&&cc.enqueueReplaceState(e,e.state,null)}function jh(t,e,n,r){var i=t.stateNode;i.props=n,i.state=t.memoizedState,i.refs=M1,vp(t);var s=e.contextType;typeof s=="object"&&s!==null?i.context=qt(s):(s=Ct(e)?hi:yt.current,i.context=vs(t,s)),i.state=t.memoizedState,s=e.getDerivedStateFromProps,typeof s=="function"&&(Lh(t,e,s,n),i.state=t.memoizedState),typeof e.getDerivedStateFromProps=="function"||typeof i.getSnapshotBeforeUpdate=="function"||typeof i.UNSAFE_componentWillMount!="function"&&typeof i.componentWillMount!="function"||(e=i.state,typeof i.componentWillMount=="function"&&i.componentWillMount(),typeof i.UNSAFE_componentWillMount=="function"&&i.UNSAFE_componentWillMount(),e!==i.state&&cc.enqueueReplaceState(i,i.state,null),yu(t,n,i,r),i.state=t.memoizedState),typeof i.componentDidMount=="function"&&(t.flags|=4194308)}function Js(t,e,n){if(t=n.ref,t!==null&&typeof t!="function"&&typeof t!="object"){if(n._owner){if(n=n._owner,n){if(n.tag!==1)throw Error(x(309));var r=n.stateNode}if(!r)throw Error(x(147,t));var i=r,s=""+t;return e!==null&&e.ref!==null&&typeof e.ref=="function"&&e.ref._stringRef===s?e.ref:(e=function(o){var a=i.refs;a===M1&&(a=i.refs={}),o===null?delete a[s]:a[s]=o},e._stringRef=s,e)}if(typeof t!="string")throw Error(x(284));if(!n._owner)throw Error(x(290,t))}return t}function hl(t,e){throw t=Object.prototype.toString.call(e),Error(x(31,t==="[object Object]"?"object with keys {"+Object.keys(e).join(", ")+"}":t))}function Yg(t){var e=t._init;return e(t._payload)}function j1(t){function e(g,p){if(t){var v=g.deletions;v===null?(g.deletions=[p],g.flags|=16):v.push(p)}}function n(g,p){if(!t)return null;for(;p!==null;)e(g,p),p=p.sibling;return null}function r(g,p){for(g=new Map;p!==null;)p.key!==null?g.set(p.key,p):g.set(p.index,p),p=p.sibling;return g}function i(g,p){return g=Rr(g,p),g.index=0,g.sibling=null,g}function s(g,p,v){return g.index=v,t?(v=g.alternate,v!==null?(v=v.index,v<p?(g.flags|=2,p):v):(g.flags|=2,p)):(g.flags|=1048576,p)}function o(g){return t&&g.alternate===null&&(g.flags|=2),g}function a(g,p,v,m){return p===null||p.tag!==6?(p=Nf(v,g.mode,m),p.return=g,p):(p=i(p,v),p.return=g,p)}function l(g,p,v,m){var S=v.type;return S===Fi?c(g,p,v.props.children,m,v.key):p!==null&&(p.elementType===S||typeof S=="object"&&S!==null&&S.$$typeof===fr&&Yg(S)===p.type)?(m=i(p,v.props),m.ref=Js(g,p,v),m.return=g,m):(m=ql(v.type,v.key,v.props,null,g.mode,m),m.ref=Js(g,p,v),m.return=g,m)}function u(g,p,v,m){return p===null||p.tag!==4||p.stateNode.containerInfo!==v.containerInfo||p.stateNode.implementation!==v.implementation?(p=bf(v,g.mode,m),p.return=g,p):(p=i(p,v.children||[]),p.return=g,p)}function c(g,p,v,m,S){return p===null||p.tag!==7?(p=li(v,g.mode,m,S),p.return=g,p):(p=i(p,v),p.return=g,p)}function h(g,p,v){if(typeof p=="string"&&p!==""||typeof p=="number")return p=Nf(""+p,g.mode,v),p.return=g,p;if(typeof p=="object"&&p!==null){switch(p.$$typeof){case tl:return v=ql(p.type,p.key,p.props,null,g.mode,v),v.ref=Js(g,null,p),v.return=g,v;case Vi:return p=bf(p,g.mode,v),p.return=g,p;case fr:var m=p._init;return h(g,m(p._payload),v)}if(lo(p)||Gs(p))return p=li(p,g.mode,v,null),p.return=g,p;hl(g,p)}return null}function d(g,p,v,m){var S=p!==null?p.key:null;if(typeof v=="string"&&v!==""||typeof v=="number")return S!==null?null:a(g,p,""+v,m);if(typeof v=="object"&&v!==null){switch(v.$$typeof){case tl:return v.key===S?l(g,p,v,m):null;case Vi:return v.key===S?u(g,p,v,m):null;case fr:return S=v._init,d(g,p,S(v._payload),m)}if(lo(v)||Gs(v))return S!==null?null:c(g,p,v,m,null);hl(g,v)}return null}function y(g,p,v,m,S){if(typeof m=="string"&&m!==""||typeof m=="number")return g=g.get(v)||null,a(p,g,""+m,S);if(typeof m=="object"&&m!==null){switch(m.$$typeof){case tl:return g=g.get(m.key===null?v:m.key)||null,l(p,g,m,S);case Vi:return g=g.get(m.key===null?v:m.key)||null,u(p,g,m,S);case fr:var P=m._init;return y(g,p,v,P(m._payload),S)}if(lo(m)||Gs(m))return g=g.get(v)||null,c(p,g,m,S,null);hl(p,m)}return null}function _(g,p,v,m){for(var S=null,P=null,A=p,O=p=0,B=null;A!==null&&O<v.length;O++){A.index>O?(B=A,A=null):B=A.sibling;var U=d(g,A,v[O],m);if(U===null){A===null&&(A=B);break}t&&A&&U.alternate===null&&e(g,A),p=s(U,p,O),P===null?S=U:P.sibling=U,P=U,A=B}if(O===v.length)return n(g,A),Te&&Qr(g,O),S;if(A===null){for(;O<v.length;O++)A=h(g,v[O],m),A!==null&&(p=s(A,p,O),P===null?S=A:P.sibling=A,P=A);return Te&&Qr(g,O),S}for(A=r(g,A);O<v.length;O++)B=y(A,g,O,v[O],m),B!==null&&(t&&B.alternate!==null&&A.delete(B.key===null?O:B.key),p=s(B,p,O),P===null?S=B:P.sibling=B,P=B);return t&&A.forEach(function(He){return e(g,He)}),Te&&Qr(g,O),S}function w(g,p,v,m){var S=Gs(v);if(typeof S!="function")throw Error(x(150));if(v=S.call(v),v==null)throw Error(x(151));for(var P=S=null,A=p,O=p=0,B=null,U=v.next();A!==null&&!U.done;O++,U=v.next()){A.index>O?(B=A,A=null):B=A.sibling;var He=d(g,A,U.value,m);if(He===null){A===null&&(A=B);break}t&&A&&He.alternate===null&&e(g,A),p=s(He,p,O),P===null?S=He:P.sibling=He,P=He,A=B}if(U.done)return n(g,A),Te&&Qr(g,O),S;if(A===null){for(;!U.done;O++,U=v.next())U=h(g,U.value,m),U!==null&&(p=s(U,p,O),P===null?S=U:P.sibling=U,P=U);return Te&&Qr(g,O),S}for(A=r(g,A);!U.done;O++,U=v.next())U=y(A,g,O,U.value,m),U!==null&&(t&&U.alternate!==null&&A.delete(U.key===null?O:U.key),p=s(U,p,O),P===null?S=U:P.sibling=U,P=U);return t&&A.forEach(function(ce){return e(g,ce)}),Te&&Qr(g,O),S}function T(g,p,v,m){if(typeof v=="object"&&v!==null&&v.type===Fi&&v.key===null&&(v=v.props.children),typeof v=="object"&&v!==null){switch(v.$$typeof){case tl:e:{for(var S=v.key,P=p;P!==null;){if(P.key===S){if(S=v.type,S===Fi){if(P.tag===7){n(g,P.sibling),p=i(P,v.props.children),p.return=g,g=p;break e}}else if(P.elementType===S||typeof S=="object"&&S!==null&&S.$$typeof===fr&&Yg(S)===P.type){n(g,P.sibling),p=i(P,v.props),p.ref=Js(g,P,v),p.return=g,g=p;break e}n(g,P);break}else e(g,P);P=P.sibling}v.type===Fi?(p=li(v.props.children,g.mode,m,v.key),p.return=g,g=p):(m=ql(v.type,v.key,v.props,null,g.mode,m),m.ref=Js(g,p,v),m.return=g,g=m)}return o(g);case Vi:e:{for(P=v.key;p!==null;){if(p.key===P)if(p.tag===4&&p.stateNode.containerInfo===v.containerInfo&&p.stateNode.implementation===v.implementation){n(g,p.sibling),p=i(p,v.children||[]),p.return=g,g=p;break e}else{n(g,p);break}else e(g,p);p=p.sibling}p=bf(v,g.mode,m),p.return=g,g=p}return o(g);case fr:return P=v._init,T(g,p,P(v._payload),m)}if(lo(v))return _(g,p,v,m);if(Gs(v))return w(g,p,v,m);hl(g,v)}return typeof v=="string"&&v!==""||typeof v=="number"?(v=""+v,p!==null&&p.tag===6?(n(g,p.sibling),p=i(p,v),p.return=g,g=p):(n(g,p),p=Nf(v,g.mode,m),p.return=g,g=p),o(g)):n(g,p)}return T}var _s=j1(!0),V1=j1(!1),ka={},Tn=zr(ka),Ho=zr(ka),Wo=zr(ka);function ei(t){if(t===ka)throw Error(x(174));return t}function yp(t,e){switch(pe(Wo,e),pe(Ho,t),pe(Tn,ka),t=e.nodeType,t){case 9:case 11:e=(e=e.documentElement)?e.namespaceURI:mh(null,"");break;default:t=t===8?e.parentNode:e,e=t.namespaceURI||null,t=t.tagName,e=mh(e,t)}ye(Tn),pe(Tn,e)}function ws(){ye(Tn),ye(Ho),ye(Wo)}function F1(t){ei(Wo.current);var e=ei(Tn.current),n=mh(e,t.type);e!==n&&(pe(Ho,t),pe(Tn,n))}function _p(t){Ho.current===t&&(ye(Tn),ye(Ho))}var ke=zr(0);function _u(t){for(var e=t;e!==null;){if(e.tag===13){var n=e.memoizedState;if(n!==null&&(n=n.dehydrated,n===null||n.data==="$?"||n.data==="$!"))return e}else if(e.tag===19&&e.memoizedProps.revealOrder!==void 0){if(e.flags&128)return e}else if(e.child!==null){e.child.return=e,e=e.child;continue}if(e===t)break;for(;e.sibling===null;){if(e.return===null||e.return===t)return null;e=e.return}e.sibling.return=e.return,e=e.sibling}return null}var kf=[];function wp(){for(var t=0;t<kf.length;t++)kf[t]._workInProgressVersionPrimary=null;kf.length=0}var Bl=Zn.ReactCurrentDispatcher,Af=Zn.ReactCurrentBatchConfig,pi=0,Ae=null,Ue=null,qe=null,wu=!1,wo=!1,Ko=0,SS=0;function ut(){throw Error(x(321))}function Ep(t,e){if(e===null)return!1;for(var n=0;n<e.length&&n<t.length;n++)if(!cn(t[n],e[n]))return!1;return!0}function Tp(t,e,n,r,i,s){if(pi=s,Ae=e,e.memoizedState=null,e.updateQueue=null,e.lanes=0,Bl.current=t===null||t.memoizedState===null?PS:RS,t=n(r,i),wo){s=0;do{if(wo=!1,Ko=0,25<=s)throw Error(x(301));s+=1,qe=Ue=null,e.updateQueue=null,Bl.current=CS,t=n(r,i)}while(wo)}if(Bl.current=Eu,e=Ue!==null&&Ue.next!==null,pi=0,qe=Ue=Ae=null,wu=!1,e)throw Error(x(300));return t}function Ip(){var t=Ko!==0;return Ko=0,t}function mn(){var t={memoizedState:null,baseState:null,baseQueue:null,queue:null,next:null};return qe===null?Ae.memoizedState=qe=t:qe=qe.next=t,qe}function Qt(){if(Ue===null){var t=Ae.alternate;t=t!==null?t.memoizedState:null}else t=Ue.next;var e=qe===null?Ae.memoizedState:qe.next;if(e!==null)qe=e,Ue=t;else{if(t===null)throw Error(x(310));Ue=t,t={memoizedState:Ue.memoizedState,baseState:Ue.baseState,baseQueue:Ue.baseQueue,queue:Ue.queue,next:null},qe===null?Ae.memoizedState=qe=t:qe=qe.next=t}return qe}function Go(t,e){return typeof e=="function"?e(t):e}function xf(t){var e=Qt(),n=e.queue;if(n===null)throw Error(x(311));n.lastRenderedReducer=t;var r=Ue,i=r.baseQueue,s=n.pending;if(s!==null){if(i!==null){var o=i.next;i.next=s.next,s.next=o}r.baseQueue=i=s,n.pending=null}if(i!==null){s=i.next,r=r.baseState;var a=o=null,l=null,u=s;do{var c=u.lane;if((pi&c)===c)l!==null&&(l=l.next={lane:0,action:u.action,hasEagerState:u.hasEagerState,eagerState:u.eagerState,next:null}),r=u.hasEagerState?u.eagerState:t(r,u.action);else{var h={lane:c,action:u.action,hasEagerState:u.hasEagerState,eagerState:u.eagerState,next:null};l===null?(a=l=h,o=r):l=l.next=h,Ae.lanes|=c,mi|=c}u=u.next}while(u!==null&&u!==s);l===null?o=r:l.next=a,cn(r,e.memoizedState)||(Pt=!0),e.memoizedState=r,e.baseState=o,e.baseQueue=l,n.lastRenderedState=r}if(t=n.interleaved,t!==null){i=t;do s=i.lane,Ae.lanes|=s,mi|=s,i=i.next;while(i!==t)}else i===null&&(n.lanes=0);return[e.memoizedState,n.dispatch]}function Pf(t){var e=Qt(),n=e.queue;if(n===null)throw Error(x(311));n.lastRenderedReducer=t;var r=n.dispatch,i=n.pending,s=e.memoizedState;if(i!==null){n.pending=null;var o=i=i.next;do s=t(s,o.action),o=o.next;while(o!==i);cn(s,e.memoizedState)||(Pt=!0),e.memoizedState=s,e.baseQueue===null&&(e.baseState=s),n.lastRenderedState=s}return[s,r]}function U1(){}function $1(t,e){var n=Ae,r=Qt(),i=e(),s=!cn(r.memoizedState,i);if(s&&(r.memoizedState=i,Pt=!0),r=r.queue,Sp(H1.bind(null,n,r,t),[t]),r.getSnapshot!==e||s||qe!==null&&qe.memoizedState.tag&1){if(n.flags|=2048,qo(9,B1.bind(null,n,r,i,e),void 0,null),Xe===null)throw Error(x(349));pi&30||z1(n,e,i)}return i}function z1(t,e,n){t.flags|=16384,t={getSnapshot:e,value:n},e=Ae.updateQueue,e===null?(e={lastEffect:null,stores:null},Ae.updateQueue=e,e.stores=[t]):(n=e.stores,n===null?e.stores=[t]:n.push(t))}function B1(t,e,n,r){e.value=n,e.getSnapshot=r,W1(e)&&K1(t)}function H1(t,e,n){return n(function(){W1(e)&&K1(t)})}function W1(t){var e=t.getSnapshot;t=t.value;try{var n=e();return!cn(t,n)}catch{return!0}}function K1(t){var e=Hn(t,1);e!==null&&un(e,t,1,-1)}function Xg(t){var e=mn();return typeof t=="function"&&(t=t()),e.memoizedState=e.baseState=t,t={pending:null,interleaved:null,lanes:0,dispatch:null,lastRenderedReducer:Go,lastRenderedState:t},e.queue=t,t=t.dispatch=xS.bind(null,Ae,t),[e.memoizedState,t]}function qo(t,e,n,r){return t={tag:t,create:e,destroy:n,deps:r,next:null},e=Ae.updateQueue,e===null?(e={lastEffect:null,stores:null},Ae.updateQueue=e,e.lastEffect=t.next=t):(n=e.lastEffect,n===null?e.lastEffect=t.next=t:(r=n.next,n.next=t,t.next=r,e.lastEffect=t)),t}function G1(){return Qt().memoizedState}function Hl(t,e,n,r){var i=mn();Ae.flags|=t,i.memoizedState=qo(1|e,n,void 0,r===void 0?null:r)}function fc(t,e,n,r){var i=Qt();r=r===void 0?null:r;var s=void 0;if(Ue!==null){var o=Ue.memoizedState;if(s=o.destroy,r!==null&&Ep(r,o.deps)){i.memoizedState=qo(e,n,s,r);return}}Ae.flags|=t,i.memoizedState=qo(1|e,n,s,r)}function Jg(t,e){return Hl(8390656,8,t,e)}function Sp(t,e){return fc(2048,8,t,e)}function q1(t,e){return fc(4,2,t,e)}function Q1(t,e){return fc(4,4,t,e)}function Y1(t,e){if(typeof e=="function")return t=t(),e(t),function(){e(null)};if(e!=null)return t=t(),e.current=t,function(){e.current=null}}function X1(t,e,n){return n=n!=null?n.concat([t]):null,fc(4,4,Y1.bind(null,e,t),n)}function kp(){}function J1(t,e){var n=Qt();e=e===void 0?null:e;var r=n.memoizedState;return r!==null&&e!==null&&Ep(e,r[1])?r[0]:(n.memoizedState=[t,e],t)}function Z1(t,e){var n=Qt();e=e===void 0?null:e;var r=n.memoizedState;return r!==null&&e!==null&&Ep(e,r[1])?r[0]:(t=t(),n.memoizedState=[t,e],t)}function e_(t,e,n){return pi&21?(cn(n,e)||(n=r1(),Ae.lanes|=n,mi|=n,t.baseState=!0),e):(t.baseState&&(t.baseState=!1,Pt=!0),t.memoizedState=n)}function kS(t,e){var n=oe;oe=n!==0&&4>n?n:4,t(!0);var r=Af.transition;Af.transition={};try{t(!1),e()}finally{oe=n,Af.transition=r}}function t_(){return Qt().memoizedState}function AS(t,e,n){var r=Pr(t);if(n={lane:r,action:n,hasEagerState:!1,eagerState:null,next:null},n_(t))r_(e,n);else if(n=O1(t,e,n,r),n!==null){var i=Tt();un(n,t,r,i),i_(n,e,r)}}function xS(t,e,n){var r=Pr(t),i={lane:r,action:n,hasEagerState:!1,eagerState:null,next:null};if(n_(t))r_(e,i);else{var s=t.alternate;if(t.lanes===0&&(s===null||s.lanes===0)&&(s=e.lastRenderedReducer,s!==null))try{var o=e.lastRenderedState,a=s(o,n);if(i.hasEagerState=!0,i.eagerState=a,cn(a,o)){var l=e.interleaved;l===null?(i.next=i,gp(e)):(i.next=l.next,l.next=i),e.interleaved=i;return}}catch{}finally{}n=O1(t,e,i,r),n!==null&&(i=Tt(),un(n,t,r,i),i_(n,e,r))}}function n_(t){var e=t.alternate;return t===Ae||e!==null&&e===Ae}function r_(t,e){wo=wu=!0;var n=t.pending;n===null?e.next=e:(e.next=n.next,n.next=e),t.pending=e}function i_(t,e,n){if(n&4194240){var r=e.lanes;r&=t.pendingLanes,n|=r,e.lanes=n,np(t,n)}}var Eu={readContext:qt,useCallback:ut,useContext:ut,useEffect:ut,useImperativeHandle:ut,useInsertionEffect:ut,useLayoutEffect:ut,useMemo:ut,useReducer:ut,useRef:ut,useState:ut,useDebugValue:ut,useDeferredValue:ut,useTransition:ut,useMutableSource:ut,useSyncExternalStore:ut,useId:ut,unstable_isNewReconciler:!1},PS={readContext:qt,useCallback:function(t,e){return mn().memoizedState=[t,e===void 0?null:e],t},useContext:qt,useEffect:Jg,useImperativeHandle:function(t,e,n){return n=n!=null?n.concat([t]):null,Hl(4194308,4,Y1.bind(null,e,t),n)},useLayoutEffect:function(t,e){return Hl(4194308,4,t,e)},useInsertionEffect:function(t,e){return Hl(4,2,t,e)},useMemo:function(t,e){var n=mn();return e=e===void 0?null:e,t=t(),n.memoizedState=[t,e],t},useReducer:function(t,e,n){var r=mn();return e=n!==void 0?n(e):e,r.memoizedState=r.baseState=e,t={pending:null,interleaved:null,lanes:0,dispatch:null,lastRenderedReducer:t,lastRenderedState:e},r.queue=t,t=t.dispatch=AS.bind(null,Ae,t),[r.memoizedState,t]},useRef:function(t){var e=mn();return t={current:t},e.memoizedState=t},useState:Xg,useDebugValue:kp,useDeferredValue:function(t){return mn().memoizedState=t},useTransition:function(){var t=Xg(!1),e=t[0];return t=kS.bind(null,t[1]),mn().memoizedState=t,[e,t]},useMutableSource:function(){},useSyncExternalStore:function(t,e,n){var r=Ae,i=mn();if(Te){if(n===void 0)throw Error(x(407));n=n()}else{if(n=e(),Xe===null)throw Error(x(349));pi&30||z1(r,e,n)}i.memoizedState=n;var s={value:n,getSnapshot:e};return i.queue=s,Jg(H1.bind(null,r,s,t),[t]),r.flags|=2048,qo(9,B1.bind(null,r,s,n,e),void 0,null),n},useId:function(){var t=mn(),e=Xe.identifierPrefix;if(Te){var n=Dn,r=On;n=(r&~(1<<32-ln(r)-1)).toString(32)+n,e=":"+e+"R"+n,n=Ko++,0<n&&(e+="H"+n.toString(32)),e+=":"}else n=SS++,e=":"+e+"r"+n.toString(32)+":";return t.memoizedState=e},unstable_isNewReconciler:!1},RS={readContext:qt,useCallback:J1,useContext:qt,useEffect:Sp,useImperativeHandle:X1,useInsertionEffect:q1,useLayoutEffect:Q1,useMemo:Z1,useReducer:xf,useRef:G1,useState:function(){return xf(Go)},useDebugValue:kp,useDeferredValue:function(t){var e=Qt();return e_(e,Ue.memoizedState,t)},useTransition:function(){var t=xf(Go)[0],e=Qt().memoizedState;return[t,e]},useMutableSource:U1,useSyncExternalStore:$1,useId:t_,unstable_isNewReconciler:!1},CS={readContext:qt,useCallback:J1,useContext:qt,useEffect:Sp,useImperativeHandle:X1,useInsertionEffect:q1,useLayoutEffect:Q1,useMemo:Z1,useReducer:Pf,useRef:G1,useState:function(){return Pf(Go)},useDebugValue:kp,useDeferredValue:function(t){var e=Qt();return Ue===null?e.memoizedState=t:e_(e,Ue.memoizedState,t)},useTransition:function(){var t=Pf(Go)[0],e=Qt().memoizedState;return[t,e]},useMutableSource:U1,useSyncExternalStore:$1,useId:t_,unstable_isNewReconciler:!1};function Es(t,e){try{var n="",r=e;do n+=i2(r),r=r.return;while(r);var i=n}catch(s){i=`
Error generating stack: `+s.message+`
`+s.stack}return{value:t,source:e,stack:i,digest:null}}function Rf(t,e,n){return{value:t,source:null,stack:n??null,digest:e??null}}function Vh(t,e){try{console.error(e.value)}catch(n){setTimeout(function(){throw n})}}var NS=typeof WeakMap=="function"?WeakMap:Map;function s_(t,e,n){n=Vn(-1,n),n.tag=3,n.payload={element:null};var r=e.value;return n.callback=function(){Iu||(Iu=!0,qh=r),Vh(t,e)},n}function o_(t,e,n){n=Vn(-1,n),n.tag=3;var r=t.type.getDerivedStateFromError;if(typeof r=="function"){var i=e.value;n.payload=function(){return r(i)},n.callback=function(){Vh(t,e)}}var s=t.stateNode;return s!==null&&typeof s.componentDidCatch=="function"&&(n.callback=function(){Vh(t,e),typeof r!="function"&&(xr===null?xr=new Set([this]):xr.add(this));var o=e.stack;this.componentDidCatch(e.value,{componentStack:o!==null?o:""})}),n}function Zg(t,e,n){var r=t.pingCache;if(r===null){r=t.pingCache=new NS;var i=new Set;r.set(e,i)}else i=r.get(e),i===void 0&&(i=new Set,r.set(e,i));i.has(n)||(i.add(n),t=WS.bind(null,t,e,n),e.then(t,t))}function ev(t){do{var e;if((e=t.tag===13)&&(e=t.memoizedState,e=e!==null?e.dehydrated!==null:!0),e)return t;t=t.return}while(t!==null);return null}function tv(t,e,n,r,i){return t.mode&1?(t.flags|=65536,t.lanes=i,t):(t===e?t.flags|=65536:(t.flags|=128,n.flags|=131072,n.flags&=-52805,n.tag===1&&(n.alternate===null?n.tag=17:(e=Vn(-1,1),e.tag=2,Ar(n,e,1))),n.lanes|=1),t)}var bS=Zn.ReactCurrentOwner,Pt=!1;function wt(t,e,n,r){e.child=t===null?V1(e,null,n,r):_s(e,t.child,n,r)}function nv(t,e,n,r,i){n=n.render;var s=e.ref;return as(e,i),r=Tp(t,e,n,r,s,i),n=Ip(),t!==null&&!Pt?(e.updateQueue=t.updateQueue,e.flags&=-2053,t.lanes&=~i,Wn(t,e,i)):(Te&&n&&cp(e),e.flags|=1,wt(t,e,r,i),e.child)}function rv(t,e,n,r,i){if(t===null){var s=n.type;return typeof s=="function"&&!Op(s)&&s.defaultProps===void 0&&n.compare===null&&n.defaultProps===void 0?(e.tag=15,e.type=s,a_(t,e,s,r,i)):(t=ql(n.type,null,r,e,e.mode,i),t.ref=e.ref,t.return=e,e.child=t)}if(s=t.child,!(t.lanes&i)){var o=s.memoizedProps;if(n=n.compare,n=n!==null?n:Uo,n(o,r)&&t.ref===e.ref)return Wn(t,e,i)}return e.flags|=1,t=Rr(s,r),t.ref=e.ref,t.return=e,e.child=t}function a_(t,e,n,r,i){if(t!==null){var s=t.memoizedProps;if(Uo(s,r)&&t.ref===e.ref)if(Pt=!1,e.pendingProps=r=s,(t.lanes&i)!==0)t.flags&131072&&(Pt=!0);else return e.lanes=t.lanes,Wn(t,e,i)}return Fh(t,e,n,r,i)}function l_(t,e,n){var r=e.pendingProps,i=r.children,s=t!==null?t.memoizedState:null;if(r.mode==="hidden")if(!(e.mode&1))e.memoizedState={baseLanes:0,cachePool:null,transitions:null},pe(Qi,Dt),Dt|=n;else{if(!(n&1073741824))return t=s!==null?s.baseLanes|n:n,e.lanes=e.childLanes=1073741824,e.memoizedState={baseLanes:t,cachePool:null,transitions:null},e.updateQueue=null,pe(Qi,Dt),Dt|=t,null;e.memoizedState={baseLanes:0,cachePool:null,transitions:null},r=s!==null?s.baseLanes:n,pe(Qi,Dt),Dt|=r}else s!==null?(r=s.baseLanes|n,e.memoizedState=null):r=n,pe(Qi,Dt),Dt|=r;return wt(t,e,i,n),e.child}function u_(t,e){var n=e.ref;(t===null&&n!==null||t!==null&&t.ref!==n)&&(e.flags|=512,e.flags|=2097152)}function Fh(t,e,n,r,i){var s=Ct(n)?hi:yt.current;return s=vs(e,s),as(e,i),n=Tp(t,e,n,r,s,i),r=Ip(),t!==null&&!Pt?(e.updateQueue=t.updateQueue,e.flags&=-2053,t.lanes&=~i,Wn(t,e,i)):(Te&&r&&cp(e),e.flags|=1,wt(t,e,n,i),e.child)}function iv(t,e,n,r,i){if(Ct(n)){var s=!0;du(e)}else s=!1;if(as(e,i),e.stateNode===null)Wl(t,e),L1(e,n,r),jh(e,n,r,i),r=!0;else if(t===null){var o=e.stateNode,a=e.memoizedProps;o.props=a;var l=o.context,u=n.contextType;typeof u=="object"&&u!==null?u=qt(u):(u=Ct(n)?hi:yt.current,u=vs(e,u));var c=n.getDerivedStateFromProps,h=typeof c=="function"||typeof o.getSnapshotBeforeUpdate=="function";h||typeof o.UNSAFE_componentWillReceiveProps!="function"&&typeof o.componentWillReceiveProps!="function"||(a!==r||l!==u)&&Qg(e,o,r,u),hr=!1;var d=e.memoizedState;o.state=d,yu(e,r,o,i),l=e.memoizedState,a!==r||d!==l||Rt.current||hr?(typeof c=="function"&&(Lh(e,n,c,r),l=e.memoizedState),(a=hr||qg(e,n,a,r,d,l,u))?(h||typeof o.UNSAFE_componentWillMount!="function"&&typeof o.componentWillMount!="function"||(typeof o.componentWillMount=="function"&&o.componentWillMount(),typeof o.UNSAFE_componentWillMount=="function"&&o.UNSAFE_componentWillMount()),typeof o.componentDidMount=="function"&&(e.flags|=4194308)):(typeof o.componentDidMount=="function"&&(e.flags|=4194308),e.memoizedProps=r,e.memoizedState=l),o.props=r,o.state=l,o.context=u,r=a):(typeof o.componentDidMount=="function"&&(e.flags|=4194308),r=!1)}else{o=e.stateNode,D1(t,e),a=e.memoizedProps,u=e.type===e.elementType?a:Jt(e.type,a),o.props=u,h=e.pendingProps,d=o.context,l=n.contextType,typeof l=="object"&&l!==null?l=qt(l):(l=Ct(n)?hi:yt.current,l=vs(e,l));var y=n.getDerivedStateFromProps;(c=typeof y=="function"||typeof o.getSnapshotBeforeUpdate=="function")||typeof o.UNSAFE_componentWillReceiveProps!="function"&&typeof o.componentWillReceiveProps!="function"||(a!==h||d!==l)&&Qg(e,o,r,l),hr=!1,d=e.memoizedState,o.state=d,yu(e,r,o,i);var _=e.memoizedState;a!==h||d!==_||Rt.current||hr?(typeof y=="function"&&(Lh(e,n,y,r),_=e.memoizedState),(u=hr||qg(e,n,u,r,d,_,l)||!1)?(c||typeof o.UNSAFE_componentWillUpdate!="function"&&typeof o.componentWillUpdate!="function"||(typeof o.componentWillUpdate=="function"&&o.componentWillUpdate(r,_,l),typeof o.UNSAFE_componentWillUpdate=="function"&&o.UNSAFE_componentWillUpdate(r,_,l)),typeof o.componentDidUpdate=="function"&&(e.flags|=4),typeof o.getSnapshotBeforeUpdate=="function"&&(e.flags|=1024)):(typeof o.componentDidUpdate!="function"||a===t.memoizedProps&&d===t.memoizedState||(e.flags|=4),typeof o.getSnapshotBeforeUpdate!="function"||a===t.memoizedProps&&d===t.memoizedState||(e.flags|=1024),e.memoizedProps=r,e.memoizedState=_),o.props=r,o.state=_,o.context=l,r=u):(typeof o.componentDidUpdate!="function"||a===t.memoizedProps&&d===t.memoizedState||(e.flags|=4),typeof o.getSnapshotBeforeUpdate!="function"||a===t.memoizedProps&&d===t.memoizedState||(e.flags|=1024),r=!1)}return Uh(t,e,n,r,s,i)}function Uh(t,e,n,r,i,s){u_(t,e);var o=(e.flags&128)!==0;if(!r&&!o)return i&&Bg(e,n,!1),Wn(t,e,s);r=e.stateNode,bS.current=e;var a=o&&typeof n.getDerivedStateFromError!="function"?null:r.render();return e.flags|=1,t!==null&&o?(e.child=_s(e,t.child,null,s),e.child=_s(e,null,a,s)):wt(t,e,a,s),e.memoizedState=r.state,i&&Bg(e,n,!0),e.child}function c_(t){var e=t.stateNode;e.pendingContext?zg(t,e.pendingContext,e.pendingContext!==e.context):e.context&&zg(t,e.context,!1),yp(t,e.containerInfo)}function sv(t,e,n,r,i){return ys(),hp(i),e.flags|=256,wt(t,e,n,r),e.child}var $h={dehydrated:null,treeContext:null,retryLane:0};function zh(t){return{baseLanes:t,cachePool:null,transitions:null}}function f_(t,e,n){var r=e.pendingProps,i=ke.current,s=!1,o=(e.flags&128)!==0,a;if((a=o)||(a=t!==null&&t.memoizedState===null?!1:(i&2)!==0),a?(s=!0,e.flags&=-129):(t===null||t.memoizedState!==null)&&(i|=1),pe(ke,i&1),t===null)return Dh(e),t=e.memoizedState,t!==null&&(t=t.dehydrated,t!==null)?(e.mode&1?t.data==="$!"?e.lanes=8:e.lanes=1073741824:e.lanes=1,null):(o=r.children,t=r.fallback,s?(r=e.mode,s=e.child,o={mode:"hidden",children:o},!(r&1)&&s!==null?(s.childLanes=0,s.pendingProps=o):s=pc(o,r,0,null),t=li(t,r,n,null),s.return=e,t.return=e,s.sibling=t,e.child=s,e.child.memoizedState=zh(n),e.memoizedState=$h,t):Ap(e,o));if(i=t.memoizedState,i!==null&&(a=i.dehydrated,a!==null))return OS(t,e,o,r,a,i,n);if(s){s=r.fallback,o=e.mode,i=t.child,a=i.sibling;var l={mode:"hidden",children:r.children};return!(o&1)&&e.child!==i?(r=e.child,r.childLanes=0,r.pendingProps=l,e.deletions=null):(r=Rr(i,l),r.subtreeFlags=i.subtreeFlags&14680064),a!==null?s=Rr(a,s):(s=li(s,o,n,null),s.flags|=2),s.return=e,r.return=e,r.sibling=s,e.child=r,r=s,s=e.child,o=t.child.memoizedState,o=o===null?zh(n):{baseLanes:o.baseLanes|n,cachePool:null,transitions:o.transitions},s.memoizedState=o,s.childLanes=t.childLanes&~n,e.memoizedState=$h,r}return s=t.child,t=s.sibling,r=Rr(s,{mode:"visible",children:r.children}),!(e.mode&1)&&(r.lanes=n),r.return=e,r.sibling=null,t!==null&&(n=e.deletions,n===null?(e.deletions=[t],e.flags|=16):n.push(t)),e.child=r,e.memoizedState=null,r}function Ap(t,e){return e=pc({mode:"visible",children:e},t.mode,0,null),e.return=t,t.child=e}function dl(t,e,n,r){return r!==null&&hp(r),_s(e,t.child,null,n),t=Ap(e,e.pendingProps.children),t.flags|=2,e.memoizedState=null,t}function OS(t,e,n,r,i,s,o){if(n)return e.flags&256?(e.flags&=-257,r=Rf(Error(x(422))),dl(t,e,o,r)):e.memoizedState!==null?(e.child=t.child,e.flags|=128,null):(s=r.fallback,i=e.mode,r=pc({mode:"visible",children:r.children},i,0,null),s=li(s,i,o,null),s.flags|=2,r.return=e,s.return=e,r.sibling=s,e.child=r,e.mode&1&&_s(e,t.child,null,o),e.child.memoizedState=zh(o),e.memoizedState=$h,s);if(!(e.mode&1))return dl(t,e,o,null);if(i.data==="$!"){if(r=i.nextSibling&&i.nextSibling.dataset,r)var a=r.dgst;return r=a,s=Error(x(419)),r=Rf(s,r,void 0),dl(t,e,o,r)}if(a=(o&t.childLanes)!==0,Pt||a){if(r=Xe,r!==null){switch(o&-o){case 4:i=2;break;case 16:i=8;break;case 64:case 128:case 256:case 512:case 1024:case 2048:case 4096:case 8192:case 16384:case 32768:case 65536:case 131072:case 262144:case 524288:case 1048576:case 2097152:case 4194304:case 8388608:case 16777216:case 33554432:case 67108864:i=32;break;case 536870912:i=268435456;break;default:i=0}i=i&(r.suspendedLanes|o)?0:i,i!==0&&i!==s.retryLane&&(s.retryLane=i,Hn(t,i),un(r,t,i,-1))}return bp(),r=Rf(Error(x(421))),dl(t,e,o,r)}return i.data==="$?"?(e.flags|=128,e.child=t.child,e=KS.bind(null,t),i._reactRetry=e,null):(t=s.treeContext,Mt=kr(i.nextSibling),Lt=e,Te=!0,nn=null,t!==null&&(Bt[Ht++]=On,Bt[Ht++]=Dn,Bt[Ht++]=di,On=t.id,Dn=t.overflow,di=e),e=Ap(e,r.children),e.flags|=4096,e)}function ov(t,e,n){t.lanes|=e;var r=t.alternate;r!==null&&(r.lanes|=e),Mh(t.return,e,n)}function Cf(t,e,n,r,i){var s=t.memoizedState;s===null?t.memoizedState={isBackwards:e,rendering:null,renderingStartTime:0,last:r,tail:n,tailMode:i}:(s.isBackwards=e,s.rendering=null,s.renderingStartTime=0,s.last=r,s.tail=n,s.tailMode=i)}function h_(t,e,n){var r=e.pendingProps,i=r.revealOrder,s=r.tail;if(wt(t,e,r.children,n),r=ke.current,r&2)r=r&1|2,e.flags|=128;else{if(t!==null&&t.flags&128)e:for(t=e.child;t!==null;){if(t.tag===13)t.memoizedState!==null&&ov(t,n,e);else if(t.tag===19)ov(t,n,e);else if(t.child!==null){t.child.return=t,t=t.child;continue}if(t===e)break e;for(;t.sibling===null;){if(t.return===null||t.return===e)break e;t=t.return}t.sibling.return=t.return,t=t.sibling}r&=1}if(pe(ke,r),!(e.mode&1))e.memoizedState=null;else switch(i){case"forwards":for(n=e.child,i=null;n!==null;)t=n.alternate,t!==null&&_u(t)===null&&(i=n),n=n.sibling;n=i,n===null?(i=e.child,e.child=null):(i=n.sibling,n.sibling=null),Cf(e,!1,i,n,s);break;case"backwards":for(n=null,i=e.child,e.child=null;i!==null;){if(t=i.alternate,t!==null&&_u(t)===null){e.child=i;break}t=i.sibling,i.sibling=n,n=i,i=t}Cf(e,!0,n,null,s);break;case"together":Cf(e,!1,null,null,void 0);break;default:e.memoizedState=null}return e.child}function Wl(t,e){!(e.mode&1)&&t!==null&&(t.alternate=null,e.alternate=null,e.flags|=2)}function Wn(t,e,n){if(t!==null&&(e.dependencies=t.dependencies),mi|=e.lanes,!(n&e.childLanes))return null;if(t!==null&&e.child!==t.child)throw Error(x(153));if(e.child!==null){for(t=e.child,n=Rr(t,t.pendingProps),e.child=n,n.return=e;t.sibling!==null;)t=t.sibling,n=n.sibling=Rr(t,t.pendingProps),n.return=e;n.sibling=null}return e.child}function DS(t,e,n){switch(e.tag){case 3:c_(e),ys();break;case 5:F1(e);break;case 1:Ct(e.type)&&du(e);break;case 4:yp(e,e.stateNode.containerInfo);break;case 10:var r=e.type._context,i=e.memoizedProps.value;pe(gu,r._currentValue),r._currentValue=i;break;case 13:if(r=e.memoizedState,r!==null)return r.dehydrated!==null?(pe(ke,ke.current&1),e.flags|=128,null):n&e.child.childLanes?f_(t,e,n):(pe(ke,ke.current&1),t=Wn(t,e,n),t!==null?t.sibling:null);pe(ke,ke.current&1);break;case 19:if(r=(n&e.childLanes)!==0,t.flags&128){if(r)return h_(t,e,n);e.flags|=128}if(i=e.memoizedState,i!==null&&(i.rendering=null,i.tail=null,i.lastEffect=null),pe(ke,ke.current),r)break;return null;case 22:case 23:return e.lanes=0,l_(t,e,n)}return Wn(t,e,n)}var d_,Bh,p_,m_;d_=function(t,e){for(var n=e.child;n!==null;){if(n.tag===5||n.tag===6)t.appendChild(n.stateNode);else if(n.tag!==4&&n.child!==null){n.child.return=n,n=n.child;continue}if(n===e)break;for(;n.sibling===null;){if(n.return===null||n.return===e)return;n=n.return}n.sibling.return=n.return,n=n.sibling}};Bh=function(){};p_=function(t,e,n,r){var i=t.memoizedProps;if(i!==r){t=e.stateNode,ei(Tn.current);var s=null;switch(n){case"input":i=fh(t,i),r=fh(t,r),s=[];break;case"select":i=xe({},i,{value:void 0}),r=xe({},r,{value:void 0}),s=[];break;case"textarea":i=ph(t,i),r=ph(t,r),s=[];break;default:typeof i.onClick!="function"&&typeof r.onClick=="function"&&(t.onclick=fu)}gh(n,r);var o;n=null;for(u in i)if(!r.hasOwnProperty(u)&&i.hasOwnProperty(u)&&i[u]!=null)if(u==="style"){var a=i[u];for(o in a)a.hasOwnProperty(o)&&(n||(n={}),n[o]="")}else u!=="dangerouslySetInnerHTML"&&u!=="children"&&u!=="suppressContentEditableWarning"&&u!=="suppressHydrationWarning"&&u!=="autoFocus"&&(Oo.hasOwnProperty(u)?s||(s=[]):(s=s||[]).push(u,null));for(u in r){var l=r[u];if(a=i!=null?i[u]:void 0,r.hasOwnProperty(u)&&l!==a&&(l!=null||a!=null))if(u==="style")if(a){for(o in a)!a.hasOwnProperty(o)||l&&l.hasOwnProperty(o)||(n||(n={}),n[o]="");for(o in l)l.hasOwnProperty(o)&&a[o]!==l[o]&&(n||(n={}),n[o]=l[o])}else n||(s||(s=[]),s.push(u,n)),n=l;else u==="dangerouslySetInnerHTML"?(l=l?l.__html:void 0,a=a?a.__html:void 0,l!=null&&a!==l&&(s=s||[]).push(u,l)):u==="children"?typeof l!="string"&&typeof l!="number"||(s=s||[]).push(u,""+l):u!=="suppressContentEditableWarning"&&u!=="suppressHydrationWarning"&&(Oo.hasOwnProperty(u)?(l!=null&&u==="onScroll"&&me("scroll",t),s||a===l||(s=[])):(s=s||[]).push(u,l))}n&&(s=s||[]).push("style",n);var u=s;(e.updateQueue=u)&&(e.flags|=4)}};m_=function(t,e,n,r){n!==r&&(e.flags|=4)};function Zs(t,e){if(!Te)switch(t.tailMode){case"hidden":e=t.tail;for(var n=null;e!==null;)e.alternate!==null&&(n=e),e=e.sibling;n===null?t.tail=null:n.sibling=null;break;case"collapsed":n=t.tail;for(var r=null;n!==null;)n.alternate!==null&&(r=n),n=n.sibling;r===null?e||t.tail===null?t.tail=null:t.tail.sibling=null:r.sibling=null}}function ct(t){var e=t.alternate!==null&&t.alternate.child===t.child,n=0,r=0;if(e)for(var i=t.child;i!==null;)n|=i.lanes|i.childLanes,r|=i.subtreeFlags&14680064,r|=i.flags&14680064,i.return=t,i=i.sibling;else for(i=t.child;i!==null;)n|=i.lanes|i.childLanes,r|=i.subtreeFlags,r|=i.flags,i.return=t,i=i.sibling;return t.subtreeFlags|=r,t.childLanes=n,e}function MS(t,e,n){var r=e.pendingProps;switch(fp(e),e.tag){case 2:case 16:case 15:case 0:case 11:case 7:case 8:case 12:case 9:case 14:return ct(e),null;case 1:return Ct(e.type)&&hu(),ct(e),null;case 3:return r=e.stateNode,ws(),ye(Rt),ye(yt),wp(),r.pendingContext&&(r.context=r.pendingContext,r.pendingContext=null),(t===null||t.child===null)&&(fl(e)?e.flags|=4:t===null||t.memoizedState.isDehydrated&&!(e.flags&256)||(e.flags|=1024,nn!==null&&(Xh(nn),nn=null))),Bh(t,e),ct(e),null;case 5:_p(e);var i=ei(Wo.current);if(n=e.type,t!==null&&e.stateNode!=null)p_(t,e,n,r,i),t.ref!==e.ref&&(e.flags|=512,e.flags|=2097152);else{if(!r){if(e.stateNode===null)throw Error(x(166));return ct(e),null}if(t=ei(Tn.current),fl(e)){r=e.stateNode,n=e.type;var s=e.memoizedProps;switch(r[gn]=e,r[Bo]=s,t=(e.mode&1)!==0,n){case"dialog":me("cancel",r),me("close",r);break;case"iframe":case"object":case"embed":me("load",r);break;case"video":case"audio":for(i=0;i<co.length;i++)me(co[i],r);break;case"source":me("error",r);break;case"img":case"image":case"link":me("error",r),me("load",r);break;case"details":me("toggle",r);break;case"input":mg(r,s),me("invalid",r);break;case"select":r._wrapperState={wasMultiple:!!s.multiple},me("invalid",r);break;case"textarea":vg(r,s),me("invalid",r)}gh(n,s),i=null;for(var o in s)if(s.hasOwnProperty(o)){var a=s[o];o==="children"?typeof a=="string"?r.textContent!==a&&(s.suppressHydrationWarning!==!0&&cl(r.textContent,a,t),i=["children",a]):typeof a=="number"&&r.textContent!==""+a&&(s.suppressHydrationWarning!==!0&&cl(r.textContent,a,t),i=["children",""+a]):Oo.hasOwnProperty(o)&&a!=null&&o==="onScroll"&&me("scroll",r)}switch(n){case"input":nl(r),gg(r,s,!0);break;case"textarea":nl(r),yg(r);break;case"select":case"option":break;default:typeof s.onClick=="function"&&(r.onclick=fu)}r=i,e.updateQueue=r,r!==null&&(e.flags|=4)}else{o=i.nodeType===9?i:i.ownerDocument,t==="http://www.w3.org/1999/xhtml"&&(t=z0(n)),t==="http://www.w3.org/1999/xhtml"?n==="script"?(t=o.createElement("div"),t.innerHTML="<script><\/script>",t=t.removeChild(t.firstChild)):typeof r.is=="string"?t=o.createElement(n,{is:r.is}):(t=o.createElement(n),n==="select"&&(o=t,r.multiple?o.multiple=!0:r.size&&(o.size=r.size))):t=o.createElementNS(t,n),t[gn]=e,t[Bo]=r,d_(t,e,!1,!1),e.stateNode=t;e:{switch(o=vh(n,r),n){case"dialog":me("cancel",t),me("close",t),i=r;break;case"iframe":case"object":case"embed":me("load",t),i=r;break;case"video":case"audio":for(i=0;i<co.length;i++)me(co[i],t);i=r;break;case"source":me("error",t),i=r;break;case"img":case"image":case"link":me("error",t),me("load",t),i=r;break;case"details":me("toggle",t),i=r;break;case"input":mg(t,r),i=fh(t,r),me("invalid",t);break;case"option":i=r;break;case"select":t._wrapperState={wasMultiple:!!r.multiple},i=xe({},r,{value:void 0}),me("invalid",t);break;case"textarea":vg(t,r),i=ph(t,r),me("invalid",t);break;default:i=r}gh(n,i),a=i;for(s in a)if(a.hasOwnProperty(s)){var l=a[s];s==="style"?W0(t,l):s==="dangerouslySetInnerHTML"?(l=l?l.__html:void 0,l!=null&&B0(t,l)):s==="children"?typeof l=="string"?(n!=="textarea"||l!=="")&&Do(t,l):typeof l=="number"&&Do(t,""+l):s!=="suppressContentEditableWarning"&&s!=="suppressHydrationWarning"&&s!=="autoFocus"&&(Oo.hasOwnProperty(s)?l!=null&&s==="onScroll"&&me("scroll",t):l!=null&&Yd(t,s,l,o))}switch(n){case"input":nl(t),gg(t,r,!1);break;case"textarea":nl(t),yg(t);break;case"option":r.value!=null&&t.setAttribute("value",""+Or(r.value));break;case"select":t.multiple=!!r.multiple,s=r.value,s!=null?rs(t,!!r.multiple,s,!1):r.defaultValue!=null&&rs(t,!!r.multiple,r.defaultValue,!0);break;default:typeof i.onClick=="function"&&(t.onclick=fu)}switch(n){case"button":case"input":case"select":case"textarea":r=!!r.autoFocus;break e;case"img":r=!0;break e;default:r=!1}}r&&(e.flags|=4)}e.ref!==null&&(e.flags|=512,e.flags|=2097152)}return ct(e),null;case 6:if(t&&e.stateNode!=null)m_(t,e,t.memoizedProps,r);else{if(typeof r!="string"&&e.stateNode===null)throw Error(x(166));if(n=ei(Wo.current),ei(Tn.current),fl(e)){if(r=e.stateNode,n=e.memoizedProps,r[gn]=e,(s=r.nodeValue!==n)&&(t=Lt,t!==null))switch(t.tag){case 3:cl(r.nodeValue,n,(t.mode&1)!==0);break;case 5:t.memoizedProps.suppressHydrationWarning!==!0&&cl(r.nodeValue,n,(t.mode&1)!==0)}s&&(e.flags|=4)}else r=(n.nodeType===9?n:n.ownerDocument).createTextNode(r),r[gn]=e,e.stateNode=r}return ct(e),null;case 13:if(ye(ke),r=e.memoizedState,t===null||t.memoizedState!==null&&t.memoizedState.dehydrated!==null){if(Te&&Mt!==null&&e.mode&1&&!(e.flags&128))b1(),ys(),e.flags|=98560,s=!1;else if(s=fl(e),r!==null&&r.dehydrated!==null){if(t===null){if(!s)throw Error(x(318));if(s=e.memoizedState,s=s!==null?s.dehydrated:null,!s)throw Error(x(317));s[gn]=e}else ys(),!(e.flags&128)&&(e.memoizedState=null),e.flags|=4;ct(e),s=!1}else nn!==null&&(Xh(nn),nn=null),s=!0;if(!s)return e.flags&65536?e:null}return e.flags&128?(e.lanes=n,e):(r=r!==null,r!==(t!==null&&t.memoizedState!==null)&&r&&(e.child.flags|=8192,e.mode&1&&(t===null||ke.current&1?$e===0&&($e=3):bp())),e.updateQueue!==null&&(e.flags|=4),ct(e),null);case 4:return ws(),Bh(t,e),t===null&&$o(e.stateNode.containerInfo),ct(e),null;case 10:return mp(e.type._context),ct(e),null;case 17:return Ct(e.type)&&hu(),ct(e),null;case 19:if(ye(ke),s=e.memoizedState,s===null)return ct(e),null;if(r=(e.flags&128)!==0,o=s.rendering,o===null)if(r)Zs(s,!1);else{if($e!==0||t!==null&&t.flags&128)for(t=e.child;t!==null;){if(o=_u(t),o!==null){for(e.flags|=128,Zs(s,!1),r=o.updateQueue,r!==null&&(e.updateQueue=r,e.flags|=4),e.subtreeFlags=0,r=n,n=e.child;n!==null;)s=n,t=r,s.flags&=14680066,o=s.alternate,o===null?(s.childLanes=0,s.lanes=t,s.child=null,s.subtreeFlags=0,s.memoizedProps=null,s.memoizedState=null,s.updateQueue=null,s.dependencies=null,s.stateNode=null):(s.childLanes=o.childLanes,s.lanes=o.lanes,s.child=o.child,s.subtreeFlags=0,s.deletions=null,s.memoizedProps=o.memoizedProps,s.memoizedState=o.memoizedState,s.updateQueue=o.updateQueue,s.type=o.type,t=o.dependencies,s.dependencies=t===null?null:{lanes:t.lanes,firstContext:t.firstContext}),n=n.sibling;return pe(ke,ke.current&1|2),e.child}t=t.sibling}s.tail!==null&&be()>Ts&&(e.flags|=128,r=!0,Zs(s,!1),e.lanes=4194304)}else{if(!r)if(t=_u(o),t!==null){if(e.flags|=128,r=!0,n=t.updateQueue,n!==null&&(e.updateQueue=n,e.flags|=4),Zs(s,!0),s.tail===null&&s.tailMode==="hidden"&&!o.alternate&&!Te)return ct(e),null}else 2*be()-s.renderingStartTime>Ts&&n!==1073741824&&(e.flags|=128,r=!0,Zs(s,!1),e.lanes=4194304);s.isBackwards?(o.sibling=e.child,e.child=o):(n=s.last,n!==null?n.sibling=o:e.child=o,s.last=o)}return s.tail!==null?(e=s.tail,s.rendering=e,s.tail=e.sibling,s.renderingStartTime=be(),e.sibling=null,n=ke.current,pe(ke,r?n&1|2:n&1),e):(ct(e),null);case 22:case 23:return Np(),r=e.memoizedState!==null,t!==null&&t.memoizedState!==null!==r&&(e.flags|=8192),r&&e.mode&1?Dt&1073741824&&(ct(e),e.subtreeFlags&6&&(e.flags|=8192)):ct(e),null;case 24:return null;case 25:return null}throw Error(x(156,e.tag))}function LS(t,e){switch(fp(e),e.tag){case 1:return Ct(e.type)&&hu(),t=e.flags,t&65536?(e.flags=t&-65537|128,e):null;case 3:return ws(),ye(Rt),ye(yt),wp(),t=e.flags,t&65536&&!(t&128)?(e.flags=t&-65537|128,e):null;case 5:return _p(e),null;case 13:if(ye(ke),t=e.memoizedState,t!==null&&t.dehydrated!==null){if(e.alternate===null)throw Error(x(340));ys()}return t=e.flags,t&65536?(e.flags=t&-65537|128,e):null;case 19:return ye(ke),null;case 4:return ws(),null;case 10:return mp(e.type._context),null;case 22:case 23:return Np(),null;case 24:return null;default:return null}}var pl=!1,dt=!1,jS=typeof WeakSet=="function"?WeakSet:Set,L=null;function qi(t,e){var n=t.ref;if(n!==null)if(typeof n=="function")try{n(null)}catch(r){Pe(t,e,r)}else n.current=null}function Hh(t,e,n){try{n()}catch(r){Pe(t,e,r)}}var av=!1;function VS(t,e){if(xh=lu,t=_1(),up(t)){if("selectionStart"in t)var n={start:t.selectionStart,end:t.selectionEnd};else e:{n=(n=t.ownerDocument)&&n.defaultView||window;var r=n.getSelection&&n.getSelection();if(r&&r.rangeCount!==0){n=r.anchorNode;var i=r.anchorOffset,s=r.focusNode;r=r.focusOffset;try{n.nodeType,s.nodeType}catch{n=null;break e}var o=0,a=-1,l=-1,u=0,c=0,h=t,d=null;t:for(;;){for(var y;h!==n||i!==0&&h.nodeType!==3||(a=o+i),h!==s||r!==0&&h.nodeType!==3||(l=o+r),h.nodeType===3&&(o+=h.nodeValue.length),(y=h.firstChild)!==null;)d=h,h=y;for(;;){if(h===t)break t;if(d===n&&++u===i&&(a=o),d===s&&++c===r&&(l=o),(y=h.nextSibling)!==null)break;h=d,d=h.parentNode}h=y}n=a===-1||l===-1?null:{start:a,end:l}}else n=null}n=n||{start:0,end:0}}else n=null;for(Ph={focusedElem:t,selectionRange:n},lu=!1,L=e;L!==null;)if(e=L,t=e.child,(e.subtreeFlags&1028)!==0&&t!==null)t.return=e,L=t;else for(;L!==null;){e=L;try{var _=e.alternate;if(e.flags&1024)switch(e.tag){case 0:case 11:case 15:break;case 1:if(_!==null){var w=_.memoizedProps,T=_.memoizedState,g=e.stateNode,p=g.getSnapshotBeforeUpdate(e.elementType===e.type?w:Jt(e.type,w),T);g.__reactInternalSnapshotBeforeUpdate=p}break;case 3:var v=e.stateNode.containerInfo;v.nodeType===1?v.textContent="":v.nodeType===9&&v.documentElement&&v.removeChild(v.documentElement);break;case 5:case 6:case 4:case 17:break;default:throw Error(x(163))}}catch(m){Pe(e,e.return,m)}if(t=e.sibling,t!==null){t.return=e.return,L=t;break}L=e.return}return _=av,av=!1,_}function Eo(t,e,n){var r=e.updateQueue;if(r=r!==null?r.lastEffect:null,r!==null){var i=r=r.next;do{if((i.tag&t)===t){var s=i.destroy;i.destroy=void 0,s!==void 0&&Hh(e,n,s)}i=i.next}while(i!==r)}}function hc(t,e){if(e=e.updateQueue,e=e!==null?e.lastEffect:null,e!==null){var n=e=e.next;do{if((n.tag&t)===t){var r=n.create;n.destroy=r()}n=n.next}while(n!==e)}}function Wh(t){var e=t.ref;if(e!==null){var n=t.stateNode;switch(t.tag){case 5:t=n;break;default:t=n}typeof e=="function"?e(t):e.current=t}}function g_(t){var e=t.alternate;e!==null&&(t.alternate=null,g_(e)),t.child=null,t.deletions=null,t.sibling=null,t.tag===5&&(e=t.stateNode,e!==null&&(delete e[gn],delete e[Bo],delete e[Nh],delete e[wS],delete e[ES])),t.stateNode=null,t.return=null,t.dependencies=null,t.memoizedProps=null,t.memoizedState=null,t.pendingProps=null,t.stateNode=null,t.updateQueue=null}function v_(t){return t.tag===5||t.tag===3||t.tag===4}function lv(t){e:for(;;){for(;t.sibling===null;){if(t.return===null||v_(t.return))return null;t=t.return}for(t.sibling.return=t.return,t=t.sibling;t.tag!==5&&t.tag!==6&&t.tag!==18;){if(t.flags&2||t.child===null||t.tag===4)continue e;t.child.return=t,t=t.child}if(!(t.flags&2))return t.stateNode}}function Kh(t,e,n){var r=t.tag;if(r===5||r===6)t=t.stateNode,e?n.nodeType===8?n.parentNode.insertBefore(t,e):n.insertBefore(t,e):(n.nodeType===8?(e=n.parentNode,e.insertBefore(t,n)):(e=n,e.appendChild(t)),n=n._reactRootContainer,n!=null||e.onclick!==null||(e.onclick=fu));else if(r!==4&&(t=t.child,t!==null))for(Kh(t,e,n),t=t.sibling;t!==null;)Kh(t,e,n),t=t.sibling}function Gh(t,e,n){var r=t.tag;if(r===5||r===6)t=t.stateNode,e?n.insertBefore(t,e):n.appendChild(t);else if(r!==4&&(t=t.child,t!==null))for(Gh(t,e,n),t=t.sibling;t!==null;)Gh(t,e,n),t=t.sibling}var tt=null,Zt=!1;function lr(t,e,n){for(n=n.child;n!==null;)y_(t,e,n),n=n.sibling}function y_(t,e,n){if(En&&typeof En.onCommitFiberUnmount=="function")try{En.onCommitFiberUnmount(ic,n)}catch{}switch(n.tag){case 5:dt||qi(n,e);case 6:var r=tt,i=Zt;tt=null,lr(t,e,n),tt=r,Zt=i,tt!==null&&(Zt?(t=tt,n=n.stateNode,t.nodeType===8?t.parentNode.removeChild(n):t.removeChild(n)):tt.removeChild(n.stateNode));break;case 18:tt!==null&&(Zt?(t=tt,n=n.stateNode,t.nodeType===8?If(t.parentNode,n):t.nodeType===1&&If(t,n),Vo(t)):If(tt,n.stateNode));break;case 4:r=tt,i=Zt,tt=n.stateNode.containerInfo,Zt=!0,lr(t,e,n),tt=r,Zt=i;break;case 0:case 11:case 14:case 15:if(!dt&&(r=n.updateQueue,r!==null&&(r=r.lastEffect,r!==null))){i=r=r.next;do{var s=i,o=s.destroy;s=s.tag,o!==void 0&&(s&2||s&4)&&Hh(n,e,o),i=i.next}while(i!==r)}lr(t,e,n);break;case 1:if(!dt&&(qi(n,e),r=n.stateNode,typeof r.componentWillUnmount=="function"))try{r.props=n.memoizedProps,r.state=n.memoizedState,r.componentWillUnmount()}catch(a){Pe(n,e,a)}lr(t,e,n);break;case 21:lr(t,e,n);break;case 22:n.mode&1?(dt=(r=dt)||n.memoizedState!==null,lr(t,e,n),dt=r):lr(t,e,n);break;default:lr(t,e,n)}}function uv(t){var e=t.updateQueue;if(e!==null){t.updateQueue=null;var n=t.stateNode;n===null&&(n=t.stateNode=new jS),e.forEach(function(r){var i=GS.bind(null,t,r);n.has(r)||(n.add(r),r.then(i,i))})}}function Xt(t,e){var n=e.deletions;if(n!==null)for(var r=0;r<n.length;r++){var i=n[r];try{var s=t,o=e,a=o;e:for(;a!==null;){switch(a.tag){case 5:tt=a.stateNode,Zt=!1;break e;case 3:tt=a.stateNode.containerInfo,Zt=!0;break e;case 4:tt=a.stateNode.containerInfo,Zt=!0;break e}a=a.return}if(tt===null)throw Error(x(160));y_(s,o,i),tt=null,Zt=!1;var l=i.alternate;l!==null&&(l.return=null),i.return=null}catch(u){Pe(i,e,u)}}if(e.subtreeFlags&12854)for(e=e.child;e!==null;)__(e,t),e=e.sibling}function __(t,e){var n=t.alternate,r=t.flags;switch(t.tag){case 0:case 11:case 14:case 15:if(Xt(e,t),pn(t),r&4){try{Eo(3,t,t.return),hc(3,t)}catch(w){Pe(t,t.return,w)}try{Eo(5,t,t.return)}catch(w){Pe(t,t.return,w)}}break;case 1:Xt(e,t),pn(t),r&512&&n!==null&&qi(n,n.return);break;case 5:if(Xt(e,t),pn(t),r&512&&n!==null&&qi(n,n.return),t.flags&32){var i=t.stateNode;try{Do(i,"")}catch(w){Pe(t,t.return,w)}}if(r&4&&(i=t.stateNode,i!=null)){var s=t.memoizedProps,o=n!==null?n.memoizedProps:s,a=t.type,l=t.updateQueue;if(t.updateQueue=null,l!==null)try{a==="input"&&s.type==="radio"&&s.name!=null&&U0(i,s),vh(a,o);var u=vh(a,s);for(o=0;o<l.length;o+=2){var c=l[o],h=l[o+1];c==="style"?W0(i,h):c==="dangerouslySetInnerHTML"?B0(i,h):c==="children"?Do(i,h):Yd(i,c,h,u)}switch(a){case"input":hh(i,s);break;case"textarea":$0(i,s);break;case"select":var d=i._wrapperState.wasMultiple;i._wrapperState.wasMultiple=!!s.multiple;var y=s.value;y!=null?rs(i,!!s.multiple,y,!1):d!==!!s.multiple&&(s.defaultValue!=null?rs(i,!!s.multiple,s.defaultValue,!0):rs(i,!!s.multiple,s.multiple?[]:"",!1))}i[Bo]=s}catch(w){Pe(t,t.return,w)}}break;case 6:if(Xt(e,t),pn(t),r&4){if(t.stateNode===null)throw Error(x(162));i=t.stateNode,s=t.memoizedProps;try{i.nodeValue=s}catch(w){Pe(t,t.return,w)}}break;case 3:if(Xt(e,t),pn(t),r&4&&n!==null&&n.memoizedState.isDehydrated)try{Vo(e.containerInfo)}catch(w){Pe(t,t.return,w)}break;case 4:Xt(e,t),pn(t);break;case 13:Xt(e,t),pn(t),i=t.child,i.flags&8192&&(s=i.memoizedState!==null,i.stateNode.isHidden=s,!s||i.alternate!==null&&i.alternate.memoizedState!==null||(Rp=be())),r&4&&uv(t);break;case 22:if(c=n!==null&&n.memoizedState!==null,t.mode&1?(dt=(u=dt)||c,Xt(e,t),dt=u):Xt(e,t),pn(t),r&8192){if(u=t.memoizedState!==null,(t.stateNode.isHidden=u)&&!c&&t.mode&1)for(L=t,c=t.child;c!==null;){for(h=L=c;L!==null;){switch(d=L,y=d.child,d.tag){case 0:case 11:case 14:case 15:Eo(4,d,d.return);break;case 1:qi(d,d.return);var _=d.stateNode;if(typeof _.componentWillUnmount=="function"){r=d,n=d.return;try{e=r,_.props=e.memoizedProps,_.state=e.memoizedState,_.componentWillUnmount()}catch(w){Pe(r,n,w)}}break;case 5:qi(d,d.return);break;case 22:if(d.memoizedState!==null){fv(h);continue}}y!==null?(y.return=d,L=y):fv(h)}c=c.sibling}e:for(c=null,h=t;;){if(h.tag===5){if(c===null){c=h;try{i=h.stateNode,u?(s=i.style,typeof s.setProperty=="function"?s.setProperty("display","none","important"):s.display="none"):(a=h.stateNode,l=h.memoizedProps.style,o=l!=null&&l.hasOwnProperty("display")?l.display:null,a.style.display=H0("display",o))}catch(w){Pe(t,t.return,w)}}}else if(h.tag===6){if(c===null)try{h.stateNode.nodeValue=u?"":h.memoizedProps}catch(w){Pe(t,t.return,w)}}else if((h.tag!==22&&h.tag!==23||h.memoizedState===null||h===t)&&h.child!==null){h.child.return=h,h=h.child;continue}if(h===t)break e;for(;h.sibling===null;){if(h.return===null||h.return===t)break e;c===h&&(c=null),h=h.return}c===h&&(c=null),h.sibling.return=h.return,h=h.sibling}}break;case 19:Xt(e,t),pn(t),r&4&&uv(t);break;case 21:break;default:Xt(e,t),pn(t)}}function pn(t){var e=t.flags;if(e&2){try{e:{for(var n=t.return;n!==null;){if(v_(n)){var r=n;break e}n=n.return}throw Error(x(160))}switch(r.tag){case 5:var i=r.stateNode;r.flags&32&&(Do(i,""),r.flags&=-33);var s=lv(t);Gh(t,s,i);break;case 3:case 4:var o=r.stateNode.containerInfo,a=lv(t);Kh(t,a,o);break;default:throw Error(x(161))}}catch(l){Pe(t,t.return,l)}t.flags&=-3}e&4096&&(t.flags&=-4097)}function FS(t,e,n){L=t,w_(t)}function w_(t,e,n){for(var r=(t.mode&1)!==0;L!==null;){var i=L,s=i.child;if(i.tag===22&&r){var o=i.memoizedState!==null||pl;if(!o){var a=i.alternate,l=a!==null&&a.memoizedState!==null||dt;a=pl;var u=dt;if(pl=o,(dt=l)&&!u)for(L=i;L!==null;)o=L,l=o.child,o.tag===22&&o.memoizedState!==null?hv(i):l!==null?(l.return=o,L=l):hv(i);for(;s!==null;)L=s,w_(s),s=s.sibling;L=i,pl=a,dt=u}cv(t)}else i.subtreeFlags&8772&&s!==null?(s.return=i,L=s):cv(t)}}function cv(t){for(;L!==null;){var e=L;if(e.flags&8772){var n=e.alternate;try{if(e.flags&8772)switch(e.tag){case 0:case 11:case 15:dt||hc(5,e);break;case 1:var r=e.stateNode;if(e.flags&4&&!dt)if(n===null)r.componentDidMount();else{var i=e.elementType===e.type?n.memoizedProps:Jt(e.type,n.memoizedProps);r.componentDidUpdate(i,n.memoizedState,r.__reactInternalSnapshotBeforeUpdate)}var s=e.updateQueue;s!==null&&Gg(e,s,r);break;case 3:var o=e.updateQueue;if(o!==null){if(n=null,e.child!==null)switch(e.child.tag){case 5:n=e.child.stateNode;break;case 1:n=e.child.stateNode}Gg(e,o,n)}break;case 5:var a=e.stateNode;if(n===null&&e.flags&4){n=a;var l=e.memoizedProps;switch(e.type){case"button":case"input":case"select":case"textarea":l.autoFocus&&n.focus();break;case"img":l.src&&(n.src=l.src)}}break;case 6:break;case 4:break;case 12:break;case 13:if(e.memoizedState===null){var u=e.alternate;if(u!==null){var c=u.memoizedState;if(c!==null){var h=c.dehydrated;h!==null&&Vo(h)}}}break;case 19:case 17:case 21:case 22:case 23:case 25:break;default:throw Error(x(163))}dt||e.flags&512&&Wh(e)}catch(d){Pe(e,e.return,d)}}if(e===t){L=null;break}if(n=e.sibling,n!==null){n.return=e.return,L=n;break}L=e.return}}function fv(t){for(;L!==null;){var e=L;if(e===t){L=null;break}var n=e.sibling;if(n!==null){n.return=e.return,L=n;break}L=e.return}}function hv(t){for(;L!==null;){var e=L;try{switch(e.tag){case 0:case 11:case 15:var n=e.return;try{hc(4,e)}catch(l){Pe(e,n,l)}break;case 1:var r=e.stateNode;if(typeof r.componentDidMount=="function"){var i=e.return;try{r.componentDidMount()}catch(l){Pe(e,i,l)}}var s=e.return;try{Wh(e)}catch(l){Pe(e,s,l)}break;case 5:var o=e.return;try{Wh(e)}catch(l){Pe(e,o,l)}}}catch(l){Pe(e,e.return,l)}if(e===t){L=null;break}var a=e.sibling;if(a!==null){a.return=e.return,L=a;break}L=e.return}}var US=Math.ceil,Tu=Zn.ReactCurrentDispatcher,xp=Zn.ReactCurrentOwner,Gt=Zn.ReactCurrentBatchConfig,ie=0,Xe=null,je=null,st=0,Dt=0,Qi=zr(0),$e=0,Qo=null,mi=0,dc=0,Pp=0,To=null,xt=null,Rp=0,Ts=1/0,Nn=null,Iu=!1,qh=null,xr=null,ml=!1,wr=null,Su=0,Io=0,Qh=null,Kl=-1,Gl=0;function Tt(){return ie&6?be():Kl!==-1?Kl:Kl=be()}function Pr(t){return t.mode&1?ie&2&&st!==0?st&-st:IS.transition!==null?(Gl===0&&(Gl=r1()),Gl):(t=oe,t!==0||(t=window.event,t=t===void 0?16:c1(t.type)),t):1}function un(t,e,n,r){if(50<Io)throw Io=0,Qh=null,Error(x(185));Ta(t,n,r),(!(ie&2)||t!==Xe)&&(t===Xe&&(!(ie&2)&&(dc|=n),$e===4&&pr(t,st)),Nt(t,r),n===1&&ie===0&&!(e.mode&1)&&(Ts=be()+500,uc&&Br()))}function Nt(t,e){var n=t.callbackNode;I2(t,e);var r=au(t,t===Xe?st:0);if(r===0)n!==null&&Eg(n),t.callbackNode=null,t.callbackPriority=0;else if(e=r&-r,t.callbackPriority!==e){if(n!=null&&Eg(n),e===1)t.tag===0?TS(dv.bind(null,t)):R1(dv.bind(null,t)),yS(function(){!(ie&6)&&Br()}),n=null;else{switch(i1(r)){case 1:n=tp;break;case 4:n=t1;break;case 16:n=ou;break;case 536870912:n=n1;break;default:n=ou}n=P_(n,E_.bind(null,t))}t.callbackPriority=e,t.callbackNode=n}}function E_(t,e){if(Kl=-1,Gl=0,ie&6)throw Error(x(327));var n=t.callbackNode;if(ls()&&t.callbackNode!==n)return null;var r=au(t,t===Xe?st:0);if(r===0)return null;if(r&30||r&t.expiredLanes||e)e=ku(t,r);else{e=r;var i=ie;ie|=2;var s=I_();(Xe!==t||st!==e)&&(Nn=null,Ts=be()+500,ai(t,e));do try{BS();break}catch(a){T_(t,a)}while(1);pp(),Tu.current=s,ie=i,je!==null?e=0:(Xe=null,st=0,e=$e)}if(e!==0){if(e===2&&(i=Th(t),i!==0&&(r=i,e=Yh(t,i))),e===1)throw n=Qo,ai(t,0),pr(t,r),Nt(t,be()),n;if(e===6)pr(t,r);else{if(i=t.current.alternate,!(r&30)&&!$S(i)&&(e=ku(t,r),e===2&&(s=Th(t),s!==0&&(r=s,e=Yh(t,s))),e===1))throw n=Qo,ai(t,0),pr(t,r),Nt(t,be()),n;switch(t.finishedWork=i,t.finishedLanes=r,e){case 0:case 1:throw Error(x(345));case 2:Yr(t,xt,Nn);break;case 3:if(pr(t,r),(r&130023424)===r&&(e=Rp+500-be(),10<e)){if(au(t,0)!==0)break;if(i=t.suspendedLanes,(i&r)!==r){Tt(),t.pingedLanes|=t.suspendedLanes&i;break}t.timeoutHandle=Ch(Yr.bind(null,t,xt,Nn),e);break}Yr(t,xt,Nn);break;case 4:if(pr(t,r),(r&4194240)===r)break;for(e=t.eventTimes,i=-1;0<r;){var o=31-ln(r);s=1<<o,o=e[o],o>i&&(i=o),r&=~s}if(r=i,r=be()-r,r=(120>r?120:480>r?480:1080>r?1080:1920>r?1920:3e3>r?3e3:4320>r?4320:1960*US(r/1960))-r,10<r){t.timeoutHandle=Ch(Yr.bind(null,t,xt,Nn),r);break}Yr(t,xt,Nn);break;case 5:Yr(t,xt,Nn);break;default:throw Error(x(329))}}}return Nt(t,be()),t.callbackNode===n?E_.bind(null,t):null}function Yh(t,e){var n=To;return t.current.memoizedState.isDehydrated&&(ai(t,e).flags|=256),t=ku(t,e),t!==2&&(e=xt,xt=n,e!==null&&Xh(e)),t}function Xh(t){xt===null?xt=t:xt.push.apply(xt,t)}function $S(t){for(var e=t;;){if(e.flags&16384){var n=e.updateQueue;if(n!==null&&(n=n.stores,n!==null))for(var r=0;r<n.length;r++){var i=n[r],s=i.getSnapshot;i=i.value;try{if(!cn(s(),i))return!1}catch{return!1}}}if(n=e.child,e.subtreeFlags&16384&&n!==null)n.return=e,e=n;else{if(e===t)break;for(;e.sibling===null;){if(e.return===null||e.return===t)return!0;e=e.return}e.sibling.return=e.return,e=e.sibling}}return!0}function pr(t,e){for(e&=~Pp,e&=~dc,t.suspendedLanes|=e,t.pingedLanes&=~e,t=t.expirationTimes;0<e;){var n=31-ln(e),r=1<<n;t[n]=-1,e&=~r}}function dv(t){if(ie&6)throw Error(x(327));ls();var e=au(t,0);if(!(e&1))return Nt(t,be()),null;var n=ku(t,e);if(t.tag!==0&&n===2){var r=Th(t);r!==0&&(e=r,n=Yh(t,r))}if(n===1)throw n=Qo,ai(t,0),pr(t,e),Nt(t,be()),n;if(n===6)throw Error(x(345));return t.finishedWork=t.current.alternate,t.finishedLanes=e,Yr(t,xt,Nn),Nt(t,be()),null}function Cp(t,e){var n=ie;ie|=1;try{return t(e)}finally{ie=n,ie===0&&(Ts=be()+500,uc&&Br())}}function gi(t){wr!==null&&wr.tag===0&&!(ie&6)&&ls();var e=ie;ie|=1;var n=Gt.transition,r=oe;try{if(Gt.transition=null,oe=1,t)return t()}finally{oe=r,Gt.transition=n,ie=e,!(ie&6)&&Br()}}function Np(){Dt=Qi.current,ye(Qi)}function ai(t,e){t.finishedWork=null,t.finishedLanes=0;var n=t.timeoutHandle;if(n!==-1&&(t.timeoutHandle=-1,vS(n)),je!==null)for(n=je.return;n!==null;){var r=n;switch(fp(r),r.tag){case 1:r=r.type.childContextTypes,r!=null&&hu();break;case 3:ws(),ye(Rt),ye(yt),wp();break;case 5:_p(r);break;case 4:ws();break;case 13:ye(ke);break;case 19:ye(ke);break;case 10:mp(r.type._context);break;case 22:case 23:Np()}n=n.return}if(Xe=t,je=t=Rr(t.current,null),st=Dt=e,$e=0,Qo=null,Pp=dc=mi=0,xt=To=null,Zr!==null){for(e=0;e<Zr.length;e++)if(n=Zr[e],r=n.interleaved,r!==null){n.interleaved=null;var i=r.next,s=n.pending;if(s!==null){var o=s.next;s.next=i,r.next=o}n.pending=r}Zr=null}return t}function T_(t,e){do{var n=je;try{if(pp(),Bl.current=Eu,wu){for(var r=Ae.memoizedState;r!==null;){var i=r.queue;i!==null&&(i.pending=null),r=r.next}wu=!1}if(pi=0,qe=Ue=Ae=null,wo=!1,Ko=0,xp.current=null,n===null||n.return===null){$e=1,Qo=e,je=null;break}e:{var s=t,o=n.return,a=n,l=e;if(e=st,a.flags|=32768,l!==null&&typeof l=="object"&&typeof l.then=="function"){var u=l,c=a,h=c.tag;if(!(c.mode&1)&&(h===0||h===11||h===15)){var d=c.alternate;d?(c.updateQueue=d.updateQueue,c.memoizedState=d.memoizedState,c.lanes=d.lanes):(c.updateQueue=null,c.memoizedState=null)}var y=ev(o);if(y!==null){y.flags&=-257,tv(y,o,a,s,e),y.mode&1&&Zg(s,u,e),e=y,l=u;var _=e.updateQueue;if(_===null){var w=new Set;w.add(l),e.updateQueue=w}else _.add(l);break e}else{if(!(e&1)){Zg(s,u,e),bp();break e}l=Error(x(426))}}else if(Te&&a.mode&1){var T=ev(o);if(T!==null){!(T.flags&65536)&&(T.flags|=256),tv(T,o,a,s,e),hp(Es(l,a));break e}}s=l=Es(l,a),$e!==4&&($e=2),To===null?To=[s]:To.push(s),s=o;do{switch(s.tag){case 3:s.flags|=65536,e&=-e,s.lanes|=e;var g=s_(s,l,e);Kg(s,g);break e;case 1:a=l;var p=s.type,v=s.stateNode;if(!(s.flags&128)&&(typeof p.getDerivedStateFromError=="function"||v!==null&&typeof v.componentDidCatch=="function"&&(xr===null||!xr.has(v)))){s.flags|=65536,e&=-e,s.lanes|=e;var m=o_(s,a,e);Kg(s,m);break e}}s=s.return}while(s!==null)}k_(n)}catch(S){e=S,je===n&&n!==null&&(je=n=n.return);continue}break}while(1)}function I_(){var t=Tu.current;return Tu.current=Eu,t===null?Eu:t}function bp(){($e===0||$e===3||$e===2)&&($e=4),Xe===null||!(mi&268435455)&&!(dc&268435455)||pr(Xe,st)}function ku(t,e){var n=ie;ie|=2;var r=I_();(Xe!==t||st!==e)&&(Nn=null,ai(t,e));do try{zS();break}catch(i){T_(t,i)}while(1);if(pp(),ie=n,Tu.current=r,je!==null)throw Error(x(261));return Xe=null,st=0,$e}function zS(){for(;je!==null;)S_(je)}function BS(){for(;je!==null&&!p2();)S_(je)}function S_(t){var e=x_(t.alternate,t,Dt);t.memoizedProps=t.pendingProps,e===null?k_(t):je=e,xp.current=null}function k_(t){var e=t;do{var n=e.alternate;if(t=e.return,e.flags&32768){if(n=LS(n,e),n!==null){n.flags&=32767,je=n;return}if(t!==null)t.flags|=32768,t.subtreeFlags=0,t.deletions=null;else{$e=6,je=null;return}}else if(n=MS(n,e,Dt),n!==null){je=n;return}if(e=e.sibling,e!==null){je=e;return}je=e=t}while(e!==null);$e===0&&($e=5)}function Yr(t,e,n){var r=oe,i=Gt.transition;try{Gt.transition=null,oe=1,HS(t,e,n,r)}finally{Gt.transition=i,oe=r}return null}function HS(t,e,n,r){do ls();while(wr!==null);if(ie&6)throw Error(x(327));n=t.finishedWork;var i=t.finishedLanes;if(n===null)return null;if(t.finishedWork=null,t.finishedLanes=0,n===t.current)throw Error(x(177));t.callbackNode=null,t.callbackPriority=0;var s=n.lanes|n.childLanes;if(S2(t,s),t===Xe&&(je=Xe=null,st=0),!(n.subtreeFlags&2064)&&!(n.flags&2064)||ml||(ml=!0,P_(ou,function(){return ls(),null})),s=(n.flags&15990)!==0,n.subtreeFlags&15990||s){s=Gt.transition,Gt.transition=null;var o=oe;oe=1;var a=ie;ie|=4,xp.current=null,VS(t,n),__(n,t),cS(Ph),lu=!!xh,Ph=xh=null,t.current=n,FS(n),m2(),ie=a,oe=o,Gt.transition=s}else t.current=n;if(ml&&(ml=!1,wr=t,Su=i),s=t.pendingLanes,s===0&&(xr=null),y2(n.stateNode),Nt(t,be()),e!==null)for(r=t.onRecoverableError,n=0;n<e.length;n++)i=e[n],r(i.value,{componentStack:i.stack,digest:i.digest});if(Iu)throw Iu=!1,t=qh,qh=null,t;return Su&1&&t.tag!==0&&ls(),s=t.pendingLanes,s&1?t===Qh?Io++:(Io=0,Qh=t):Io=0,Br(),null}function ls(){if(wr!==null){var t=i1(Su),e=Gt.transition,n=oe;try{if(Gt.transition=null,oe=16>t?16:t,wr===null)var r=!1;else{if(t=wr,wr=null,Su=0,ie&6)throw Error(x(331));var i=ie;for(ie|=4,L=t.current;L!==null;){var s=L,o=s.child;if(L.flags&16){var a=s.deletions;if(a!==null){for(var l=0;l<a.length;l++){var u=a[l];for(L=u;L!==null;){var c=L;switch(c.tag){case 0:case 11:case 15:Eo(8,c,s)}var h=c.child;if(h!==null)h.return=c,L=h;else for(;L!==null;){c=L;var d=c.sibling,y=c.return;if(g_(c),c===u){L=null;break}if(d!==null){d.return=y,L=d;break}L=y}}}var _=s.alternate;if(_!==null){var w=_.child;if(w!==null){_.child=null;do{var T=w.sibling;w.sibling=null,w=T}while(w!==null)}}L=s}}if(s.subtreeFlags&2064&&o!==null)o.return=s,L=o;else e:for(;L!==null;){if(s=L,s.flags&2048)switch(s.tag){case 0:case 11:case 15:Eo(9,s,s.return)}var g=s.sibling;if(g!==null){g.return=s.return,L=g;break e}L=s.return}}var p=t.current;for(L=p;L!==null;){o=L;var v=o.child;if(o.subtreeFlags&2064&&v!==null)v.return=o,L=v;else e:for(o=p;L!==null;){if(a=L,a.flags&2048)try{switch(a.tag){case 0:case 11:case 15:hc(9,a)}}catch(S){Pe(a,a.return,S)}if(a===o){L=null;break e}var m=a.sibling;if(m!==null){m.return=a.return,L=m;break e}L=a.return}}if(ie=i,Br(),En&&typeof En.onPostCommitFiberRoot=="function")try{En.onPostCommitFiberRoot(ic,t)}catch{}r=!0}return r}finally{oe=n,Gt.transition=e}}return!1}function pv(t,e,n){e=Es(n,e),e=s_(t,e,1),t=Ar(t,e,1),e=Tt(),t!==null&&(Ta(t,1,e),Nt(t,e))}function Pe(t,e,n){if(t.tag===3)pv(t,t,n);else for(;e!==null;){if(e.tag===3){pv(e,t,n);break}else if(e.tag===1){var r=e.stateNode;if(typeof e.type.getDerivedStateFromError=="function"||typeof r.componentDidCatch=="function"&&(xr===null||!xr.has(r))){t=Es(n,t),t=o_(e,t,1),e=Ar(e,t,1),t=Tt(),e!==null&&(Ta(e,1,t),Nt(e,t));break}}e=e.return}}function WS(t,e,n){var r=t.pingCache;r!==null&&r.delete(e),e=Tt(),t.pingedLanes|=t.suspendedLanes&n,Xe===t&&(st&n)===n&&($e===4||$e===3&&(st&130023424)===st&&500>be()-Rp?ai(t,0):Pp|=n),Nt(t,e)}function A_(t,e){e===0&&(t.mode&1?(e=sl,sl<<=1,!(sl&130023424)&&(sl=4194304)):e=1);var n=Tt();t=Hn(t,e),t!==null&&(Ta(t,e,n),Nt(t,n))}function KS(t){var e=t.memoizedState,n=0;e!==null&&(n=e.retryLane),A_(t,n)}function GS(t,e){var n=0;switch(t.tag){case 13:var r=t.stateNode,i=t.memoizedState;i!==null&&(n=i.retryLane);break;case 19:r=t.stateNode;break;default:throw Error(x(314))}r!==null&&r.delete(e),A_(t,n)}var x_;x_=function(t,e,n){if(t!==null)if(t.memoizedProps!==e.pendingProps||Rt.current)Pt=!0;else{if(!(t.lanes&n)&&!(e.flags&128))return Pt=!1,DS(t,e,n);Pt=!!(t.flags&131072)}else Pt=!1,Te&&e.flags&1048576&&C1(e,mu,e.index);switch(e.lanes=0,e.tag){case 2:var r=e.type;Wl(t,e),t=e.pendingProps;var i=vs(e,yt.current);as(e,n),i=Tp(null,e,r,t,i,n);var s=Ip();return e.flags|=1,typeof i=="object"&&i!==null&&typeof i.render=="function"&&i.$$typeof===void 0?(e.tag=1,e.memoizedState=null,e.updateQueue=null,Ct(r)?(s=!0,du(e)):s=!1,e.memoizedState=i.state!==null&&i.state!==void 0?i.state:null,vp(e),i.updater=cc,e.stateNode=i,i._reactInternals=e,jh(e,r,t,n),e=Uh(null,e,r,!0,s,n)):(e.tag=0,Te&&s&&cp(e),wt(null,e,i,n),e=e.child),e;case 16:r=e.elementType;e:{switch(Wl(t,e),t=e.pendingProps,i=r._init,r=i(r._payload),e.type=r,i=e.tag=QS(r),t=Jt(r,t),i){case 0:e=Fh(null,e,r,t,n);break e;case 1:e=iv(null,e,r,t,n);break e;case 11:e=nv(null,e,r,t,n);break e;case 14:e=rv(null,e,r,Jt(r.type,t),n);break e}throw Error(x(306,r,""))}return e;case 0:return r=e.type,i=e.pendingProps,i=e.elementType===r?i:Jt(r,i),Fh(t,e,r,i,n);case 1:return r=e.type,i=e.pendingProps,i=e.elementType===r?i:Jt(r,i),iv(t,e,r,i,n);case 3:e:{if(c_(e),t===null)throw Error(x(387));r=e.pendingProps,s=e.memoizedState,i=s.element,D1(t,e),yu(e,r,null,n);var o=e.memoizedState;if(r=o.element,s.isDehydrated)if(s={element:r,isDehydrated:!1,cache:o.cache,pendingSuspenseBoundaries:o.pendingSuspenseBoundaries,transitions:o.transitions},e.updateQueue.baseState=s,e.memoizedState=s,e.flags&256){i=Es(Error(x(423)),e),e=sv(t,e,r,n,i);break e}else if(r!==i){i=Es(Error(x(424)),e),e=sv(t,e,r,n,i);break e}else for(Mt=kr(e.stateNode.containerInfo.firstChild),Lt=e,Te=!0,nn=null,n=V1(e,null,r,n),e.child=n;n;)n.flags=n.flags&-3|4096,n=n.sibling;else{if(ys(),r===i){e=Wn(t,e,n);break e}wt(t,e,r,n)}e=e.child}return e;case 5:return F1(e),t===null&&Dh(e),r=e.type,i=e.pendingProps,s=t!==null?t.memoizedProps:null,o=i.children,Rh(r,i)?o=null:s!==null&&Rh(r,s)&&(e.flags|=32),u_(t,e),wt(t,e,o,n),e.child;case 6:return t===null&&Dh(e),null;case 13:return f_(t,e,n);case 4:return yp(e,e.stateNode.containerInfo),r=e.pendingProps,t===null?e.child=_s(e,null,r,n):wt(t,e,r,n),e.child;case 11:return r=e.type,i=e.pendingProps,i=e.elementType===r?i:Jt(r,i),nv(t,e,r,i,n);case 7:return wt(t,e,e.pendingProps,n),e.child;case 8:return wt(t,e,e.pendingProps.children,n),e.child;case 12:return wt(t,e,e.pendingProps.children,n),e.child;case 10:e:{if(r=e.type._context,i=e.pendingProps,s=e.memoizedProps,o=i.value,pe(gu,r._currentValue),r._currentValue=o,s!==null)if(cn(s.value,o)){if(s.children===i.children&&!Rt.current){e=Wn(t,e,n);break e}}else for(s=e.child,s!==null&&(s.return=e);s!==null;){var a=s.dependencies;if(a!==null){o=s.child;for(var l=a.firstContext;l!==null;){if(l.context===r){if(s.tag===1){l=Vn(-1,n&-n),l.tag=2;var u=s.updateQueue;if(u!==null){u=u.shared;var c=u.pending;c===null?l.next=l:(l.next=c.next,c.next=l),u.pending=l}}s.lanes|=n,l=s.alternate,l!==null&&(l.lanes|=n),Mh(s.return,n,e),a.lanes|=n;break}l=l.next}}else if(s.tag===10)o=s.type===e.type?null:s.child;else if(s.tag===18){if(o=s.return,o===null)throw Error(x(341));o.lanes|=n,a=o.alternate,a!==null&&(a.lanes|=n),Mh(o,n,e),o=s.sibling}else o=s.child;if(o!==null)o.return=s;else for(o=s;o!==null;){if(o===e){o=null;break}if(s=o.sibling,s!==null){s.return=o.return,o=s;break}o=o.return}s=o}wt(t,e,i.children,n),e=e.child}return e;case 9:return i=e.type,r=e.pendingProps.children,as(e,n),i=qt(i),r=r(i),e.flags|=1,wt(t,e,r,n),e.child;case 14:return r=e.type,i=Jt(r,e.pendingProps),i=Jt(r.type,i),rv(t,e,r,i,n);case 15:return a_(t,e,e.type,e.pendingProps,n);case 17:return r=e.type,i=e.pendingProps,i=e.elementType===r?i:Jt(r,i),Wl(t,e),e.tag=1,Ct(r)?(t=!0,du(e)):t=!1,as(e,n),L1(e,r,i),jh(e,r,i,n),Uh(null,e,r,!0,t,n);case 19:return h_(t,e,n);case 22:return l_(t,e,n)}throw Error(x(156,e.tag))};function P_(t,e){return e1(t,e)}function qS(t,e,n,r){this.tag=t,this.key=n,this.sibling=this.child=this.return=this.stateNode=this.type=this.elementType=null,this.index=0,this.ref=null,this.pendingProps=e,this.dependencies=this.memoizedState=this.updateQueue=this.memoizedProps=null,this.mode=r,this.subtreeFlags=this.flags=0,this.deletions=null,this.childLanes=this.lanes=0,this.alternate=null}function Kt(t,e,n,r){return new qS(t,e,n,r)}function Op(t){return t=t.prototype,!(!t||!t.isReactComponent)}function QS(t){if(typeof t=="function")return Op(t)?1:0;if(t!=null){if(t=t.$$typeof,t===Jd)return 11;if(t===Zd)return 14}return 2}function Rr(t,e){var n=t.alternate;return n===null?(n=Kt(t.tag,e,t.key,t.mode),n.elementType=t.elementType,n.type=t.type,n.stateNode=t.stateNode,n.alternate=t,t.alternate=n):(n.pendingProps=e,n.type=t.type,n.flags=0,n.subtreeFlags=0,n.deletions=null),n.flags=t.flags&14680064,n.childLanes=t.childLanes,n.lanes=t.lanes,n.child=t.child,n.memoizedProps=t.memoizedProps,n.memoizedState=t.memoizedState,n.updateQueue=t.updateQueue,e=t.dependencies,n.dependencies=e===null?null:{lanes:e.lanes,firstContext:e.firstContext},n.sibling=t.sibling,n.index=t.index,n.ref=t.ref,n}function ql(t,e,n,r,i,s){var o=2;if(r=t,typeof t=="function")Op(t)&&(o=1);else if(typeof t=="string")o=5;else e:switch(t){case Fi:return li(n.children,i,s,e);case Xd:o=8,i|=8;break;case ah:return t=Kt(12,n,e,i|2),t.elementType=ah,t.lanes=s,t;case lh:return t=Kt(13,n,e,i),t.elementType=lh,t.lanes=s,t;case uh:return t=Kt(19,n,e,i),t.elementType=uh,t.lanes=s,t;case j0:return pc(n,i,s,e);default:if(typeof t=="object"&&t!==null)switch(t.$$typeof){case M0:o=10;break e;case L0:o=9;break e;case Jd:o=11;break e;case Zd:o=14;break e;case fr:o=16,r=null;break e}throw Error(x(130,t==null?t:typeof t,""))}return e=Kt(o,n,e,i),e.elementType=t,e.type=r,e.lanes=s,e}function li(t,e,n,r){return t=Kt(7,t,r,e),t.lanes=n,t}function pc(t,e,n,r){return t=Kt(22,t,r,e),t.elementType=j0,t.lanes=n,t.stateNode={isHidden:!1},t}function Nf(t,e,n){return t=Kt(6,t,null,e),t.lanes=n,t}function bf(t,e,n){return e=Kt(4,t.children!==null?t.children:[],t.key,e),e.lanes=n,e.stateNode={containerInfo:t.containerInfo,pendingChildren:null,implementation:t.implementation},e}function YS(t,e,n,r,i){this.tag=e,this.containerInfo=t,this.finishedWork=this.pingCache=this.current=this.pendingChildren=null,this.timeoutHandle=-1,this.callbackNode=this.pendingContext=this.context=null,this.callbackPriority=0,this.eventTimes=hf(0),this.expirationTimes=hf(-1),this.entangledLanes=this.finishedLanes=this.mutableReadLanes=this.expiredLanes=this.pingedLanes=this.suspendedLanes=this.pendingLanes=0,this.entanglements=hf(0),this.identifierPrefix=r,this.onRecoverableError=i,this.mutableSourceEagerHydrationData=null}function Dp(t,e,n,r,i,s,o,a,l){return t=new YS(t,e,n,a,l),e===1?(e=1,s===!0&&(e|=8)):e=0,s=Kt(3,null,null,e),t.current=s,s.stateNode=t,s.memoizedState={element:r,isDehydrated:n,cache:null,transitions:null,pendingSuspenseBoundaries:null},vp(s),t}function XS(t,e,n){var r=3<arguments.length&&arguments[3]!==void 0?arguments[3]:null;return{$$typeof:Vi,key:r==null?null:""+r,children:t,containerInfo:e,implementation:n}}function R_(t){if(!t)return Dr;t=t._reactInternals;e:{if(Pi(t)!==t||t.tag!==1)throw Error(x(170));var e=t;do{switch(e.tag){case 3:e=e.stateNode.context;break e;case 1:if(Ct(e.type)){e=e.stateNode.__reactInternalMemoizedMergedChildContext;break e}}e=e.return}while(e!==null);throw Error(x(171))}if(t.tag===1){var n=t.type;if(Ct(n))return P1(t,n,e)}return e}function C_(t,e,n,r,i,s,o,a,l){return t=Dp(n,r,!0,t,i,s,o,a,l),t.context=R_(null),n=t.current,r=Tt(),i=Pr(n),s=Vn(r,i),s.callback=e??null,Ar(n,s,i),t.current.lanes=i,Ta(t,i,r),Nt(t,r),t}function mc(t,e,n,r){var i=e.current,s=Tt(),o=Pr(i);return n=R_(n),e.context===null?e.context=n:e.pendingContext=n,e=Vn(s,o),e.payload={element:t},r=r===void 0?null:r,r!==null&&(e.callback=r),t=Ar(i,e,o),t!==null&&(un(t,i,o,s),zl(t,i,o)),o}function Au(t){if(t=t.current,!t.child)return null;switch(t.child.tag){case 5:return t.child.stateNode;default:return t.child.stateNode}}function mv(t,e){if(t=t.memoizedState,t!==null&&t.dehydrated!==null){var n=t.retryLane;t.retryLane=n!==0&&n<e?n:e}}function Mp(t,e){mv(t,e),(t=t.alternate)&&mv(t,e)}function JS(){return null}var N_=typeof reportError=="function"?reportError:function(t){console.error(t)};function Lp(t){this._internalRoot=t}gc.prototype.render=Lp.prototype.render=function(t){var e=this._internalRoot;if(e===null)throw Error(x(409));mc(t,e,null,null)};gc.prototype.unmount=Lp.prototype.unmount=function(){var t=this._internalRoot;if(t!==null){this._internalRoot=null;var e=t.containerInfo;gi(function(){mc(null,t,null,null)}),e[Bn]=null}};function gc(t){this._internalRoot=t}gc.prototype.unstable_scheduleHydration=function(t){if(t){var e=a1();t={blockedOn:null,target:t,priority:e};for(var n=0;n<dr.length&&e!==0&&e<dr[n].priority;n++);dr.splice(n,0,t),n===0&&u1(t)}};function jp(t){return!(!t||t.nodeType!==1&&t.nodeType!==9&&t.nodeType!==11)}function vc(t){return!(!t||t.nodeType!==1&&t.nodeType!==9&&t.nodeType!==11&&(t.nodeType!==8||t.nodeValue!==" react-mount-point-unstable "))}function gv(){}function ZS(t,e,n,r,i){if(i){if(typeof r=="function"){var s=r;r=function(){var u=Au(o);s.call(u)}}var o=C_(e,r,t,0,null,!1,!1,"",gv);return t._reactRootContainer=o,t[Bn]=o.current,$o(t.nodeType===8?t.parentNode:t),gi(),o}for(;i=t.lastChild;)t.removeChild(i);if(typeof r=="function"){var a=r;r=function(){var u=Au(l);a.call(u)}}var l=Dp(t,0,!1,null,null,!1,!1,"",gv);return t._reactRootContainer=l,t[Bn]=l.current,$o(t.nodeType===8?t.parentNode:t),gi(function(){mc(e,l,n,r)}),l}function yc(t,e,n,r,i){var s=n._reactRootContainer;if(s){var o=s;if(typeof i=="function"){var a=i;i=function(){var l=Au(o);a.call(l)}}mc(e,o,t,i)}else o=ZS(n,e,t,i,r);return Au(o)}s1=function(t){switch(t.tag){case 3:var e=t.stateNode;if(e.current.memoizedState.isDehydrated){var n=uo(e.pendingLanes);n!==0&&(np(e,n|1),Nt(e,be()),!(ie&6)&&(Ts=be()+500,Br()))}break;case 13:gi(function(){var r=Hn(t,1);if(r!==null){var i=Tt();un(r,t,1,i)}}),Mp(t,1)}};rp=function(t){if(t.tag===13){var e=Hn(t,134217728);if(e!==null){var n=Tt();un(e,t,134217728,n)}Mp(t,134217728)}};o1=function(t){if(t.tag===13){var e=Pr(t),n=Hn(t,e);if(n!==null){var r=Tt();un(n,t,e,r)}Mp(t,e)}};a1=function(){return oe};l1=function(t,e){var n=oe;try{return oe=t,e()}finally{oe=n}};_h=function(t,e,n){switch(e){case"input":if(hh(t,n),e=n.name,n.type==="radio"&&e!=null){for(n=t;n.parentNode;)n=n.parentNode;for(n=n.querySelectorAll("input[name="+JSON.stringify(""+e)+'][type="radio"]'),e=0;e<n.length;e++){var r=n[e];if(r!==t&&r.form===t.form){var i=lc(r);if(!i)throw Error(x(90));F0(r),hh(r,i)}}}break;case"textarea":$0(t,n);break;case"select":e=n.value,e!=null&&rs(t,!!n.multiple,e,!1)}};q0=Cp;Q0=gi;var ek={usingClientEntryPoint:!1,Events:[Sa,Bi,lc,K0,G0,Cp]},eo={findFiberByHostInstance:Jr,bundleType:0,version:"18.2.0",rendererPackageName:"react-dom"},tk={bundleType:eo.bundleType,version:eo.version,rendererPackageName:eo.rendererPackageName,rendererConfig:eo.rendererConfig,overrideHookState:null,overrideHookStateDeletePath:null,overrideHookStateRenamePath:null,overrideProps:null,overridePropsDeletePath:null,overridePropsRenamePath:null,setErrorHandler:null,setSuspenseHandler:null,scheduleUpdate:null,currentDispatcherRef:Zn.ReactCurrentDispatcher,findHostInstanceByFiber:function(t){return t=J0(t),t===null?null:t.stateNode},findFiberByHostInstance:eo.findFiberByHostInstance||JS,findHostInstancesForRefresh:null,scheduleRefresh:null,scheduleRoot:null,setRefreshHandler:null,getCurrentFiber:null,reconcilerVersion:"18.2.0-next-9e3b772b8-20220608"};if(typeof __REACT_DEVTOOLS_GLOBAL_HOOK__<"u"){var gl=__REACT_DEVTOOLS_GLOBAL_HOOK__;if(!gl.isDisabled&&gl.supportsFiber)try{ic=gl.inject(tk),En=gl}catch{}}Ft.__SECRET_INTERNALS_DO_NOT_USE_OR_YOU_WILL_BE_FIRED=ek;Ft.createPortal=function(t,e){var n=2<arguments.length&&arguments[2]!==void 0?arguments[2]:null;if(!jp(e))throw Error(x(200));return XS(t,e,null,n)};Ft.createRoot=function(t,e){if(!jp(t))throw Error(x(299));var n=!1,r="",i=N_;return e!=null&&(e.unstable_strictMode===!0&&(n=!0),e.identifierPrefix!==void 0&&(r=e.identifierPrefix),e.onRecoverableError!==void 0&&(i=e.onRecoverableError)),e=Dp(t,1,!1,null,null,n,!1,r,i),t[Bn]=e.current,$o(t.nodeType===8?t.parentNode:t),new Lp(e)};Ft.findDOMNode=function(t){if(t==null)return null;if(t.nodeType===1)return t;var e=t._reactInternals;if(e===void 0)throw typeof t.render=="function"?Error(x(188)):(t=Object.keys(t).join(","),Error(x(268,t)));return t=J0(e),t=t===null?null:t.stateNode,t};Ft.flushSync=function(t){return gi(t)};Ft.hydrate=function(t,e,n){if(!vc(e))throw Error(x(200));return yc(null,t,e,!0,n)};Ft.hydrateRoot=function(t,e,n){if(!jp(t))throw Error(x(405));var r=n!=null&&n.hydratedSources||null,i=!1,s="",o=N_;if(n!=null&&(n.unstable_strictMode===!0&&(i=!0),n.identifierPrefix!==void 0&&(s=n.identifierPrefix),n.onRecoverableError!==void 0&&(o=n.onRecoverableError)),e=C_(e,null,t,1,n??null,i,!1,s,o),t[Bn]=e.current,$o(t),r)for(t=0;t<r.length;t++)n=r[t],i=n._getVersion,i=i(n._source),e.mutableSourceEagerHydrationData==null?e.mutableSourceEagerHydrationData=[n,i]:e.mutableSourceEagerHydrationData.push(n,i);return new gc(e)};Ft.render=function(t,e,n){if(!vc(e))throw Error(x(200));return yc(null,t,e,!1,n)};Ft.unmountComponentAtNode=function(t){if(!vc(t))throw Error(x(40));return t._reactRootContainer?(gi(function(){yc(null,null,t,!1,function(){t._reactRootContainer=null,t[Bn]=null})}),!0):!1};Ft.unstable_batchedUpdates=Cp;Ft.unstable_renderSubtreeIntoContainer=function(t,e,n,r){if(!vc(n))throw Error(x(200));if(t==null||t._reactInternals===void 0)throw Error(x(38));return yc(t,e,n,!1,r)};Ft.version="18.2.0-next-9e3b772b8-20220608";function b_(){if(!(typeof __REACT_DEVTOOLS_GLOBAL_HOOK__>"u"||typeof __REACT_DEVTOOLS_GLOBAL_HOOK__.checkDCE!="function"))try{__REACT_DEVTOOLS_GLOBAL_HOOK__.checkDCE(b_)}catch(t){console.error(t)}}b_(),C0.exports=Ft;var nk=C0.exports,vv=nk;sh.createRoot=vv.createRoot,sh.hydrateRoot=vv.hydrateRoot;/**
 * @remix-run/router v1.7.2
 *
 * Copyright (c) Remix Software Inc.
 *
 * This source code is licensed under the MIT license found in the
 * LICENSE.md file in the root directory of this source tree.
 *
 * @license MIT
 */function Ee(){return Ee=Object.assign?Object.assign.bind():function(t){for(var e=1;e<arguments.length;e++){var n=arguments[e];for(var r in n)Object.prototype.hasOwnProperty.call(n,r)&&(t[r]=n[r])}return t},Ee.apply(this,arguments)}var Me;(function(t){t.Pop="POP",t.Push="PUSH",t.Replace="REPLACE"})(Me||(Me={}));const yv="popstate";function rk(t){t===void 0&&(t={});function e(r,i){let{pathname:s,search:o,hash:a}=r.location;return Yo("",{pathname:s,search:o,hash:a},i.state&&i.state.usr||null,i.state&&i.state.key||"default")}function n(r,i){return typeof i=="string"?i:vi(i)}return sk(e,n,null,t)}function J(t,e){if(t===!1||t===null||typeof t>"u")throw new Error(e)}function Is(t,e){if(!t){typeof console<"u"&&console.warn(e);try{throw new Error(e)}catch{}}}function ik(){return Math.random().toString(36).substr(2,8)}function _v(t,e){return{usr:t.state,key:t.key,idx:e}}function Yo(t,e,n,r){return n===void 0&&(n=null),Ee({pathname:typeof t=="string"?t:t.pathname,search:"",hash:""},typeof e=="string"?er(e):e,{state:n,key:e&&e.key||r||ik()})}function vi(t){let{pathname:e="/",search:n="",hash:r=""}=t;return n&&n!=="?"&&(e+=n.charAt(0)==="?"?n:"?"+n),r&&r!=="#"&&(e+=r.charAt(0)==="#"?r:"#"+r),e}function er(t){let e={};if(t){let n=t.indexOf("#");n>=0&&(e.hash=t.substr(n),t=t.substr(0,n));let r=t.indexOf("?");r>=0&&(e.search=t.substr(r),t=t.substr(0,r)),t&&(e.pathname=t)}return e}function sk(t,e,n,r){r===void 0&&(r={});let{window:i=document.defaultView,v5Compat:s=!1}=r,o=i.history,a=Me.Pop,l=null,u=c();u==null&&(u=0,o.replaceState(Ee({},o.state,{idx:u}),""));function c(){return(o.state||{idx:null}).idx}function h(){a=Me.Pop;let T=c(),g=T==null?null:T-u;u=T,l&&l({action:a,location:w.location,delta:g})}function d(T,g){a=Me.Push;let p=Yo(w.location,T,g);n&&n(p,T),u=c()+1;let v=_v(p,u),m=w.createHref(p);try{o.pushState(v,"",m)}catch(S){if(S instanceof DOMException&&S.name==="DataCloneError")throw S;i.location.assign(m)}s&&l&&l({action:a,location:w.location,delta:1})}function y(T,g){a=Me.Replace;let p=Yo(w.location,T,g);n&&n(p,T),u=c();let v=_v(p,u),m=w.createHref(p);o.replaceState(v,"",m),s&&l&&l({action:a,location:w.location,delta:0})}function _(T){let g=i.location.origin!=="null"?i.location.origin:i.location.href,p=typeof T=="string"?T:vi(T);return J(g,"No window.location.(origin|href) available to create URL for href: "+p),new URL(p,g)}let w={get action(){return a},get location(){return t(i,o)},listen(T){if(l)throw new Error("A history only accepts one active listener");return i.addEventListener(yv,h),l=T,()=>{i.removeEventListener(yv,h),l=null}},createHref(T){return e(i,T)},createURL:_,encodeLocation(T){let g=_(T);return{pathname:g.pathname,search:g.search,hash:g.hash}},push:d,replace:y,go(T){return o.go(T)}};return w}var Fe;(function(t){t.data="data",t.deferred="deferred",t.redirect="redirect",t.error="error"})(Fe||(Fe={}));const ok=new Set(["lazy","caseSensitive","path","id","index","children"]);function ak(t){return t.index===!0}function Jh(t,e,n,r){return n===void 0&&(n=[]),r===void 0&&(r={}),t.map((i,s)=>{let o=[...n,s],a=typeof i.id=="string"?i.id:o.join("-");if(J(i.index!==!0||!i.children,"Cannot specify children on an index route"),J(!r[a],'Found a route id collision on id "'+a+`".  Route id's must be globally unique within Data Router usages`),ak(i)){let l=Ee({},i,e(i),{id:a});return r[a]=l,l}else{let l=Ee({},i,e(i),{id:a,children:void 0});return r[a]=l,i.children&&(l.children=Jh(i.children,e,o,r)),l}})}function Yi(t,e,n){n===void 0&&(n="/");let r=typeof e=="string"?er(e):e,i=js(r.pathname||"/",n);if(i==null)return null;let s=O_(t);lk(s);let o=null;for(let a=0;o==null&&a<s.length;++a)o=vk(s[a],wk(i));return o}function O_(t,e,n,r){e===void 0&&(e=[]),n===void 0&&(n=[]),r===void 0&&(r="");let i=(s,o,a)=>{let l={relativePath:a===void 0?s.path||"":a,caseSensitive:s.caseSensitive===!0,childrenIndex:o,route:s};l.relativePath.startsWith("/")&&(J(l.relativePath.startsWith(r),'Absolute route path "'+l.relativePath+'" nested under path '+('"'+r+'" is not valid. An absolute child route path ')+"must start with the combined path of all its parent routes."),l.relativePath=l.relativePath.slice(r.length));let u=Fn([r,l.relativePath]),c=n.concat(l);s.children&&s.children.length>0&&(J(s.index!==!0,"Index routes must not have child routes. Please remove "+('all child routes from route path "'+u+'".')),O_(s.children,e,c,u)),!(s.path==null&&!s.index)&&e.push({path:u,score:mk(u,s.index),routesMeta:c})};return t.forEach((s,o)=>{var a;if(s.path===""||!((a=s.path)!=null&&a.includes("?")))i(s,o);else for(let l of D_(s.path))i(s,o,l)}),e}function D_(t){let e=t.split("/");if(e.length===0)return[];let[n,...r]=e,i=n.endsWith("?"),s=n.replace(/\?$/,"");if(r.length===0)return i?[s,""]:[s];let o=D_(r.join("/")),a=[];return a.push(...o.map(l=>l===""?s:[s,l].join("/"))),i&&a.push(...o),a.map(l=>t.startsWith("/")&&l===""?"/":l)}function lk(t){t.sort((e,n)=>e.score!==n.score?n.score-e.score:gk(e.routesMeta.map(r=>r.childrenIndex),n.routesMeta.map(r=>r.childrenIndex)))}const uk=/^:\w+$/,ck=3,fk=2,hk=1,dk=10,pk=-2,wv=t=>t==="*";function mk(t,e){let n=t.split("/"),r=n.length;return n.some(wv)&&(r+=pk),e&&(r+=fk),n.filter(i=>!wv(i)).reduce((i,s)=>i+(uk.test(s)?ck:s===""?hk:dk),r)}function gk(t,e){return t.length===e.length&&t.slice(0,-1).every((r,i)=>r===e[i])?t[t.length-1]-e[e.length-1]:0}function vk(t,e){let{routesMeta:n}=t,r={},i="/",s=[];for(let o=0;o<n.length;++o){let a=n[o],l=o===n.length-1,u=i==="/"?e:e.slice(i.length)||"/",c=yk({path:a.relativePath,caseSensitive:a.caseSensitive,end:l},u);if(!c)return null;Object.assign(r,c.params);let h=a.route;s.push({params:r,pathname:Fn([i,c.pathname]),pathnameBase:Sk(Fn([i,c.pathnameBase])),route:h}),c.pathnameBase!=="/"&&(i=Fn([i,c.pathnameBase]))}return s}function yk(t,e){typeof t=="string"&&(t={path:t,caseSensitive:!1,end:!0});let[n,r]=_k(t.path,t.caseSensitive,t.end),i=e.match(n);if(!i)return null;let s=i[0],o=s.replace(/(.)\/+$/,"$1"),a=i.slice(1);return{params:r.reduce((u,c,h)=>{if(c==="*"){let d=a[h]||"";o=s.slice(0,s.length-d.length).replace(/(.)\/+$/,"$1")}return u[c]=Ek(a[h]||"",c),u},{}),pathname:s,pathnameBase:o,pattern:t}}function _k(t,e,n){e===void 0&&(e=!1),n===void 0&&(n=!0),Is(t==="*"||!t.endsWith("*")||t.endsWith("/*"),'Route path "'+t+'" will be treated as if it were '+('"'+t.replace(/\*$/,"/*")+'" because the `*` character must ')+"always follow a `/` in the pattern. To get rid of this warning, "+('please change the route path to "'+t.replace(/\*$/,"/*")+'".'));let r=[],i="^"+t.replace(/\/*\*?$/,"").replace(/^\/*/,"/").replace(/[\\.*+^$?{}|()[\]]/g,"\\$&").replace(/\/:(\w+)/g,(o,a)=>(r.push(a),"/([^\\/]+)"));return t.endsWith("*")?(r.push("*"),i+=t==="*"||t==="/*"?"(.*)$":"(?:\\/(.+)|\\/*)$"):n?i+="\\/*$":t!==""&&t!=="/"&&(i+="(?:(?=\\/|$))"),[new RegExp(i,e?void 0:"i"),r]}function wk(t){try{return decodeURI(t)}catch(e){return Is(!1,'The URL path "'+t+'" could not be decoded because it is is a malformed URL segment. This is probably due to a bad percent '+("encoding ("+e+").")),t}}function Ek(t,e){try{return decodeURIComponent(t)}catch(n){return Is(!1,'The value for the URL param "'+e+'" will not be decoded because'+(' the string "'+t+'" is a malformed URL segment. This is probably')+(" due to a bad percent encoding ("+n+").")),t}}function js(t,e){if(e==="/")return t;if(!t.toLowerCase().startsWith(e.toLowerCase()))return null;let n=e.endsWith("/")?e.length-1:e.length,r=t.charAt(n);return r&&r!=="/"?null:t.slice(n)||"/"}function Tk(t,e){e===void 0&&(e="/");let{pathname:n,search:r="",hash:i=""}=typeof t=="string"?er(t):t;return{pathname:n?n.startsWith("/")?n:Ik(n,e):e,search:kk(r),hash:Ak(i)}}function Ik(t,e){let n=e.replace(/\/+$/,"").split("/");return t.split("/").forEach(i=>{i===".."?n.length>1&&n.pop():i!=="."&&n.push(i)}),n.length>1?n.join("/"):"/"}function Of(t,e,n,r){return"Cannot include a '"+t+"' character in a manually specified "+("`to."+e+"` field ["+JSON.stringify(r)+"].  Please separate it out to the ")+("`to."+n+"` field. Alternatively you may provide the full path as ")+'a string in <Link to="..."> and the router will parse it for you.'}function _c(t){return t.filter((e,n)=>n===0||e.route.path&&e.route.path.length>0)}function Vp(t,e,n,r){r===void 0&&(r=!1);let i;typeof t=="string"?i=er(t):(i=Ee({},t),J(!i.pathname||!i.pathname.includes("?"),Of("?","pathname","search",i)),J(!i.pathname||!i.pathname.includes("#"),Of("#","pathname","hash",i)),J(!i.search||!i.search.includes("#"),Of("#","search","hash",i)));let s=t===""||i.pathname==="",o=s?"/":i.pathname,a;if(r||o==null)a=n;else{let h=e.length-1;if(o.startsWith("..")){let d=o.split("/");for(;d[0]==="..";)d.shift(),h-=1;i.pathname=d.join("/")}a=h>=0?e[h]:"/"}let l=Tk(i,a),u=o&&o!=="/"&&o.endsWith("/"),c=(s||o===".")&&n.endsWith("/");return!l.pathname.endsWith("/")&&(u||c)&&(l.pathname+="/"),l}const Fn=t=>t.join("/").replace(/\/\/+/g,"/"),Sk=t=>t.replace(/\/+$/,"").replace(/^\/*/,"/"),kk=t=>!t||t==="?"?"":t.startsWith("?")?t:"?"+t,Ak=t=>!t||t==="#"?"":t.startsWith("#")?t:"#"+t;class Fp{constructor(e,n,r,i){i===void 0&&(i=!1),this.status=e,this.statusText=n||"",this.internal=i,r instanceof Error?(this.data=r.toString(),this.error=r):this.data=r}}function M_(t){return t!=null&&typeof t.status=="number"&&typeof t.statusText=="string"&&typeof t.internal=="boolean"&&"data"in t}const L_=["post","put","patch","delete"],xk=new Set(L_),Pk=["get",...L_],Rk=new Set(Pk),Ck=new Set([301,302,303,307,308]),Nk=new Set([307,308]),Df={state:"idle",location:void 0,formMethod:void 0,formAction:void 0,formEncType:void 0,formData:void 0,json:void 0,text:void 0},bk={state:"idle",data:void 0,formMethod:void 0,formAction:void 0,formEncType:void 0,formData:void 0,json:void 0,text:void 0},to={state:"unblocked",proceed:void 0,reset:void 0,location:void 0},j_=/^(?:[a-z][a-z0-9+.-]*:|\/\/)/i,Ok=t=>({hasErrorBoundary:!!t.hasErrorBoundary});function Dk(t){const e=t.window?t.window:typeof window<"u"?window:void 0,n=typeof e<"u"&&typeof e.document<"u"&&typeof e.document.createElement<"u",r=!n;J(t.routes.length>0,"You must provide a non-empty routes array to createRouter");let i;if(t.mapRouteProperties)i=t.mapRouteProperties;else if(t.detectErrorBoundary){let E=t.detectErrorBoundary;i=I=>({hasErrorBoundary:E(I)})}else i=Ok;let s={},o=Jh(t.routes,i,void 0,s),a,l=t.basename||"/",u=Ee({v7_normalizeFormMethod:!1,v7_prependBasename:!1},t.future),c=null,h=new Set,d=null,y=null,_=null,w=t.hydrationData!=null,T=Yi(o,t.history.location,l),g=null;if(T==null){let E=zt(404,{pathname:t.history.location.pathname}),{matches:I,route:k}=Pv(o);T=I,g={[k.id]:E}}let p=!T.some(E=>E.route.lazy)&&(!T.some(E=>E.route.loader)||t.hydrationData!=null),v,m={historyAction:t.history.action,location:t.history.location,matches:T,initialized:p,navigation:Df,restoreScrollPosition:t.hydrationData!=null?!1:null,preventScrollReset:!1,revalidation:"idle",loaderData:t.hydrationData&&t.hydrationData.loaderData||{},actionData:t.hydrationData&&t.hydrationData.actionData||null,errors:t.hydrationData&&t.hydrationData.errors||g,fetchers:new Map,blockers:new Map},S=Me.Pop,P=!1,A,O=!1,B=!1,U=[],He=[],ce=new Map,fn=0,rr=-1,ir=new Map,Ot=new Set,hn=new Map,D=new Map,H=new Map,X=!1;function Ie(){return c=t.history.listen(E=>{let{action:I,location:k,delta:j}=E;if(X){X=!1;return}Is(H.size===0||j!=null,"You are trying to use a blocker on a POP navigation to a location that was not created by @remix-run/router. This will fail silently in production. This can happen if you are navigating outside the router via `window.history.pushState`/`window.location.hash` instead of using router navigation APIs.  This can also happen if you are using createHashRouter and the user manually changes the URL.");let Y=sg({currentLocation:m.location,nextLocation:k,historyAction:I});if(Y&&j!=null){X=!0,t.history.go(j*-1),Ja(Y,{state:"blocked",location:k,proceed(){Ja(Y,{state:"proceeding",proceed:void 0,reset:void 0,location:k}),t.history.go(j)},reset(){let W=new Map(m.blockers);W.set(Y,to),fe({blockers:W})}});return}return sr(I,k)}),m.initialized||sr(Me.Pop,m.location),v}function Oe(){c&&c(),h.clear(),A&&A.abort(),m.fetchers.forEach((E,I)=>Zc(I)),m.blockers.forEach((E,I)=>ig(I))}function bi(E){return h.add(E),()=>h.delete(E)}function fe(E){m=Ee({},m,E),h.forEach(I=>I(m))}function Pn(E,I){var k,j;let Y=m.actionData!=null&&m.navigation.formMethod!=null&&en(m.navigation.formMethod)&&m.navigation.state==="loading"&&((k=E.state)==null?void 0:k._isRedirect)!==!0,W;I.actionData?Object.keys(I.actionData).length>0?W=I.actionData:W=null:Y?W=m.actionData:W=null;let q=I.loaderData?xv(m.loaderData,I.loaderData,I.matches||[],I.errors):m.loaderData,z=m.blockers;z.size>0&&(z=new Map(z),z.forEach((he,lt)=>z.set(lt,to)));let F=P===!0||m.navigation.formMethod!=null&&en(m.navigation.formMethod)&&((j=E.state)==null?void 0:j._isRedirect)!==!0;a&&(o=a,a=void 0),O||S===Me.Pop||(S===Me.Push?t.history.push(E,E.state):S===Me.Replace&&t.history.replace(E,E.state)),fe(Ee({},I,{actionData:W,loaderData:q,historyAction:S,location:E,initialized:!0,navigation:Df,revalidation:"idle",restoreScrollPosition:ag(E,I.matches||m.matches),preventScrollReset:F,blockers:z})),S=Me.Pop,P=!1,O=!1,B=!1,U=[],He=[]}async function dn(E,I){if(typeof E=="number"){t.history.go(E);return}let k=Zh(m.location,m.matches,l,u.v7_prependBasename,E,I==null?void 0:I.fromRouteId,I==null?void 0:I.relative),{path:j,submission:Y,error:W}=Ev(u.v7_normalizeFormMethod,!1,k,I),q=m.location,z=Yo(m.location,j,I&&I.state);z=Ee({},z,t.history.encodeLocation(z));let F=I&&I.replace!=null?I.replace:void 0,he=Me.Push;F===!0?he=Me.Replace:F===!1||Y!=null&&en(Y.formMethod)&&Y.formAction===m.location.pathname+m.location.search&&(he=Me.Replace);let lt=I&&"preventScrollReset"in I?I.preventScrollReset===!0:void 0,le=sg({currentLocation:q,nextLocation:z,historyAction:he});if(le){Ja(le,{state:"blocked",location:z,proceed(){Ja(le,{state:"proceeding",proceed:void 0,reset:void 0,location:z}),dn(E,I)},reset(){let Ne=new Map(m.blockers);Ne.set(le,to),fe({blockers:Ne})}});return}return await sr(he,z,{submission:Y,pendingError:W,preventScrollReset:lt,replace:I&&I.replace})}function Oi(){if(Jc(),fe({revalidation:"loading"}),m.navigation.state!=="submitting"){if(m.navigation.state==="idle"){sr(m.historyAction,m.location,{startUninterruptedRevalidation:!0});return}sr(S||m.historyAction,m.navigation.location,{overrideNavigation:m.navigation})}}async function sr(E,I,k){A&&A.abort(),A=null,S=E,O=(k&&k.startUninterruptedRevalidation)===!0,xI(m.location,m.matches),P=(k&&k.preventScrollReset)===!0;let j=a||o,Y=k&&k.overrideNavigation,W=Yi(j,I,l);if(!W){let Ne=zt(404,{pathname:I.pathname}),{matches:We,route:Gr}=Pv(j);ef(),Pn(I,{matches:We,loaderData:{},errors:{[Gr.id]:Ne}});return}if(m.initialized&&!B&&Fk(m.location,I)&&!(k&&k.submission&&en(k.submission.formMethod))){Pn(I,{matches:W});return}A=new AbortController;let q=ro(t.history,I,A.signal,k&&k.submission),z,F;if(k&&k.pendingError)F={[Xi(W).route.id]:k.pendingError};else if(k&&k.submission&&en(k.submission.formMethod)){let Ne=await wI(q,I,k.submission,W,{replace:k.replace});if(Ne.shortCircuited)return;z=Ne.pendingActionData,F=Ne.pendingActionError,Y=vl(I,k.submission),q=new Request(q.url,{signal:q.signal})}let{shortCircuited:he,loaderData:lt,errors:le}=await EI(q,I,W,Y,k&&k.submission,k&&k.fetcherSubmission,k&&k.replace,z,F);he||(A=null,Pn(I,Ee({matches:W},z?{actionData:z}:{},{loaderData:lt,errors:le})))}async function wI(E,I,k,j,Y){Y===void 0&&(Y={}),Jc();let W=Hk(I,k);fe({navigation:W});let q,z=td(j,I);if(!z.route.action&&!z.route.lazy)q={type:Fe.error,error:zt(405,{method:E.method,pathname:I.pathname,routeId:z.route.id})};else if(q=await no("action",E,z,j,s,i,l),E.signal.aborted)return{shortCircuited:!0};if(us(q)){let F;return Y&&Y.replace!=null?F=Y.replace:F=q.location===m.location.pathname+m.location.search,await Hs(m,q,{submission:k,replace:F}),{shortCircuited:!0}}if(So(q)){let F=Xi(j,z.route.id);return(Y&&Y.replace)!==!0&&(S=Me.Push),{pendingActionData:{},pendingActionError:{[F.route.id]:q.error}}}if(ti(q))throw zt(400,{type:"defer-action"});return{pendingActionData:{[z.route.id]:q.data}}}async function EI(E,I,k,j,Y,W,q,z,F){let he=j||vl(I,Y),lt=Y||W||Nv(he),le=a||o,[Ne,We]=Tv(t.history,m,k,lt,I,B,U,He,hn,Ot,le,l,z,F);if(ef(ue=>!(k&&k.some(Yt=>Yt.route.id===ue))||Ne&&Ne.some(Yt=>Yt.route.id===ue)),rr=++fn,Ne.length===0&&We.length===0){let ue=ng();return Pn(I,Ee({matches:k,loaderData:{},errors:F||null},z?{actionData:z}:{},ue?{fetchers:new Map(m.fetchers)}:{})),{shortCircuited:!0}}if(!O){We.forEach(Yt=>{let ar=m.fetchers.get(Yt.key),of=io(void 0,ar?ar.data:void 0);m.fetchers.set(Yt.key,of)});let ue=z||m.actionData;fe(Ee({navigation:he},ue?Object.keys(ue).length===0?{actionData:null}:{actionData:ue}:{},We.length>0?{fetchers:new Map(m.fetchers)}:{}))}We.forEach(ue=>{ce.has(ue.key)&&or(ue.key),ue.controller&&ce.set(ue.key,ue.controller)});let Gr=()=>We.forEach(ue=>or(ue.key));A&&A.signal.addEventListener("abort",Gr);let{results:qr,loaderResults:Ws,fetcherResults:tf}=await eg(m.matches,k,Ne,We,E);if(E.signal.aborted)return{shortCircuited:!0};A&&A.signal.removeEventListener("abort",Gr),We.forEach(ue=>ce.delete(ue.key));let Rn=Rv(qr);if(Rn){if(Rn.idx>=Ne.length){let ue=We[Rn.idx-Ne.length].key;Ot.add(ue)}return await Hs(m,Rn.result,{replace:q}),{shortCircuited:!0}}let{loaderData:Cn,errors:Za}=Av(m,k,Ne,Ws,F,We,tf,D);D.forEach((ue,Yt)=>{ue.subscribe(ar=>{(ar||ue.done)&&D.delete(Yt)})});let nf=ng(),rf=rg(rr),sf=nf||rf||We.length>0;return Ee({loaderData:Cn,errors:Za},sf?{fetchers:new Map(m.fetchers)}:{})}function Zm(E){return m.fetchers.get(E)||bk}function TI(E,I,k,j){if(r)throw new Error("router.fetch() was called during the server render, but it shouldn't be. You are likely calling a useFetcher() method in the body of your component. Try moving it to a useEffect or a callback.");ce.has(E)&&or(E);let Y=a||o,W=Zh(m.location,m.matches,l,u.v7_prependBasename,k,I,j==null?void 0:j.relative),q=Yi(Y,W,l);if(!q){Xa(E,I,zt(404,{pathname:W}));return}let{path:z,submission:F,error:he}=Ev(u.v7_normalizeFormMethod,!0,W,j);if(he){Xa(E,I,he);return}let lt=td(q,z);if(P=(j&&j.preventScrollReset)===!0,F&&en(F.formMethod)){II(E,I,z,lt,q,F);return}hn.set(E,{routeId:I,path:z}),SI(E,I,z,lt,q,F)}async function II(E,I,k,j,Y,W){if(Jc(),hn.delete(E),!j.route.action&&!j.route.lazy){let Ve=zt(405,{method:W.formMethod,pathname:k,routeId:I});Xa(E,I,Ve);return}let q=m.fetchers.get(E),z=Wk(W,q);m.fetchers.set(E,z),fe({fetchers:new Map(m.fetchers)});let F=new AbortController,he=ro(t.history,k,F.signal,W);ce.set(E,F);let lt=fn,le=await no("action",he,j,Y,s,i,l);if(he.signal.aborted){ce.get(E)===F&&ce.delete(E);return}if(us(le))if(ce.delete(E),rr>lt){let Ve=Li(void 0);m.fetchers.set(E,Ve),fe({fetchers:new Map(m.fetchers)});return}else{Ot.add(E);let Ve=io(W);return m.fetchers.set(E,Ve),fe({fetchers:new Map(m.fetchers)}),Hs(m,le,{submission:W,isFetchActionRedirect:!0})}if(So(le)){Xa(E,I,le.error);return}if(ti(le))throw zt(400,{type:"defer-action"});let Ne=m.navigation.location||m.location,We=ro(t.history,Ne,F.signal),Gr=a||o,qr=m.navigation.state!=="idle"?Yi(Gr,m.navigation.location,l):m.matches;J(qr,"Didn't find any matches after fetcher action");let Ws=++fn;ir.set(E,Ws);let tf=io(W,le.data);m.fetchers.set(E,tf);let[Rn,Cn]=Tv(t.history,m,qr,W,Ne,B,U,He,hn,Ot,Gr,l,{[j.route.id]:le.data},void 0);Cn.filter(Ve=>Ve.key!==E).forEach(Ve=>{let Ks=Ve.key,lg=m.fetchers.get(Ks),RI=io(void 0,lg?lg.data:void 0);m.fetchers.set(Ks,RI),ce.has(Ks)&&or(Ks),Ve.controller&&ce.set(Ks,Ve.controller)}),fe({fetchers:new Map(m.fetchers)});let Za=()=>Cn.forEach(Ve=>or(Ve.key));F.signal.addEventListener("abort",Za);let{results:nf,loaderResults:rf,fetcherResults:sf}=await eg(m.matches,qr,Rn,Cn,We);if(F.signal.aborted)return;F.signal.removeEventListener("abort",Za),ir.delete(E),ce.delete(E),Cn.forEach(Ve=>ce.delete(Ve.key));let ue=Rv(nf);if(ue){if(ue.idx>=Rn.length){let Ve=Cn[ue.idx-Rn.length].key;Ot.add(Ve)}return Hs(m,ue.result)}let{loaderData:Yt,errors:ar}=Av(m,m.matches,Rn,rf,void 0,Cn,sf,D);if(m.fetchers.has(E)){let Ve=Li(le.data);m.fetchers.set(E,Ve)}let of=rg(Ws);m.navigation.state==="loading"&&Ws>rr?(J(S,"Expected pending action"),A&&A.abort(),Pn(m.navigation.location,{matches:qr,loaderData:Yt,errors:ar,fetchers:new Map(m.fetchers)})):(fe(Ee({errors:ar,loaderData:xv(m.loaderData,Yt,qr,ar)},of||Cn.length>0?{fetchers:new Map(m.fetchers)}:{})),B=!1)}async function SI(E,I,k,j,Y,W){let q=m.fetchers.get(E),z=io(W,q?q.data:void 0);m.fetchers.set(E,z),fe({fetchers:new Map(m.fetchers)});let F=new AbortController,he=ro(t.history,k,F.signal);ce.set(E,F);let lt=fn,le=await no("loader",he,j,Y,s,i,l);if(ti(le)&&(le=await U_(le,he.signal,!0)||le),ce.get(E)===F&&ce.delete(E),he.signal.aborted)return;if(us(le))if(rr>lt){let We=Li(void 0);m.fetchers.set(E,We),fe({fetchers:new Map(m.fetchers)});return}else{Ot.add(E),await Hs(m,le);return}if(So(le)){let We=Xi(m.matches,I);m.fetchers.delete(E),fe({fetchers:new Map(m.fetchers),errors:{[We.route.id]:le.error}});return}J(!ti(le),"Unhandled fetcher deferred data");let Ne=Li(le.data);m.fetchers.set(E,Ne),fe({fetchers:new Map(m.fetchers)})}async function Hs(E,I,k){let{submission:j,replace:Y,isFetchActionRedirect:W}=k===void 0?{}:k;I.revalidate&&(B=!0);let q=Yo(E.location,I.location,Ee({_isRedirect:!0},W?{_isFetchActionRedirect:!0}:{}));if(J(q,"Expected a location on the redirect navigation"),j_.test(I.location)&&n){let he=t.history.createURL(I.location),lt=js(he.pathname,l)==null;if(e.location.origin!==he.origin||lt){Y?e.location.replace(I.location):e.location.assign(I.location);return}}A=null;let z=Y===!0?Me.Replace:Me.Push,F=j||Nv(E.navigation);if(Nk.has(I.status)&&F&&en(F.formMethod))await sr(z,q,{submission:Ee({},F,{formAction:I.location}),preventScrollReset:P});else if(W)await sr(z,q,{overrideNavigation:vl(q),fetcherSubmission:F,preventScrollReset:P});else{let he=vl(q,F);await sr(z,q,{overrideNavigation:he,preventScrollReset:P})}}async function eg(E,I,k,j,Y){let W=await Promise.all([...k.map(F=>no("loader",Y,F,I,s,i,l)),...j.map(F=>F.matches&&F.match&&F.controller?no("loader",ro(t.history,F.path,F.controller.signal),F.match,F.matches,s,i,l):{type:Fe.error,error:zt(404,{pathname:F.path})})]),q=W.slice(0,k.length),z=W.slice(k.length);return await Promise.all([Cv(E,k,q,q.map(()=>Y.signal),!1,m.loaderData),Cv(E,j.map(F=>F.match),z,j.map(F=>F.controller?F.controller.signal:null),!0)]),{results:W,loaderResults:q,fetcherResults:z}}function Jc(){B=!0,U.push(...ef()),hn.forEach((E,I)=>{ce.has(I)&&(He.push(I),or(I))})}function Xa(E,I,k){let j=Xi(m.matches,I);Zc(E),fe({errors:{[j.route.id]:k},fetchers:new Map(m.fetchers)})}function Zc(E){let I=m.fetchers.get(E);ce.has(E)&&!(I&&I.state==="loading"&&ir.has(E))&&or(E),hn.delete(E),ir.delete(E),Ot.delete(E),m.fetchers.delete(E)}function or(E){let I=ce.get(E);J(I,"Expected fetch controller: "+E),I.abort(),ce.delete(E)}function tg(E){for(let I of E){let k=Zm(I),j=Li(k.data);m.fetchers.set(I,j)}}function ng(){let E=[],I=!1;for(let k of Ot){let j=m.fetchers.get(k);J(j,"Expected fetcher: "+k),j.state==="loading"&&(Ot.delete(k),E.push(k),I=!0)}return tg(E),I}function rg(E){let I=[];for(let[k,j]of ir)if(j<E){let Y=m.fetchers.get(k);J(Y,"Expected fetcher: "+k),Y.state==="loading"&&(or(k),ir.delete(k),I.push(k))}return tg(I),I.length>0}function kI(E,I){let k=m.blockers.get(E)||to;return H.get(E)!==I&&H.set(E,I),k}function ig(E){m.blockers.delete(E),H.delete(E)}function Ja(E,I){let k=m.blockers.get(E)||to;J(k.state==="unblocked"&&I.state==="blocked"||k.state==="blocked"&&I.state==="blocked"||k.state==="blocked"&&I.state==="proceeding"||k.state==="blocked"&&I.state==="unblocked"||k.state==="proceeding"&&I.state==="unblocked","Invalid blocker state transition: "+k.state+" -> "+I.state);let j=new Map(m.blockers);j.set(E,I),fe({blockers:j})}function sg(E){let{currentLocation:I,nextLocation:k,historyAction:j}=E;if(H.size===0)return;H.size>1&&Is(!1,"A router only supports one blocker at a time");let Y=Array.from(H.entries()),[W,q]=Y[Y.length-1],z=m.blockers.get(W);if(!(z&&z.state==="proceeding")&&q({currentLocation:I,nextLocation:k,historyAction:j}))return W}function ef(E){let I=[];return D.forEach((k,j)=>{(!E||E(j))&&(k.cancel(),I.push(j),D.delete(j))}),I}function AI(E,I,k){if(d=E,_=I,y=k||null,!w&&m.navigation===Df){w=!0;let j=ag(m.location,m.matches);j!=null&&fe({restoreScrollPosition:j})}return()=>{d=null,_=null,y=null}}function og(E,I){return y&&y(E,I.map(j=>Bk(j,m.loaderData)))||E.key}function xI(E,I){if(d&&_){let k=og(E,I);d[k]=_()}}function ag(E,I){if(d){let k=og(E,I),j=d[k];if(typeof j=="number")return j}return null}function PI(E){s={},a=Jh(E,i,void 0,s)}return v={get basename(){return l},get state(){return m},get routes(){return o},initialize:Ie,subscribe:bi,enableScrollRestoration:AI,navigate:dn,fetch:TI,revalidate:Oi,createHref:E=>t.history.createHref(E),encodeLocation:E=>t.history.encodeLocation(E),getFetcher:Zm,deleteFetcher:Zc,dispose:Oe,getBlocker:kI,deleteBlocker:ig,_internalFetchControllers:ce,_internalActiveDeferreds:D,_internalSetRoutes:PI},v}function Mk(t){return t!=null&&("formData"in t&&t.formData!=null||"body"in t&&t.body!==void 0)}function Zh(t,e,n,r,i,s,o){let a,l;if(s!=null&&o!=="path"){a=[];for(let c of e)if(a.push(c),c.route.id===s){l=c;break}}else a=e,l=e[e.length-1];let u=Vp(i||".",_c(a).map(c=>c.pathnameBase),js(t.pathname,n)||t.pathname,o==="path");return i==null&&(u.search=t.search,u.hash=t.hash),(i==null||i===""||i===".")&&l&&l.route.index&&!Up(u.search)&&(u.search=u.search?u.search.replace(/^\?/,"?index&"):"?index"),r&&n!=="/"&&(u.pathname=u.pathname==="/"?n:Fn([n,u.pathname])),vi(u)}function Ev(t,e,n,r){if(!r||!Mk(r))return{path:n};if(r.formMethod&&!zk(r.formMethod))return{path:n,error:zt(405,{method:r.formMethod})};let i=()=>({path:n,error:zt(400,{type:"invalid-body"})}),s=r.formMethod||"get",o=t?s.toUpperCase():s.toLowerCase(),a=F_(n);if(r.body!==void 0){if(r.formEncType==="text/plain"){if(!en(o))return i();let d=typeof r.body=="string"?r.body:r.body instanceof FormData||r.body instanceof URLSearchParams?Array.from(r.body.entries()).reduce((y,_)=>{let[w,T]=_;return""+y+w+"="+T+`
`},""):String(r.body);return{path:n,submission:{formMethod:o,formAction:a,formEncType:r.formEncType,formData:void 0,json:void 0,text:d}}}else if(r.formEncType==="application/json"){if(!en(o))return i();try{let d=typeof r.body=="string"?JSON.parse(r.body):r.body;return{path:n,submission:{formMethod:o,formAction:a,formEncType:r.formEncType,formData:void 0,json:d,text:void 0}}}catch{return i()}}}J(typeof FormData=="function","FormData is not available in this environment");let l,u;if(r.formData)l=ed(r.formData),u=r.formData;else if(r.body instanceof FormData)l=ed(r.body),u=r.body;else if(r.body instanceof URLSearchParams)l=r.body,u=kv(l);else if(r.body==null)l=new URLSearchParams,u=new FormData;else try{l=new URLSearchParams(r.body),u=kv(l)}catch{return i()}let c={formMethod:o,formAction:a,formEncType:r&&r.formEncType||"application/x-www-form-urlencoded",formData:u,json:void 0,text:void 0};if(en(c.formMethod))return{path:n,submission:c};let h=er(n);return e&&h.search&&Up(h.search)&&l.append("index",""),h.search="?"+l,{path:vi(h),submission:c}}function Lk(t,e){let n=t;if(e){let r=t.findIndex(i=>i.route.id===e);r>=0&&(n=t.slice(0,r))}return n}function Tv(t,e,n,r,i,s,o,a,l,u,c,h,d,y){let _=y?Object.values(y)[0]:d?Object.values(d)[0]:void 0,w=t.createURL(e.location),T=t.createURL(i),g=y?Object.keys(y)[0]:void 0,v=Lk(n,g).filter((S,P)=>{if(S.route.lazy)return!0;if(S.route.loader==null)return!1;if(jk(e.loaderData,e.matches[P],S)||o.some(B=>B===S.route.id))return!0;let A=e.matches[P],O=S;return Iv(S,Ee({currentUrl:w,currentParams:A.params,nextUrl:T,nextParams:O.params},r,{actionResult:_,defaultShouldRevalidate:s||w.pathname+w.search===T.pathname+T.search||w.search!==T.search||V_(A,O)}))}),m=[];return l.forEach((S,P)=>{if(!n.some(He=>He.route.id===S.routeId))return;let A=Yi(c,S.path,h);if(!A){m.push({key:P,routeId:S.routeId,path:S.path,matches:null,match:null,controller:null});return}let O=e.fetchers.get(P),B=td(A,S.path),U=!1;u.has(P)?U=!1:a.includes(P)?U=!0:O&&O.state!=="idle"&&O.data===void 0?U=s:U=Iv(B,Ee({currentUrl:w,currentParams:e.matches[e.matches.length-1].params,nextUrl:T,nextParams:n[n.length-1].params},r,{actionResult:_,defaultShouldRevalidate:s})),U&&m.push({key:P,routeId:S.routeId,path:S.path,matches:A,match:B,controller:new AbortController})}),[v,m]}function jk(t,e,n){let r=!e||n.route.id!==e.route.id,i=t[n.route.id]===void 0;return r||i}function V_(t,e){let n=t.route.path;return t.pathname!==e.pathname||n!=null&&n.endsWith("*")&&t.params["*"]!==e.params["*"]}function Iv(t,e){if(t.route.shouldRevalidate){let n=t.route.shouldRevalidate(e);if(typeof n=="boolean")return n}return e.defaultShouldRevalidate}async function Sv(t,e,n){if(!t.lazy)return;let r=await t.lazy();if(!t.lazy)return;let i=n[t.id];J(i,"No route found in manifest");let s={};for(let o in r){let l=i[o]!==void 0&&o!=="hasErrorBoundary";Is(!l,'Route "'+i.id+'" has a static property "'+o+'" defined but its lazy function is also returning a value for this property. '+('The lazy route property "'+o+'" will be ignored.')),!l&&!ok.has(o)&&(s[o]=r[o])}Object.assign(i,s),Object.assign(i,Ee({},e(i),{lazy:void 0}))}async function no(t,e,n,r,i,s,o,a){a===void 0&&(a={});let l,u,c,h=_=>{let w,T=new Promise((g,p)=>w=p);return c=()=>w(),e.signal.addEventListener("abort",c),Promise.race([_({request:e,params:n.params,context:a.requestContext}),T])};try{let _=n.route[t];if(n.route.lazy)if(_)u=(await Promise.all([h(_),Sv(n.route,s,i)]))[0];else if(await Sv(n.route,s,i),_=n.route[t],_)u=await h(_);else if(t==="action"){let w=new URL(e.url),T=w.pathname+w.search;throw zt(405,{method:e.method,pathname:T,routeId:n.route.id})}else return{type:Fe.data,data:void 0};else if(_)u=await h(_);else{let w=new URL(e.url),T=w.pathname+w.search;throw zt(404,{pathname:T})}J(u!==void 0,"You defined "+(t==="action"?"an action":"a loader")+" for route "+('"'+n.route.id+"\" but didn't return anything from your `"+t+"` ")+"function. Please return a value or `null`.")}catch(_){l=Fe.error,u=_}finally{c&&e.signal.removeEventListener("abort",c)}if($k(u)){let _=u.status;if(Ck.has(_)){let g=u.headers.get("Location");if(J(g,"Redirects returned/thrown from loaders/actions must have a Location header"),!j_.test(g))g=Zh(new URL(e.url),r.slice(0,r.indexOf(n)+1),o,!0,g);else if(!a.isStaticRequest){let p=new URL(e.url),v=g.startsWith("//")?new URL(p.protocol+g):new URL(g),m=js(v.pathname,o)!=null;v.origin===p.origin&&m&&(g=v.pathname+v.search+v.hash)}if(a.isStaticRequest)throw u.headers.set("Location",g),u;return{type:Fe.redirect,status:_,location:g,revalidate:u.headers.get("X-Remix-Revalidate")!==null}}if(a.isRouteRequest)throw{type:l||Fe.data,response:u};let w,T=u.headers.get("Content-Type");return T&&/\bapplication\/json\b/.test(T)?w=await u.json():w=await u.text(),l===Fe.error?{type:l,error:new Fp(_,u.statusText,w),headers:u.headers}:{type:Fe.data,data:w,statusCode:u.status,headers:u.headers}}if(l===Fe.error)return{type:l,error:u};if(Uk(u)){var d,y;return{type:Fe.deferred,deferredData:u,statusCode:(d=u.init)==null?void 0:d.status,headers:((y=u.init)==null?void 0:y.headers)&&new Headers(u.init.headers)}}return{type:Fe.data,data:u}}function ro(t,e,n,r){let i=t.createURL(F_(e)).toString(),s={signal:n};if(r&&en(r.formMethod)){let{formMethod:o,formEncType:a}=r;s.method=o.toUpperCase(),a==="application/json"?(s.headers=new Headers({"Content-Type":a}),s.body=JSON.stringify(r.json)):a==="text/plain"?s.body=r.text:a==="application/x-www-form-urlencoded"&&r.formData?s.body=ed(r.formData):s.body=r.formData}return new Request(i,s)}function ed(t){let e=new URLSearchParams;for(let[n,r]of t.entries())e.append(n,typeof r=="string"?r:r.name);return e}function kv(t){let e=new FormData;for(let[n,r]of t.entries())e.append(n,r);return e}function Vk(t,e,n,r,i){let s={},o=null,a,l=!1,u={};return n.forEach((c,h)=>{let d=e[h].route.id;if(J(!us(c),"Cannot handle redirect results in processLoaderData"),So(c)){let y=Xi(t,d),_=c.error;r&&(_=Object.values(r)[0],r=void 0),o=o||{},o[y.route.id]==null&&(o[y.route.id]=_),s[d]=void 0,l||(l=!0,a=M_(c.error)?c.error.status:500),c.headers&&(u[d]=c.headers)}else ti(c)?(i.set(d,c.deferredData),s[d]=c.deferredData.data):s[d]=c.data,c.statusCode!=null&&c.statusCode!==200&&!l&&(a=c.statusCode),c.headers&&(u[d]=c.headers)}),r&&(o=r,s[Object.keys(r)[0]]=void 0),{loaderData:s,errors:o,statusCode:a||200,loaderHeaders:u}}function Av(t,e,n,r,i,s,o,a){let{loaderData:l,errors:u}=Vk(e,n,r,i,a);for(let c=0;c<s.length;c++){let{key:h,match:d,controller:y}=s[c];J(o!==void 0&&o[c]!==void 0,"Did not find corresponding fetcher result");let _=o[c];if(!(y&&y.signal.aborted))if(So(_)){let w=Xi(t.matches,d==null?void 0:d.route.id);u&&u[w.route.id]||(u=Ee({},u,{[w.route.id]:_.error})),t.fetchers.delete(h)}else if(us(_))J(!1,"Unhandled fetcher revalidation redirect");else if(ti(_))J(!1,"Unhandled fetcher deferred data");else{let w=Li(_.data);t.fetchers.set(h,w)}}return{loaderData:l,errors:u}}function xv(t,e,n,r){let i=Ee({},e);for(let s of n){let o=s.route.id;if(e.hasOwnProperty(o)?e[o]!==void 0&&(i[o]=e[o]):t[o]!==void 0&&s.route.loader&&(i[o]=t[o]),r&&r.hasOwnProperty(o))break}return i}function Xi(t,e){return(e?t.slice(0,t.findIndex(r=>r.route.id===e)+1):[...t]).reverse().find(r=>r.route.hasErrorBoundary===!0)||t[0]}function Pv(t){let e=t.find(n=>n.index||!n.path||n.path==="/")||{id:"__shim-error-route__"};return{matches:[{params:{},pathname:"",pathnameBase:"",route:e}],route:e}}function zt(t,e){let{pathname:n,routeId:r,method:i,type:s}=e===void 0?{}:e,o="Unknown Server Error",a="Unknown @remix-run/router error";return t===400?(o="Bad Request",i&&n&&r?a="You made a "+i+' request to "'+n+'" but '+('did not provide a `loader` for route "'+r+'", ')+"so there is no way to handle the request.":s==="defer-action"?a="defer() is not supported in actions":s==="invalid-body"&&(a="Unable to encode submission body")):t===403?(o="Forbidden",a='Route "'+r+'" does not match URL "'+n+'"'):t===404?(o="Not Found",a='No route matches URL "'+n+'"'):t===405&&(o="Method Not Allowed",i&&n&&r?a="You made a "+i.toUpperCase()+' request to "'+n+'" but '+('did not provide an `action` for route "'+r+'", ')+"so there is no way to handle the request.":i&&(a='Invalid request method "'+i.toUpperCase()+'"')),new Fp(t||500,o,new Error(a),!0)}function Rv(t){for(let e=t.length-1;e>=0;e--){let n=t[e];if(us(n))return{result:n,idx:e}}}function F_(t){let e=typeof t=="string"?er(t):t;return vi(Ee({},e,{hash:""}))}function Fk(t,e){return t.pathname!==e.pathname||t.search!==e.search?!1:t.hash===""?e.hash!=="":t.hash===e.hash?!0:e.hash!==""}function ti(t){return t.type===Fe.deferred}function So(t){return t.type===Fe.error}function us(t){return(t&&t.type)===Fe.redirect}function Uk(t){let e=t;return e&&typeof e=="object"&&typeof e.data=="object"&&typeof e.subscribe=="function"&&typeof e.cancel=="function"&&typeof e.resolveData=="function"}function $k(t){return t!=null&&typeof t.status=="number"&&typeof t.statusText=="string"&&typeof t.headers=="object"&&typeof t.body<"u"}function zk(t){return Rk.has(t.toLowerCase())}function en(t){return xk.has(t.toLowerCase())}async function Cv(t,e,n,r,i,s){for(let o=0;o<n.length;o++){let a=n[o],l=e[o];if(!l)continue;let u=t.find(h=>h.route.id===l.route.id),c=u!=null&&!V_(u,l)&&(s&&s[l.route.id])!==void 0;if(ti(a)&&(i||c)){let h=r[o];J(h,"Expected an AbortSignal for revalidating fetcher deferred result"),await U_(a,h,i).then(d=>{d&&(n[o]=d||n[o])})}}}async function U_(t,e,n){if(n===void 0&&(n=!1),!await t.deferredData.resolveData(e)){if(n)try{return{type:Fe.data,data:t.deferredData.unwrappedData}}catch(i){return{type:Fe.error,error:i}}return{type:Fe.data,data:t.deferredData.data}}}function Up(t){return new URLSearchParams(t).getAll("index").some(e=>e==="")}function Bk(t,e){let{route:n,pathname:r,params:i}=t;return{id:n.id,pathname:r,params:i,data:e[n.id],handle:n.handle}}function td(t,e){let n=typeof e=="string"?er(e).search:e.search;if(t[t.length-1].route.index&&Up(n||""))return t[t.length-1];let r=_c(t);return r[r.length-1]}function Nv(t){let{formMethod:e,formAction:n,formEncType:r,text:i,formData:s,json:o}=t;if(!(!e||!n||!r)){if(i!=null)return{formMethod:e,formAction:n,formEncType:r,formData:void 0,json:void 0,text:i};if(s!=null)return{formMethod:e,formAction:n,formEncType:r,formData:s,json:void 0,text:void 0};if(o!==void 0)return{formMethod:e,formAction:n,formEncType:r,formData:void 0,json:o,text:void 0}}}function vl(t,e){return e?{state:"loading",location:t,formMethod:e.formMethod,formAction:e.formAction,formEncType:e.formEncType,formData:e.formData,json:e.json,text:e.text}:{state:"loading",location:t,formMethod:void 0,formAction:void 0,formEncType:void 0,formData:void 0,json:void 0,text:void 0}}function Hk(t,e){return{state:"submitting",location:t,formMethod:e.formMethod,formAction:e.formAction,formEncType:e.formEncType,formData:e.formData,json:e.json,text:e.text}}function io(t,e){return t?{state:"loading",formMethod:t.formMethod,formAction:t.formAction,formEncType:t.formEncType,formData:t.formData,json:t.json,text:t.text,data:e," _hasFetcherDoneAnything ":!0}:{state:"loading",formMethod:void 0,formAction:void 0,formEncType:void 0,formData:void 0,json:void 0,text:void 0,data:e," _hasFetcherDoneAnything ":!0}}function Wk(t,e){return{state:"submitting",formMethod:t.formMethod,formAction:t.formAction,formEncType:t.formEncType,formData:t.formData,json:t.json,text:t.text,data:e?e.data:void 0," _hasFetcherDoneAnything ":!0}}function Li(t){return{state:"idle",formMethod:void 0,formAction:void 0,formEncType:void 0,formData:void 0,json:void 0,text:void 0,data:t," _hasFetcherDoneAnything ":!0}}/**
 * React Router v6.14.2
 *
 * Copyright (c) Remix Software Inc.
 *
 * This source code is licensed under the MIT license found in the
 * LICENSE.md file in the root directory of this source tree.
 *
 * @license MIT
 */function xu(){return xu=Object.assign?Object.assign.bind():function(t){for(var e=1;e<arguments.length;e++){var n=arguments[e];for(var r in n)Object.prototype.hasOwnProperty.call(n,r)&&(t[r]=n[r])}return t},xu.apply(this,arguments)}const wc=N.createContext(null),$p=N.createContext(null),Ri=N.createContext(null),Ec=N.createContext(null),Hr=N.createContext({outlet:null,matches:[],isDataRoute:!1}),$_=N.createContext(null);function Kk(t,e){let{relative:n}=e===void 0?{}:e;Aa()||J(!1);let{basename:r,navigator:i}=N.useContext(Ri),{hash:s,pathname:o,search:a}=zp(t,{relative:n}),l=o;return r!=="/"&&(l=o==="/"?r:Fn([r,o])),i.createHref({pathname:l,search:a,hash:s})}function Aa(){return N.useContext(Ec)!=null}function xa(){return Aa()||J(!1),N.useContext(Ec).location}function z_(t){N.useContext(Ri).static||N.useLayoutEffect(t)}function B_(){let{isDataRoute:t}=N.useContext(Hr);return t?oA():Gk()}function Gk(){Aa()||J(!1);let t=N.useContext(wc),{basename:e,navigator:n}=N.useContext(Ri),{matches:r}=N.useContext(Hr),{pathname:i}=xa(),s=JSON.stringify(_c(r).map(l=>l.pathnameBase)),o=N.useRef(!1);return z_(()=>{o.current=!0}),N.useCallback(function(l,u){if(u===void 0&&(u={}),!o.current)return;if(typeof l=="number"){n.go(l);return}let c=Vp(l,JSON.parse(s),i,u.relative==="path");t==null&&e!=="/"&&(c.pathname=c.pathname==="/"?e:Fn([e,c.pathname])),(u.replace?n.replace:n.push)(c,u.state,u)},[e,n,s,i,t])}const qk=N.createContext(null);function Qk(t){let e=N.useContext(Hr).outlet;return e&&N.createElement(qk.Provider,{value:t},e)}function zp(t,e){let{relative:n}=e===void 0?{}:e,{matches:r}=N.useContext(Hr),{pathname:i}=xa(),s=JSON.stringify(_c(r).map(o=>o.pathnameBase));return N.useMemo(()=>Vp(t,JSON.parse(s),i,n==="path"),[t,s,i,n])}function Yk(t,e,n){Aa()||J(!1);let{navigator:r}=N.useContext(Ri),{matches:i}=N.useContext(Hr),s=i[i.length-1],o=s?s.params:{};s&&s.pathname;let a=s?s.pathnameBase:"/";s&&s.route;let l=xa(),u;if(e){var c;let w=typeof e=="string"?er(e):e;a==="/"||(c=w.pathname)!=null&&c.startsWith(a)||J(!1),u=w}else u=l;let h=u.pathname||"/",d=a==="/"?h:h.slice(a.length)||"/",y=Yi(t,{pathname:d}),_=tA(y&&y.map(w=>Object.assign({},w,{params:Object.assign({},o,w.params),pathname:Fn([a,r.encodeLocation?r.encodeLocation(w.pathname).pathname:w.pathname]),pathnameBase:w.pathnameBase==="/"?a:Fn([a,r.encodeLocation?r.encodeLocation(w.pathnameBase).pathname:w.pathnameBase])})),i,n);return e&&_?N.createElement(Ec.Provider,{value:{location:xu({pathname:"/",search:"",hash:"",state:null,key:"default"},u),navigationType:Me.Pop}},_):_}function Xk(){let t=sA(),e=M_(t)?t.status+" "+t.statusText:t instanceof Error?t.message:JSON.stringify(t),n=t instanceof Error?t.stack:null,i={padding:"0.5rem",backgroundColor:"rgba(200,200,200, 0.5)"},s=null;return N.createElement(N.Fragment,null,N.createElement("h2",null,"Unexpected Application Error!"),N.createElement("h3",{style:{fontStyle:"italic"}},e),n?N.createElement("pre",{style:i},n):null,s)}const Jk=N.createElement(Xk,null);class Zk extends N.Component{constructor(e){super(e),this.state={location:e.location,revalidation:e.revalidation,error:e.error}}static getDerivedStateFromError(e){return{error:e}}static getDerivedStateFromProps(e,n){return n.location!==e.location||n.revalidation!=="idle"&&e.revalidation==="idle"?{error:e.error,location:e.location,revalidation:e.revalidation}:{error:e.error||n.error,location:n.location,revalidation:e.revalidation||n.revalidation}}componentDidCatch(e,n){console.error("React Router caught the following error during render",e,n)}render(){return this.state.error?N.createElement(Hr.Provider,{value:this.props.routeContext},N.createElement($_.Provider,{value:this.state.error,children:this.props.component})):this.props.children}}function eA(t){let{routeContext:e,match:n,children:r}=t,i=N.useContext(wc);return i&&i.static&&i.staticContext&&(n.route.errorElement||n.route.ErrorBoundary)&&(i.staticContext._deepestRenderedBoundaryId=n.route.id),N.createElement(Hr.Provider,{value:e},r)}function tA(t,e,n){var r;if(e===void 0&&(e=[]),n===void 0&&(n=null),t==null){var i;if((i=n)!=null&&i.errors)t=n.matches;else return null}let s=t,o=(r=n)==null?void 0:r.errors;if(o!=null){let a=s.findIndex(l=>l.route.id&&(o==null?void 0:o[l.route.id]));a>=0||J(!1),s=s.slice(0,Math.min(s.length,a+1))}return s.reduceRight((a,l,u)=>{let c=l.route.id?o==null?void 0:o[l.route.id]:null,h=null;n&&(h=l.route.errorElement||Jk);let d=e.concat(s.slice(0,u+1)),y=()=>{let _;return c?_=h:l.route.Component?_=N.createElement(l.route.Component,null):l.route.element?_=l.route.element:_=a,N.createElement(eA,{match:l,routeContext:{outlet:a,matches:d,isDataRoute:n!=null},children:_})};return n&&(l.route.ErrorBoundary||l.route.errorElement||u===0)?N.createElement(Zk,{location:n.location,revalidation:n.revalidation,component:h,error:c,children:y(),routeContext:{outlet:null,matches:d,isDataRoute:!0}}):y()},null)}var nd;(function(t){t.UseBlocker="useBlocker",t.UseRevalidator="useRevalidator",t.UseNavigateStable="useNavigate"})(nd||(nd={}));var Xo;(function(t){t.UseBlocker="useBlocker",t.UseLoaderData="useLoaderData",t.UseActionData="useActionData",t.UseRouteError="useRouteError",t.UseNavigation="useNavigation",t.UseRouteLoaderData="useRouteLoaderData",t.UseMatches="useMatches",t.UseRevalidator="useRevalidator",t.UseNavigateStable="useNavigate",t.UseRouteId="useRouteId"})(Xo||(Xo={}));function nA(t){let e=N.useContext(wc);return e||J(!1),e}function rA(t){let e=N.useContext($p);return e||J(!1),e}function iA(t){let e=N.useContext(Hr);return e||J(!1),e}function H_(t){let e=iA(),n=e.matches[e.matches.length-1];return n.route.id||J(!1),n.route.id}function sA(){var t;let e=N.useContext($_),n=rA(Xo.UseRouteError),r=H_(Xo.UseRouteError);return e||((t=n.errors)==null?void 0:t[r])}function oA(){let{router:t}=nA(nd.UseNavigateStable),e=H_(Xo.UseNavigateStable),n=N.useRef(!1);return z_(()=>{n.current=!0}),N.useCallback(function(i,s){s===void 0&&(s={}),n.current&&(typeof i=="number"?t.navigate(i):t.navigate(i,xu({fromRouteId:e},s)))},[t,e])}const aA="startTransition",bv=KI[aA];function lA(t){let{fallbackElement:e,router:n,future:r}=t,[i,s]=N.useState(n.state),{v7_startTransition:o}=r||{},a=N.useCallback(h=>{o&&bv?bv(()=>s(h)):s(h)},[s,o]);N.useLayoutEffect(()=>n.subscribe(a),[n,a]);let l=N.useMemo(()=>({createHref:n.createHref,encodeLocation:n.encodeLocation,go:h=>n.navigate(h),push:(h,d,y)=>n.navigate(h,{state:d,preventScrollReset:y==null?void 0:y.preventScrollReset}),replace:(h,d,y)=>n.navigate(h,{replace:!0,state:d,preventScrollReset:y==null?void 0:y.preventScrollReset})}),[n]),u=n.basename||"/",c=N.useMemo(()=>({router:n,navigator:l,static:!1,basename:u}),[n,l,u]);return N.createElement(N.Fragment,null,N.createElement(wc.Provider,{value:c},N.createElement($p.Provider,{value:i},N.createElement(cA,{basename:u,location:i.location,navigationType:i.historyAction,navigator:l},i.initialized?N.createElement(uA,{routes:n.routes,state:i}):e))),null)}function uA(t){let{routes:e,state:n}=t;return Yk(e,void 0,n)}function W_(t){return Qk(t.context)}function Ke(t){J(!1)}function cA(t){let{basename:e="/",children:n=null,location:r,navigationType:i=Me.Pop,navigator:s,static:o=!1}=t;Aa()&&J(!1);let a=e.replace(/^\/*/,"/"),l=N.useMemo(()=>({basename:a,navigator:s,static:o}),[a,s,o]);typeof r=="string"&&(r=er(r));let{pathname:u="/",search:c="",hash:h="",state:d=null,key:y="default"}=r,_=N.useMemo(()=>{let w=js(u,a);return w==null?null:{location:{pathname:w,search:c,hash:h,state:d,key:y},navigationType:i}},[a,u,c,h,d,y,i]);return _==null?null:N.createElement(Ri.Provider,{value:l},N.createElement(Ec.Provider,{children:n,value:_}))}var Ov;(function(t){t[t.pending=0]="pending",t[t.success=1]="success",t[t.error=2]="error"})(Ov||(Ov={}));new Promise(()=>{});function rd(t,e){e===void 0&&(e=[]);let n=[];return N.Children.forEach(t,(r,i)=>{if(!N.isValidElement(r))return;let s=[...e,i];if(r.type===N.Fragment){n.push.apply(n,rd(r.props.children,s));return}r.type!==Ke&&J(!1),!r.props.index||!r.props.children||J(!1);let o={id:r.props.id||s.join("-"),caseSensitive:r.props.caseSensitive,element:r.props.element,Component:r.props.Component,index:r.props.index,path:r.props.path,loader:r.props.loader,action:r.props.action,errorElement:r.props.errorElement,ErrorBoundary:r.props.ErrorBoundary,hasErrorBoundary:r.props.ErrorBoundary!=null||r.props.errorElement!=null,shouldRevalidate:r.props.shouldRevalidate,handle:r.props.handle,lazy:r.props.lazy};r.props.children&&(o.children=rd(r.props.children,s)),n.push(o)}),n}function fA(t){let e={hasErrorBoundary:t.ErrorBoundary!=null||t.errorElement!=null};return t.Component&&Object.assign(e,{element:N.createElement(t.Component),Component:void 0}),t.ErrorBoundary&&Object.assign(e,{errorElement:N.createElement(t.ErrorBoundary),ErrorBoundary:void 0}),e}/**
 * React Router DOM v6.14.2
 *
 * Copyright (c) Remix Software Inc.
 *
 * This source code is licensed under the MIT license found in the
 * LICENSE.md file in the root directory of this source tree.
 *
 * @license MIT
 */function Ss(){return Ss=Object.assign?Object.assign.bind():function(t){for(var e=1;e<arguments.length;e++){var n=arguments[e];for(var r in n)Object.prototype.hasOwnProperty.call(n,r)&&(t[r]=n[r])}return t},Ss.apply(this,arguments)}function K_(t,e){if(t==null)return{};var n={},r=Object.keys(t),i,s;for(s=0;s<r.length;s++)i=r[s],!(e.indexOf(i)>=0)&&(n[i]=t[i]);return n}function hA(t){return!!(t.metaKey||t.altKey||t.ctrlKey||t.shiftKey)}function dA(t,e){return t.button===0&&(!e||e==="_self")&&!hA(t)}const pA=["onClick","relative","reloadDocument","replace","state","target","to","preventScrollReset"],mA=["aria-current","caseSensitive","className","end","style","to","children"];function gA(t,e){return Dk({basename:e==null?void 0:e.basename,future:Ss({},e==null?void 0:e.future,{v7_prependBasename:!0}),history:rk({window:e==null?void 0:e.window}),hydrationData:(e==null?void 0:e.hydrationData)||vA(),routes:t,mapRouteProperties:fA}).initialize()}function vA(){var t;let e=(t=window)==null?void 0:t.__staticRouterHydrationData;return e&&e.errors&&(e=Ss({},e,{errors:yA(e.errors)})),e}function yA(t){if(!t)return null;let e=Object.entries(t),n={};for(let[r,i]of e)if(i&&i.__type==="RouteErrorResponse")n[r]=new Fp(i.status,i.statusText,i.data,i.internal===!0);else if(i&&i.__type==="Error"){if(i.__subType){let s=window[i.__subType];if(typeof s=="function")try{let o=new s(i.message);o.stack="",n[r]=o}catch{}}if(n[r]==null){let s=new Error(i.message);s.stack="",n[r]=s}}else n[r]=i;return n}const _A=typeof window<"u"&&typeof window.document<"u"&&typeof window.document.createElement<"u",wA=/^(?:[a-z][a-z0-9+.-]*:|\/\/)/i,Bp=N.forwardRef(function(e,n){let{onClick:r,relative:i,reloadDocument:s,replace:o,state:a,target:l,to:u,preventScrollReset:c}=e,h=K_(e,pA),{basename:d}=N.useContext(Ri),y,_=!1;if(typeof u=="string"&&wA.test(u)&&(y=u,_A))try{let p=new URL(window.location.href),v=u.startsWith("//")?new URL(p.protocol+u):new URL(u),m=js(v.pathname,d);v.origin===p.origin&&m!=null?u=m+v.search+v.hash:_=!0}catch{}let w=Kk(u,{relative:i}),T=EA(u,{replace:o,state:a,target:l,preventScrollReset:c,relative:i});function g(p){r&&r(p),p.defaultPrevented||T(p)}return N.createElement("a",Ss({},h,{href:y||w,onClick:_||s?r:g,ref:n,target:l}))}),tn=N.forwardRef(function(e,n){let{"aria-current":r="page",caseSensitive:i=!1,className:s="",end:o=!1,style:a,to:l,children:u}=e,c=K_(e,mA),h=zp(l,{relative:c.relative}),d=xa(),y=N.useContext($p),{navigator:_}=N.useContext(Ri),w=_.encodeLocation?_.encodeLocation(h).pathname:h.pathname,T=d.pathname,g=y&&y.navigation&&y.navigation.location?y.navigation.location.pathname:null;i||(T=T.toLowerCase(),g=g?g.toLowerCase():null,w=w.toLowerCase());let p=T===w||!o&&T.startsWith(w)&&T.charAt(w.length)==="/",v=g!=null&&(g===w||!o&&g.startsWith(w)&&g.charAt(w.length)==="/"),m=p?r:void 0,S;typeof s=="function"?S=s({isActive:p,isPending:v}):S=[s,p?"active":null,v?"pending":null].filter(Boolean).join(" ");let P=typeof a=="function"?a({isActive:p,isPending:v}):a;return N.createElement(Bp,Ss({},c,{"aria-current":m,className:S,ref:n,style:P,to:l}),typeof u=="function"?u({isActive:p,isPending:v}):u)});var Dv;(function(t){t.UseScrollRestoration="useScrollRestoration",t.UseSubmit="useSubmit",t.UseSubmitFetcher="useSubmitFetcher",t.UseFetcher="useFetcher"})(Dv||(Dv={}));var Mv;(function(t){t.UseFetchers="useFetchers",t.UseScrollRestoration="useScrollRestoration"})(Mv||(Mv={}));function EA(t,e){let{target:n,replace:r,state:i,preventScrollReset:s,relative:o}=e===void 0?{}:e,a=B_(),l=xa(),u=zp(t,{relative:o});return N.useCallback(c=>{if(dA(c,n)){c.preventDefault();let h=r!==void 0?r:vi(l)===vi(u);a(t,{replace:h,state:i,preventScrollReset:s,relative:o})}},[l,a,u,r,i,n,t,s,o])}/**
 * @license
 * Copyright 2017 Google LLC
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 *//**
 * @license
 * Copyright 2017 Google LLC
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */const G_=function(t){const e=[];let n=0;for(let r=0;r<t.length;r++){let i=t.charCodeAt(r);i<128?e[n++]=i:i<2048?(e[n++]=i>>6|192,e[n++]=i&63|128):(i&64512)===55296&&r+1<t.length&&(t.charCodeAt(r+1)&64512)===56320?(i=65536+((i&1023)<<10)+(t.charCodeAt(++r)&1023),e[n++]=i>>18|240,e[n++]=i>>12&63|128,e[n++]=i>>6&63|128,e[n++]=i&63|128):(e[n++]=i>>12|224,e[n++]=i>>6&63|128,e[n++]=i&63|128)}return e},TA=function(t){const e=[];let n=0,r=0;for(;n<t.length;){const i=t[n++];if(i<128)e[r++]=String.fromCharCode(i);else if(i>191&&i<224){const s=t[n++];e[r++]=String.fromCharCode((i&31)<<6|s&63)}else if(i>239&&i<365){const s=t[n++],o=t[n++],a=t[n++],l=((i&7)<<18|(s&63)<<12|(o&63)<<6|a&63)-65536;e[r++]=String.fromCharCode(55296+(l>>10)),e[r++]=String.fromCharCode(56320+(l&1023))}else{const s=t[n++],o=t[n++];e[r++]=String.fromCharCode((i&15)<<12|(s&63)<<6|o&63)}}return e.join("")},q_={byteToCharMap_:null,charToByteMap_:null,byteToCharMapWebSafe_:null,charToByteMapWebSafe_:null,ENCODED_VALS_BASE:"ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz0123456789",get ENCODED_VALS(){return this.ENCODED_VALS_BASE+"+/="},get ENCODED_VALS_WEBSAFE(){return this.ENCODED_VALS_BASE+"-_."},HAS_NATIVE_SUPPORT:typeof atob=="function",encodeByteArray(t,e){if(!Array.isArray(t))throw Error("encodeByteArray takes an array as a parameter");this.init_();const n=e?this.byteToCharMapWebSafe_:this.byteToCharMap_,r=[];for(let i=0;i<t.length;i+=3){const s=t[i],o=i+1<t.length,a=o?t[i+1]:0,l=i+2<t.length,u=l?t[i+2]:0,c=s>>2,h=(s&3)<<4|a>>4;let d=(a&15)<<2|u>>6,y=u&63;l||(y=64,o||(d=64)),r.push(n[c],n[h],n[d],n[y])}return r.join("")},encodeString(t,e){return this.HAS_NATIVE_SUPPORT&&!e?btoa(t):this.encodeByteArray(G_(t),e)},decodeString(t,e){return this.HAS_NATIVE_SUPPORT&&!e?atob(t):TA(this.decodeStringToByteArray(t,e))},decodeStringToByteArray(t,e){this.init_();const n=e?this.charToByteMapWebSafe_:this.charToByteMap_,r=[];for(let i=0;i<t.length;){const s=n[t.charAt(i++)],a=i<t.length?n[t.charAt(i)]:0;++i;const u=i<t.length?n[t.charAt(i)]:64;++i;const h=i<t.length?n[t.charAt(i)]:64;if(++i,s==null||a==null||u==null||h==null)throw new IA;const d=s<<2|a>>4;if(r.push(d),u!==64){const y=a<<4&240|u>>2;if(r.push(y),h!==64){const _=u<<6&192|h;r.push(_)}}}return r},init_(){if(!this.byteToCharMap_){this.byteToCharMap_={},this.charToByteMap_={},this.byteToCharMapWebSafe_={},this.charToByteMapWebSafe_={};for(let t=0;t<this.ENCODED_VALS.length;t++)this.byteToCharMap_[t]=this.ENCODED_VALS.charAt(t),this.charToByteMap_[this.byteToCharMap_[t]]=t,this.byteToCharMapWebSafe_[t]=this.ENCODED_VALS_WEBSAFE.charAt(t),this.charToByteMapWebSafe_[this.byteToCharMapWebSafe_[t]]=t,t>=this.ENCODED_VALS_BASE.length&&(this.charToByteMap_[this.ENCODED_VALS_WEBSAFE.charAt(t)]=t,this.charToByteMapWebSafe_[this.ENCODED_VALS.charAt(t)]=t)}}};class IA extends Error{constructor(){super(...arguments),this.name="DecodeBase64StringError"}}const SA=function(t){const e=G_(t);return q_.encodeByteArray(e,!0)},Pu=function(t){return SA(t).replace(/\./g,"")},Q_=function(t){try{return q_.decodeString(t,!0)}catch(e){console.error("base64Decode failed: ",e)}return null};/**
 * @license
 * Copyright 2022 Google LLC
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */function kA(){if(typeof self<"u")return self;if(typeof window<"u")return window;if(typeof global<"u")return global;throw new Error("Unable to locate global object.")}/**
 * @license
 * Copyright 2022 Google LLC
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */const AA=()=>kA().__FIREBASE_DEFAULTS__,xA=()=>{if(typeof process>"u"||typeof process.env>"u")return;const t={}.__FIREBASE_DEFAULTS__;if(t)return JSON.parse(t)},PA=()=>{if(typeof document>"u")return;let t;try{t=document.cookie.match(/__FIREBASE_DEFAULTS__=([^;]+)/)}catch{return}const e=t&&Q_(t[1]);return e&&JSON.parse(e)},Hp=()=>{try{return AA()||xA()||PA()}catch(t){console.info(`Unable to get __FIREBASE_DEFAULTS__ due to: ${t}`);return}},Y_=t=>{var e,n;return(n=(e=Hp())===null||e===void 0?void 0:e.emulatorHosts)===null||n===void 0?void 0:n[t]},RA=t=>{const e=Y_(t);if(!e)return;const n=e.lastIndexOf(":");if(n<=0||n+1===e.length)throw new Error(`Invalid host ${e} with no separate hostname and port!`);const r=parseInt(e.substring(n+1),10);return e[0]==="["?[e.substring(1,n-1),r]:[e.substring(0,n),r]},X_=()=>{var t;return(t=Hp())===null||t===void 0?void 0:t.config},J_=t=>{var e;return(e=Hp())===null||e===void 0?void 0:e[`_${t}`]};/**
 * @license
 * Copyright 2017 Google LLC
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */class CA{constructor(){this.reject=()=>{},this.resolve=()=>{},this.promise=new Promise((e,n)=>{this.resolve=e,this.reject=n})}wrapCallback(e){return(n,r)=>{n?this.reject(n):this.resolve(r),typeof e=="function"&&(this.promise.catch(()=>{}),e.length===1?e(n):e(n,r))}}}/**
 * @license
 * Copyright 2021 Google LLC
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */function NA(t,e){if(t.uid)throw new Error('The "uid" field is no longer supported by mockUserToken. Please use "sub" instead for Firebase Auth User ID.');const n={alg:"none",type:"JWT"},r=e||"demo-project",i=t.iat||0,s=t.sub||t.user_id;if(!s)throw new Error("mockUserToken must contain 'sub' or 'user_id' field!");const o=Object.assign({iss:`https://securetoken.google.com/${r}`,aud:r,iat:i,exp:i+3600,auth_time:i,sub:s,user_id:s,firebase:{sign_in_provider:"custom",identities:{}}},t),a="";return[Pu(JSON.stringify(n)),Pu(JSON.stringify(o)),a].join(".")}/**
 * @license
 * Copyright 2017 Google LLC
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */function _t(){return typeof navigator<"u"&&typeof navigator.userAgent=="string"?navigator.userAgent:""}function bA(){return typeof window<"u"&&!!(window.cordova||window.phonegap||window.PhoneGap)&&/ios|iphone|ipod|ipad|android|blackberry|iemobile/i.test(_t())}function OA(){const t=typeof chrome=="object"?chrome.runtime:typeof browser=="object"?browser.runtime:void 0;return typeof t=="object"&&t.id!==void 0}function DA(){return typeof navigator=="object"&&navigator.product==="ReactNative"}function MA(){const t=_t();return t.indexOf("MSIE ")>=0||t.indexOf("Trident/")>=0}function LA(){try{return typeof indexedDB=="object"}catch{return!1}}function jA(){return new Promise((t,e)=>{try{let n=!0;const r="validate-browser-context-for-indexeddb-analytics-module",i=self.indexedDB.open(r);i.onsuccess=()=>{i.result.close(),n||self.indexedDB.deleteDatabase(r),t(!0)},i.onupgradeneeded=()=>{n=!1},i.onerror=()=>{var s;e(((s=i.error)===null||s===void 0?void 0:s.message)||"")}}catch(n){e(n)}})}/**
 * @license
 * Copyright 2017 Google LLC
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */const VA="FirebaseError";class tr extends Error{constructor(e,n,r){super(n),this.code=e,this.customData=r,this.name=VA,Object.setPrototypeOf(this,tr.prototype),Error.captureStackTrace&&Error.captureStackTrace(this,Pa.prototype.create)}}class Pa{constructor(e,n,r){this.service=e,this.serviceName=n,this.errors=r}create(e,...n){const r=n[0]||{},i=`${this.service}/${e}`,s=this.errors[e],o=s?FA(s,r):"Error",a=`${this.serviceName}: ${o} (${i}).`;return new tr(i,a,r)}}function FA(t,e){return t.replace(UA,(n,r)=>{const i=e[r];return i!=null?String(i):`<${r}?>`})}const UA=/\{\$([^}]+)}/g;function $A(t){for(const e in t)if(Object.prototype.hasOwnProperty.call(t,e))return!1;return!0}function Ru(t,e){if(t===e)return!0;const n=Object.keys(t),r=Object.keys(e);for(const i of n){if(!r.includes(i))return!1;const s=t[i],o=e[i];if(Lv(s)&&Lv(o)){if(!Ru(s,o))return!1}else if(s!==o)return!1}for(const i of r)if(!n.includes(i))return!1;return!0}function Lv(t){return t!==null&&typeof t=="object"}/**
 * @license
 * Copyright 2017 Google LLC
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */function Ra(t){const e=[];for(const[n,r]of Object.entries(t))Array.isArray(r)?r.forEach(i=>{e.push(encodeURIComponent(n)+"="+encodeURIComponent(i))}):e.push(encodeURIComponent(n)+"="+encodeURIComponent(r));return e.length?"&"+e.join("&"):""}function zA(t,e){const n=new BA(t,e);return n.subscribe.bind(n)}class BA{constructor(e,n){this.observers=[],this.unsubscribes=[],this.observerCount=0,this.task=Promise.resolve(),this.finalized=!1,this.onNoObservers=n,this.task.then(()=>{e(this)}).catch(r=>{this.error(r)})}next(e){this.forEachObserver(n=>{n.next(e)})}error(e){this.forEachObserver(n=>{n.error(e)}),this.close(e)}complete(){this.forEachObserver(e=>{e.complete()}),this.close()}subscribe(e,n,r){let i;if(e===void 0&&n===void 0&&r===void 0)throw new Error("Missing Observer.");HA(e,["next","error","complete"])?i=e:i={next:e,error:n,complete:r},i.next===void 0&&(i.next=Mf),i.error===void 0&&(i.error=Mf),i.complete===void 0&&(i.complete=Mf);const s=this.unsubscribeOne.bind(this,this.observers.length);return this.finalized&&this.task.then(()=>{try{this.finalError?i.error(this.finalError):i.complete()}catch{}}),this.observers.push(i),s}unsubscribeOne(e){this.observers===void 0||this.observers[e]===void 0||(delete this.observers[e],this.observerCount-=1,this.observerCount===0&&this.onNoObservers!==void 0&&this.onNoObservers(this))}forEachObserver(e){if(!this.finalized)for(let n=0;n<this.observers.length;n++)this.sendOne(n,e)}sendOne(e,n){this.task.then(()=>{if(this.observers!==void 0&&this.observers[e]!==void 0)try{n(this.observers[e])}catch(r){typeof console<"u"&&console.error&&console.error(r)}})}close(e){this.finalized||(this.finalized=!0,e!==void 0&&(this.finalError=e),this.task.then(()=>{this.observers=void 0,this.onNoObservers=void 0}))}}function HA(t,e){if(typeof t!="object"||t===null)return!1;for(const n of e)if(n in t&&typeof t[n]=="function")return!0;return!1}function Mf(){}/**
 * @license
 * Copyright 2021 Google LLC
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */function kn(t){return t&&t._delegate?t._delegate:t}class yi{constructor(e,n,r){this.name=e,this.instanceFactory=n,this.type=r,this.multipleInstances=!1,this.serviceProps={},this.instantiationMode="LAZY",this.onInstanceCreated=null}setInstantiationMode(e){return this.instantiationMode=e,this}setMultipleInstances(e){return this.multipleInstances=e,this}setServiceProps(e){return this.serviceProps=e,this}setInstanceCreatedCallback(e){return this.onInstanceCreated=e,this}}/**
 * @license
 * Copyright 2019 Google LLC
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */const Xr="[DEFAULT]";/**
 * @license
 * Copyright 2019 Google LLC
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */class WA{constructor(e,n){this.name=e,this.container=n,this.component=null,this.instances=new Map,this.instancesDeferred=new Map,this.instancesOptions=new Map,this.onInitCallbacks=new Map}get(e){const n=this.normalizeInstanceIdentifier(e);if(!this.instancesDeferred.has(n)){const r=new CA;if(this.instancesDeferred.set(n,r),this.isInitialized(n)||this.shouldAutoInitialize())try{const i=this.getOrInitializeService({instanceIdentifier:n});i&&r.resolve(i)}catch{}}return this.instancesDeferred.get(n).promise}getImmediate(e){var n;const r=this.normalizeInstanceIdentifier(e==null?void 0:e.identifier),i=(n=e==null?void 0:e.optional)!==null&&n!==void 0?n:!1;if(this.isInitialized(r)||this.shouldAutoInitialize())try{return this.getOrInitializeService({instanceIdentifier:r})}catch(s){if(i)return null;throw s}else{if(i)return null;throw Error(`Service ${this.name} is not available`)}}getComponent(){return this.component}setComponent(e){if(e.name!==this.name)throw Error(`Mismatching Component ${e.name} for Provider ${this.name}.`);if(this.component)throw Error(`Component for ${this.name} has already been provided`);if(this.component=e,!!this.shouldAutoInitialize()){if(GA(e))try{this.getOrInitializeService({instanceIdentifier:Xr})}catch{}for(const[n,r]of this.instancesDeferred.entries()){const i=this.normalizeInstanceIdentifier(n);try{const s=this.getOrInitializeService({instanceIdentifier:i});r.resolve(s)}catch{}}}}clearInstance(e=Xr){this.instancesDeferred.delete(e),this.instancesOptions.delete(e),this.instances.delete(e)}async delete(){const e=Array.from(this.instances.values());await Promise.all([...e.filter(n=>"INTERNAL"in n).map(n=>n.INTERNAL.delete()),...e.filter(n=>"_delete"in n).map(n=>n._delete())])}isComponentSet(){return this.component!=null}isInitialized(e=Xr){return this.instances.has(e)}getOptions(e=Xr){return this.instancesOptions.get(e)||{}}initialize(e={}){const{options:n={}}=e,r=this.normalizeInstanceIdentifier(e.instanceIdentifier);if(this.isInitialized(r))throw Error(`${this.name}(${r}) has already been initialized`);if(!this.isComponentSet())throw Error(`Component ${this.name} has not been registered yet`);const i=this.getOrInitializeService({instanceIdentifier:r,options:n});for(const[s,o]of this.instancesDeferred.entries()){const a=this.normalizeInstanceIdentifier(s);r===a&&o.resolve(i)}return i}onInit(e,n){var r;const i=this.normalizeInstanceIdentifier(n),s=(r=this.onInitCallbacks.get(i))!==null&&r!==void 0?r:new Set;s.add(e),this.onInitCallbacks.set(i,s);const o=this.instances.get(i);return o&&e(o,i),()=>{s.delete(e)}}invokeOnInitCallbacks(e,n){const r=this.onInitCallbacks.get(n);if(r)for(const i of r)try{i(e,n)}catch{}}getOrInitializeService({instanceIdentifier:e,options:n={}}){let r=this.instances.get(e);if(!r&&this.component&&(r=this.component.instanceFactory(this.container,{instanceIdentifier:KA(e),options:n}),this.instances.set(e,r),this.instancesOptions.set(e,n),this.invokeOnInitCallbacks(r,e),this.component.onInstanceCreated))try{this.component.onInstanceCreated(this.container,e,r)}catch{}return r||null}normalizeInstanceIdentifier(e=Xr){return this.component?this.component.multipleInstances?e:Xr:e}shouldAutoInitialize(){return!!this.component&&this.component.instantiationMode!=="EXPLICIT"}}function KA(t){return t===Xr?void 0:t}function GA(t){return t.instantiationMode==="EAGER"}/**
 * @license
 * Copyright 2019 Google LLC
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */class qA{constructor(e){this.name=e,this.providers=new Map}addComponent(e){const n=this.getProvider(e.name);if(n.isComponentSet())throw new Error(`Component ${e.name} has already been registered with ${this.name}`);n.setComponent(e)}addOrOverwriteComponent(e){this.getProvider(e.name).isComponentSet()&&this.providers.delete(e.name),this.addComponent(e)}getProvider(e){if(this.providers.has(e))return this.providers.get(e);const n=new WA(e,this);return this.providers.set(e,n),n}getProviders(){return Array.from(this.providers.values())}}/**
 * @license
 * Copyright 2017 Google LLC
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */var se;(function(t){t[t.DEBUG=0]="DEBUG",t[t.VERBOSE=1]="VERBOSE",t[t.INFO=2]="INFO",t[t.WARN=3]="WARN",t[t.ERROR=4]="ERROR",t[t.SILENT=5]="SILENT"})(se||(se={}));const QA={debug:se.DEBUG,verbose:se.VERBOSE,info:se.INFO,warn:se.WARN,error:se.ERROR,silent:se.SILENT},YA=se.INFO,XA={[se.DEBUG]:"log",[se.VERBOSE]:"log",[se.INFO]:"info",[se.WARN]:"warn",[se.ERROR]:"error"},JA=(t,e,...n)=>{if(e<t.logLevel)return;const r=new Date().toISOString(),i=XA[e];if(i)console[i](`[${r}]  ${t.name}:`,...n);else throw new Error(`Attempted to log a message with an invalid logType (value: ${e})`)};class Wp{constructor(e){this.name=e,this._logLevel=YA,this._logHandler=JA,this._userLogHandler=null}get logLevel(){return this._logLevel}set logLevel(e){if(!(e in se))throw new TypeError(`Invalid value "${e}" assigned to \`logLevel\``);this._logLevel=e}setLogLevel(e){this._logLevel=typeof e=="string"?QA[e]:e}get logHandler(){return this._logHandler}set logHandler(e){if(typeof e!="function")throw new TypeError("Value assigned to `logHandler` must be a function");this._logHandler=e}get userLogHandler(){return this._userLogHandler}set userLogHandler(e){this._userLogHandler=e}debug(...e){this._userLogHandler&&this._userLogHandler(this,se.DEBUG,...e),this._logHandler(this,se.DEBUG,...e)}log(...e){this._userLogHandler&&this._userLogHandler(this,se.VERBOSE,...e),this._logHandler(this,se.VERBOSE,...e)}info(...e){this._userLogHandler&&this._userLogHandler(this,se.INFO,...e),this._logHandler(this,se.INFO,...e)}warn(...e){this._userLogHandler&&this._userLogHandler(this,se.WARN,...e),this._logHandler(this,se.WARN,...e)}error(...e){this._userLogHandler&&this._userLogHandler(this,se.ERROR,...e),this._logHandler(this,se.ERROR,...e)}}const ZA=(t,e)=>e.some(n=>t instanceof n);let jv,Vv;function ex(){return jv||(jv=[IDBDatabase,IDBObjectStore,IDBIndex,IDBCursor,IDBTransaction])}function tx(){return Vv||(Vv=[IDBCursor.prototype.advance,IDBCursor.prototype.continue,IDBCursor.prototype.continuePrimaryKey])}const Z_=new WeakMap,id=new WeakMap,ew=new WeakMap,Lf=new WeakMap,Kp=new WeakMap;function nx(t){const e=new Promise((n,r)=>{const i=()=>{t.removeEventListener("success",s),t.removeEventListener("error",o)},s=()=>{n(Cr(t.result)),i()},o=()=>{r(t.error),i()};t.addEventListener("success",s),t.addEventListener("error",o)});return e.then(n=>{n instanceof IDBCursor&&Z_.set(n,t)}).catch(()=>{}),Kp.set(e,t),e}function rx(t){if(id.has(t))return;const e=new Promise((n,r)=>{const i=()=>{t.removeEventListener("complete",s),t.removeEventListener("error",o),t.removeEventListener("abort",o)},s=()=>{n(),i()},o=()=>{r(t.error||new DOMException("AbortError","AbortError")),i()};t.addEventListener("complete",s),t.addEventListener("error",o),t.addEventListener("abort",o)});id.set(t,e)}let sd={get(t,e,n){if(t instanceof IDBTransaction){if(e==="done")return id.get(t);if(e==="objectStoreNames")return t.objectStoreNames||ew.get(t);if(e==="store")return n.objectStoreNames[1]?void 0:n.objectStore(n.objectStoreNames[0])}return Cr(t[e])},set(t,e,n){return t[e]=n,!0},has(t,e){return t instanceof IDBTransaction&&(e==="done"||e==="store")?!0:e in t}};function ix(t){sd=t(sd)}function sx(t){return t===IDBDatabase.prototype.transaction&&!("objectStoreNames"in IDBTransaction.prototype)?function(e,...n){const r=t.call(jf(this),e,...n);return ew.set(r,e.sort?e.sort():[e]),Cr(r)}:tx().includes(t)?function(...e){return t.apply(jf(this),e),Cr(Z_.get(this))}:function(...e){return Cr(t.apply(jf(this),e))}}function ox(t){return typeof t=="function"?sx(t):(t instanceof IDBTransaction&&rx(t),ZA(t,ex())?new Proxy(t,sd):t)}function Cr(t){if(t instanceof IDBRequest)return nx(t);if(Lf.has(t))return Lf.get(t);const e=ox(t);return e!==t&&(Lf.set(t,e),Kp.set(e,t)),e}const jf=t=>Kp.get(t);function ax(t,e,{blocked:n,upgrade:r,blocking:i,terminated:s}={}){const o=indexedDB.open(t,e),a=Cr(o);return r&&o.addEventListener("upgradeneeded",l=>{r(Cr(o.result),l.oldVersion,l.newVersion,Cr(o.transaction),l)}),n&&o.addEventListener("blocked",l=>n(l.oldVersion,l.newVersion,l)),a.then(l=>{s&&l.addEventListener("close",()=>s()),i&&l.addEventListener("versionchange",u=>i(u.oldVersion,u.newVersion,u))}).catch(()=>{}),a}const lx=["get","getKey","getAll","getAllKeys","count"],ux=["put","add","delete","clear"],Vf=new Map;function Fv(t,e){if(!(t instanceof IDBDatabase&&!(e in t)&&typeof e=="string"))return;if(Vf.get(e))return Vf.get(e);const n=e.replace(/FromIndex$/,""),r=e!==n,i=ux.includes(n);if(!(n in(r?IDBIndex:IDBObjectStore).prototype)||!(i||lx.includes(n)))return;const s=async function(o,...a){const l=this.transaction(o,i?"readwrite":"readonly");let u=l.store;return r&&(u=u.index(a.shift())),(await Promise.all([u[n](...a),i&&l.done]))[0]};return Vf.set(e,s),s}ix(t=>({...t,get:(e,n,r)=>Fv(e,n)||t.get(e,n,r),has:(e,n)=>!!Fv(e,n)||t.has(e,n)}));/**
 * @license
 * Copyright 2019 Google LLC
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */class cx{constructor(e){this.container=e}getPlatformInfoString(){return this.container.getProviders().map(n=>{if(fx(n)){const r=n.getImmediate();return`${r.library}/${r.version}`}else return null}).filter(n=>n).join(" ")}}function fx(t){const e=t.getComponent();return(e==null?void 0:e.type)==="VERSION"}const od="@firebase/app",Uv="0.9.15";/**
 * @license
 * Copyright 2019 Google LLC
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */const _i=new Wp("@firebase/app"),hx="@firebase/app-compat",dx="@firebase/analytics-compat",px="@firebase/analytics",mx="@firebase/app-check-compat",gx="@firebase/app-check",vx="@firebase/auth",yx="@firebase/auth-compat",_x="@firebase/database",wx="@firebase/database-compat",Ex="@firebase/functions",Tx="@firebase/functions-compat",Ix="@firebase/installations",Sx="@firebase/installations-compat",kx="@firebase/messaging",Ax="@firebase/messaging-compat",xx="@firebase/performance",Px="@firebase/performance-compat",Rx="@firebase/remote-config",Cx="@firebase/remote-config-compat",Nx="@firebase/storage",bx="@firebase/storage-compat",Ox="@firebase/firestore",Dx="@firebase/firestore-compat",Mx="firebase",Lx="10.1.0";/**
 * @license
 * Copyright 2019 Google LLC
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */const ad="[DEFAULT]",jx={[od]:"fire-core",[hx]:"fire-core-compat",[px]:"fire-analytics",[dx]:"fire-analytics-compat",[gx]:"fire-app-check",[mx]:"fire-app-check-compat",[vx]:"fire-auth",[yx]:"fire-auth-compat",[_x]:"fire-rtdb",[wx]:"fire-rtdb-compat",[Ex]:"fire-fn",[Tx]:"fire-fn-compat",[Ix]:"fire-iid",[Sx]:"fire-iid-compat",[kx]:"fire-fcm",[Ax]:"fire-fcm-compat",[xx]:"fire-perf",[Px]:"fire-perf-compat",[Rx]:"fire-rc",[Cx]:"fire-rc-compat",[Nx]:"fire-gcs",[bx]:"fire-gcs-compat",[Ox]:"fire-fst",[Dx]:"fire-fst-compat","fire-js":"fire-js",[Mx]:"fire-js-all"};/**
 * @license
 * Copyright 2019 Google LLC
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */const Cu=new Map,ld=new Map;function Vx(t,e){try{t.container.addComponent(e)}catch(n){_i.debug(`Component ${e.name} failed to register with FirebaseApp ${t.name}`,n)}}function ks(t){const e=t.name;if(ld.has(e))return _i.debug(`There were multiple attempts to register component ${e}.`),!1;ld.set(e,t);for(const n of Cu.values())Vx(n,t);return!0}function Gp(t,e){const n=t.container.getProvider("heartbeat").getImmediate({optional:!0});return n&&n.triggerHeartbeat(),t.container.getProvider(e)}/**
 * @license
 * Copyright 2019 Google LLC
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */const Fx={"no-app":"No Firebase App '{$appName}' has been created - call initializeApp() first","bad-app-name":"Illegal App name: '{$appName}","duplicate-app":"Firebase App named '{$appName}' already exists with different options or config","app-deleted":"Firebase App named '{$appName}' already deleted","no-options":"Need to provide options, when not being deployed to hosting via source.","invalid-app-argument":"firebase.{$appName}() takes either no argument or a Firebase App instance.","invalid-log-argument":"First argument to `onLog` must be null or a function.","idb-open":"Error thrown when opening IndexedDB. Original error: {$originalErrorMessage}.","idb-get":"Error thrown when reading from IndexedDB. Original error: {$originalErrorMessage}.","idb-set":"Error thrown when writing to IndexedDB. Original error: {$originalErrorMessage}.","idb-delete":"Error thrown when deleting from IndexedDB. Original error: {$originalErrorMessage}."},Nr=new Pa("app","Firebase",Fx);/**
 * @license
 * Copyright 2019 Google LLC
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */class Ux{constructor(e,n,r){this._isDeleted=!1,this._options=Object.assign({},e),this._config=Object.assign({},n),this._name=n.name,this._automaticDataCollectionEnabled=n.automaticDataCollectionEnabled,this._container=r,this.container.addComponent(new yi("app",()=>this,"PUBLIC"))}get automaticDataCollectionEnabled(){return this.checkDestroyed(),this._automaticDataCollectionEnabled}set automaticDataCollectionEnabled(e){this.checkDestroyed(),this._automaticDataCollectionEnabled=e}get name(){return this.checkDestroyed(),this._name}get options(){return this.checkDestroyed(),this._options}get config(){return this.checkDestroyed(),this._config}get container(){return this._container}get isDeleted(){return this._isDeleted}set isDeleted(e){this._isDeleted=e}checkDestroyed(){if(this.isDeleted)throw Nr.create("app-deleted",{appName:this._name})}}/**
 * @license
 * Copyright 2019 Google LLC
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */const Vs=Lx;function tw(t,e={}){let n=t;typeof e!="object"&&(e={name:e});const r=Object.assign({name:ad,automaticDataCollectionEnabled:!1},e),i=r.name;if(typeof i!="string"||!i)throw Nr.create("bad-app-name",{appName:String(i)});if(n||(n=X_()),!n)throw Nr.create("no-options");const s=Cu.get(i);if(s){if(Ru(n,s.options)&&Ru(r,s.config))return s;throw Nr.create("duplicate-app",{appName:i})}const o=new qA(i);for(const l of ld.values())o.addComponent(l);const a=new Ux(n,r,o);return Cu.set(i,a),a}function nw(t=ad){const e=Cu.get(t);if(!e&&t===ad&&X_())return tw();if(!e)throw Nr.create("no-app",{appName:t});return e}function br(t,e,n){var r;let i=(r=jx[t])!==null&&r!==void 0?r:t;n&&(i+=`-${n}`);const s=i.match(/\s|\//),o=e.match(/\s|\//);if(s||o){const a=[`Unable to register library "${i}" with version "${e}":`];s&&a.push(`library name "${i}" contains illegal characters (whitespace or "/")`),s&&o&&a.push("and"),o&&a.push(`version name "${e}" contains illegal characters (whitespace or "/")`),_i.warn(a.join(" "));return}ks(new yi(`${i}-version`,()=>({library:i,version:e}),"VERSION"))}/**
 * @license
 * Copyright 2021 Google LLC
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */const $x="firebase-heartbeat-database",zx=1,Jo="firebase-heartbeat-store";let Ff=null;function rw(){return Ff||(Ff=ax($x,zx,{upgrade:(t,e)=>{switch(e){case 0:t.createObjectStore(Jo)}}}).catch(t=>{throw Nr.create("idb-open",{originalErrorMessage:t.message})})),Ff}async function Bx(t){try{return await(await rw()).transaction(Jo).objectStore(Jo).get(iw(t))}catch(e){if(e instanceof tr)_i.warn(e.message);else{const n=Nr.create("idb-get",{originalErrorMessage:e==null?void 0:e.message});_i.warn(n.message)}}}async function $v(t,e){try{const r=(await rw()).transaction(Jo,"readwrite");await r.objectStore(Jo).put(e,iw(t)),await r.done}catch(n){if(n instanceof tr)_i.warn(n.message);else{const r=Nr.create("idb-set",{originalErrorMessage:n==null?void 0:n.message});_i.warn(r.message)}}}function iw(t){return`${t.name}!${t.options.appId}`}/**
 * @license
 * Copyright 2021 Google LLC
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */const Hx=1024,Wx=30*24*60*60*1e3;class Kx{constructor(e){this.container=e,this._heartbeatsCache=null;const n=this.container.getProvider("app").getImmediate();this._storage=new qx(n),this._heartbeatsCachePromise=this._storage.read().then(r=>(this._heartbeatsCache=r,r))}async triggerHeartbeat(){const n=this.container.getProvider("platform-logger").getImmediate().getPlatformInfoString(),r=zv();if(this._heartbeatsCache===null&&(this._heartbeatsCache=await this._heartbeatsCachePromise),!(this._heartbeatsCache.lastSentHeartbeatDate===r||this._heartbeatsCache.heartbeats.some(i=>i.date===r)))return this._heartbeatsCache.heartbeats.push({date:r,agent:n}),this._heartbeatsCache.heartbeats=this._heartbeatsCache.heartbeats.filter(i=>{const s=new Date(i.date).valueOf();return Date.now()-s<=Wx}),this._storage.overwrite(this._heartbeatsCache)}async getHeartbeatsHeader(){if(this._heartbeatsCache===null&&await this._heartbeatsCachePromise,this._heartbeatsCache===null||this._heartbeatsCache.heartbeats.length===0)return"";const e=zv(),{heartbeatsToSend:n,unsentEntries:r}=Gx(this._heartbeatsCache.heartbeats),i=Pu(JSON.stringify({version:2,heartbeats:n}));return this._heartbeatsCache.lastSentHeartbeatDate=e,r.length>0?(this._heartbeatsCache.heartbeats=r,await this._storage.overwrite(this._heartbeatsCache)):(this._heartbeatsCache.heartbeats=[],this._storage.overwrite(this._heartbeatsCache)),i}}function zv(){return new Date().toISOString().substring(0,10)}function Gx(t,e=Hx){const n=[];let r=t.slice();for(const i of t){const s=n.find(o=>o.agent===i.agent);if(s){if(s.dates.push(i.date),Bv(n)>e){s.dates.pop();break}}else if(n.push({agent:i.agent,dates:[i.date]}),Bv(n)>e){n.pop();break}r=r.slice(1)}return{heartbeatsToSend:n,unsentEntries:r}}class qx{constructor(e){this.app=e,this._canUseIndexedDBPromise=this.runIndexedDBEnvironmentCheck()}async runIndexedDBEnvironmentCheck(){return LA()?jA().then(()=>!0).catch(()=>!1):!1}async read(){return await this._canUseIndexedDBPromise?await Bx(this.app)||{heartbeats:[]}:{heartbeats:[]}}async overwrite(e){var n;if(await this._canUseIndexedDBPromise){const i=await this.read();return $v(this.app,{lastSentHeartbeatDate:(n=e.lastSentHeartbeatDate)!==null&&n!==void 0?n:i.lastSentHeartbeatDate,heartbeats:e.heartbeats})}else return}async add(e){var n;if(await this._canUseIndexedDBPromise){const i=await this.read();return $v(this.app,{lastSentHeartbeatDate:(n=e.lastSentHeartbeatDate)!==null&&n!==void 0?n:i.lastSentHeartbeatDate,heartbeats:[...i.heartbeats,...e.heartbeats]})}else return}}function Bv(t){return Pu(JSON.stringify({version:2,heartbeats:t})).length}/**
 * @license
 * Copyright 2019 Google LLC
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */function Qx(t){ks(new yi("platform-logger",e=>new cx(e),"PRIVATE")),ks(new yi("heartbeat",e=>new Kx(e),"PRIVATE")),br(od,Uv,t),br(od,Uv,"esm2017"),br("fire-js","")}Qx("");function qp(t,e){var n={};for(var r in t)Object.prototype.hasOwnProperty.call(t,r)&&e.indexOf(r)<0&&(n[r]=t[r]);if(t!=null&&typeof Object.getOwnPropertySymbols=="function")for(var i=0,r=Object.getOwnPropertySymbols(t);i<r.length;i++)e.indexOf(r[i])<0&&Object.prototype.propertyIsEnumerable.call(t,r[i])&&(n[r[i]]=t[r[i]]);return n}function sw(){return{"dependent-sdk-initialized-before-auth":"Another Firebase SDK was initialized and is trying to use Auth before Auth is initialized. Please be sure to call `initializeAuth` or `getAuth` before starting any other Firebase SDK."}}const Yx=sw,ow=new Pa("auth","Firebase",sw());/**
 * @license
 * Copyright 2020 Google LLC
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */const Nu=new Wp("@firebase/auth");function Xx(t,...e){Nu.logLevel<=se.WARN&&Nu.warn(`Auth (${Vs}): ${t}`,...e)}function Ql(t,...e){Nu.logLevel<=se.ERROR&&Nu.error(`Auth (${Vs}): ${t}`,...e)}/**
 * @license
 * Copyright 2020 Google LLC
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */function Kn(t,...e){throw Qp(t,...e)}function In(t,...e){return Qp(t,...e)}function Jx(t,e,n){const r=Object.assign(Object.assign({},Yx()),{[e]:n});return new Pa("auth","Firebase",r).create(e,{appName:t.name})}function Qp(t,...e){if(typeof t!="string"){const n=e[0],r=[...e.slice(1)];return r[0]&&(r[0].appName=t.name),t._errorFactory.create(n,...r)}return ow.create(t,...e)}function Q(t,e,...n){if(!t)throw Qp(e,...n)}function Mn(t){const e="INTERNAL ASSERTION FAILED: "+t;throw Ql(e),new Error(e)}function Gn(t,e){t||Mn(e)}/**
 * @license
 * Copyright 2020 Google LLC
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */function ud(){var t;return typeof self<"u"&&((t=self.location)===null||t===void 0?void 0:t.href)||""}function Zx(){return Hv()==="http:"||Hv()==="https:"}function Hv(){var t;return typeof self<"u"&&((t=self.location)===null||t===void 0?void 0:t.protocol)||null}/**
 * @license
 * Copyright 2020 Google LLC
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */function eP(){return typeof navigator<"u"&&navigator&&"onLine"in navigator&&typeof navigator.onLine=="boolean"&&(Zx()||OA()||"connection"in navigator)?navigator.onLine:!0}function tP(){if(typeof navigator>"u")return null;const t=navigator;return t.languages&&t.languages[0]||t.language||null}/**
 * @license
 * Copyright 2020 Google LLC
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */class Ca{constructor(e,n){this.shortDelay=e,this.longDelay=n,Gn(n>e,"Short delay should be less than long delay!"),this.isMobile=bA()||DA()}get(){return eP()?this.isMobile?this.longDelay:this.shortDelay:Math.min(5e3,this.shortDelay)}}/**
 * @license
 * Copyright 2020 Google LLC
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */function Yp(t,e){Gn(t.emulator,"Emulator should always be set here");const{url:n}=t.emulator;return e?`${n}${e.startsWith("/")?e.slice(1):e}`:n}/**
 * @license
 * Copyright 2020 Google LLC
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */class aw{static initialize(e,n,r){this.fetchImpl=e,n&&(this.headersImpl=n),r&&(this.responseImpl=r)}static fetch(){if(this.fetchImpl)return this.fetchImpl;if(typeof self<"u"&&"fetch"in self)return self.fetch;Mn("Could not find fetch implementation, make sure you call FetchProvider.initialize() with an appropriate polyfill")}static headers(){if(this.headersImpl)return this.headersImpl;if(typeof self<"u"&&"Headers"in self)return self.Headers;Mn("Could not find Headers implementation, make sure you call FetchProvider.initialize() with an appropriate polyfill")}static response(){if(this.responseImpl)return this.responseImpl;if(typeof self<"u"&&"Response"in self)return self.Response;Mn("Could not find Response implementation, make sure you call FetchProvider.initialize() with an appropriate polyfill")}}/**
 * @license
 * Copyright 2020 Google LLC
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */const nP={CREDENTIAL_MISMATCH:"custom-token-mismatch",MISSING_CUSTOM_TOKEN:"internal-error",INVALID_IDENTIFIER:"invalid-email",MISSING_CONTINUE_URI:"internal-error",INVALID_PASSWORD:"wrong-password",MISSING_PASSWORD:"missing-password",EMAIL_EXISTS:"email-already-in-use",PASSWORD_LOGIN_DISABLED:"operation-not-allowed",INVALID_IDP_RESPONSE:"invalid-credential",INVALID_PENDING_TOKEN:"invalid-credential",FEDERATED_USER_ID_ALREADY_LINKED:"credential-already-in-use",MISSING_REQ_TYPE:"internal-error",EMAIL_NOT_FOUND:"user-not-found",RESET_PASSWORD_EXCEED_LIMIT:"too-many-requests",EXPIRED_OOB_CODE:"expired-action-code",INVALID_OOB_CODE:"invalid-action-code",MISSING_OOB_CODE:"internal-error",CREDENTIAL_TOO_OLD_LOGIN_AGAIN:"requires-recent-login",INVALID_ID_TOKEN:"invalid-user-token",TOKEN_EXPIRED:"user-token-expired",USER_NOT_FOUND:"user-token-expired",TOO_MANY_ATTEMPTS_TRY_LATER:"too-many-requests",INVALID_CODE:"invalid-verification-code",INVALID_SESSION_INFO:"invalid-verification-id",INVALID_TEMPORARY_PROOF:"invalid-credential",MISSING_SESSION_INFO:"missing-verification-id",SESSION_EXPIRED:"code-expired",MISSING_ANDROID_PACKAGE_NAME:"missing-android-pkg-name",UNAUTHORIZED_DOMAIN:"unauthorized-continue-uri",INVALID_OAUTH_CLIENT_ID:"invalid-oauth-client-id",ADMIN_ONLY_OPERATION:"admin-restricted-operation",INVALID_MFA_PENDING_CREDENTIAL:"invalid-multi-factor-session",MFA_ENROLLMENT_NOT_FOUND:"multi-factor-info-not-found",MISSING_MFA_ENROLLMENT_ID:"missing-multi-factor-info",MISSING_MFA_PENDING_CREDENTIAL:"missing-multi-factor-session",SECOND_FACTOR_EXISTS:"second-factor-already-in-use",SECOND_FACTOR_LIMIT_EXCEEDED:"maximum-second-factor-count-exceeded",BLOCKING_FUNCTION_ERROR_RESPONSE:"internal-error",RECAPTCHA_NOT_ENABLED:"recaptcha-not-enabled",MISSING_RECAPTCHA_TOKEN:"missing-recaptcha-token",INVALID_RECAPTCHA_TOKEN:"invalid-recaptcha-token",INVALID_RECAPTCHA_ACTION:"invalid-recaptcha-action",MISSING_CLIENT_TYPE:"missing-client-type",MISSING_RECAPTCHA_VERSION:"missing-recaptcha-version",INVALID_RECAPTCHA_VERSION:"invalid-recaptcha-version",INVALID_REQ_TYPE:"invalid-req-type"};/**
 * @license
 * Copyright 2020 Google LLC
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */const rP=new Ca(3e4,6e4);function Xp(t,e){return t.tenantId&&!e.tenantId?Object.assign(Object.assign({},e),{tenantId:t.tenantId}):e}async function Na(t,e,n,r,i={}){return lw(t,i,async()=>{let s={},o={};r&&(e==="GET"?o=r:s={body:JSON.stringify(r)});const a=Ra(Object.assign({key:t.config.apiKey},o)).slice(1),l=await t._getAdditionalHeaders();return l["Content-Type"]="application/json",t.languageCode&&(l["X-Firebase-Locale"]=t.languageCode),aw.fetch()(cw(t,t.config.apiHost,n,a),Object.assign({method:e,headers:l,referrerPolicy:"no-referrer"},s))})}async function lw(t,e,n){t._canInitEmulator=!1;const r=Object.assign(Object.assign({},nP),e);try{const i=new iP(t),s=await Promise.race([n(),i.promise]);i.clearNetworkTimeout();const o=await s.json();if("needConfirmation"in o)throw yl(t,"account-exists-with-different-credential",o);if(s.ok&&!("errorMessage"in o))return o;{const a=s.ok?o.errorMessage:o.error.message,[l,u]=a.split(" : ");if(l==="FEDERATED_USER_ID_ALREADY_LINKED")throw yl(t,"credential-already-in-use",o);if(l==="EMAIL_EXISTS")throw yl(t,"email-already-in-use",o);if(l==="USER_DISABLED")throw yl(t,"user-disabled",o);const c=r[l]||l.toLowerCase().replace(/[_\s]+/g,"-");if(u)throw Jx(t,c,u);Kn(t,c)}}catch(i){if(i instanceof tr)throw i;Kn(t,"network-request-failed",{message:String(i)})}}async function uw(t,e,n,r,i={}){const s=await Na(t,e,n,r,i);return"mfaPendingCredential"in s&&Kn(t,"multi-factor-auth-required",{_serverResponse:s}),s}function cw(t,e,n,r){const i=`${e}${n}?${r}`;return t.config.emulator?Yp(t.config,i):`${t.config.apiScheme}://${i}`}class iP{constructor(e){this.auth=e,this.timer=null,this.promise=new Promise((n,r)=>{this.timer=setTimeout(()=>r(In(this.auth,"network-request-failed")),rP.get())})}clearNetworkTimeout(){clearTimeout(this.timer)}}function yl(t,e,n){const r={appName:t.name};n.email&&(r.email=n.email),n.phoneNumber&&(r.phoneNumber=n.phoneNumber);const i=In(t,e,r);return i.customData._tokenResponse=n,i}/**
 * @license
 * Copyright 2020 Google LLC
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */async function sP(t,e){return Na(t,"POST","/v1/accounts:delete",e)}async function oP(t,e){return Na(t,"POST","/v1/accounts:lookup",e)}/**
 * @license
 * Copyright 2020 Google LLC
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */function ko(t){if(t)try{const e=new Date(Number(t));if(!isNaN(e.getTime()))return e.toUTCString()}catch{}}async function aP(t,e=!1){const n=kn(t),r=await n.getIdToken(e),i=Jp(r);Q(i&&i.exp&&i.auth_time&&i.iat,n.auth,"internal-error");const s=typeof i.firebase=="object"?i.firebase:void 0,o=s==null?void 0:s.sign_in_provider;return{claims:i,token:r,authTime:ko(Uf(i.auth_time)),issuedAtTime:ko(Uf(i.iat)),expirationTime:ko(Uf(i.exp)),signInProvider:o||null,signInSecondFactor:(s==null?void 0:s.sign_in_second_factor)||null}}function Uf(t){return Number(t)*1e3}function Jp(t){const[e,n,r]=t.split(".");if(e===void 0||n===void 0||r===void 0)return Ql("JWT malformed, contained fewer than 3 sections"),null;try{const i=Q_(n);return i?JSON.parse(i):(Ql("Failed to decode base64 JWT payload"),null)}catch(i){return Ql("Caught error parsing JWT payload as JSON",i==null?void 0:i.toString()),null}}function lP(t){const e=Jp(t);return Q(e,"internal-error"),Q(typeof e.exp<"u","internal-error"),Q(typeof e.iat<"u","internal-error"),Number(e.exp)-Number(e.iat)}/**
 * @license
 * Copyright 2020 Google LLC
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */async function Zo(t,e,n=!1){if(n)return e;try{return await e}catch(r){throw r instanceof tr&&uP(r)&&t.auth.currentUser===t&&await t.auth.signOut(),r}}function uP({code:t}){return t==="auth/user-disabled"||t==="auth/user-token-expired"}/**
 * @license
 * Copyright 2020 Google LLC
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */class cP{constructor(e){this.user=e,this.isRunning=!1,this.timerId=null,this.errorBackoff=3e4}_start(){this.isRunning||(this.isRunning=!0,this.schedule())}_stop(){this.isRunning&&(this.isRunning=!1,this.timerId!==null&&clearTimeout(this.timerId))}getInterval(e){var n;if(e){const r=this.errorBackoff;return this.errorBackoff=Math.min(this.errorBackoff*2,96e4),r}else{this.errorBackoff=3e4;const i=((n=this.user.stsTokenManager.expirationTime)!==null&&n!==void 0?n:0)-Date.now()-3e5;return Math.max(0,i)}}schedule(e=!1){if(!this.isRunning)return;const n=this.getInterval(e);this.timerId=setTimeout(async()=>{await this.iteration()},n)}async iteration(){try{await this.user.getIdToken(!0)}catch(e){(e==null?void 0:e.code)==="auth/network-request-failed"&&this.schedule(!0);return}this.schedule()}}/**
 * @license
 * Copyright 2020 Google LLC
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */class fw{constructor(e,n){this.createdAt=e,this.lastLoginAt=n,this._initializeTime()}_initializeTime(){this.lastSignInTime=ko(this.lastLoginAt),this.creationTime=ko(this.createdAt)}_copy(e){this.createdAt=e.createdAt,this.lastLoginAt=e.lastLoginAt,this._initializeTime()}toJSON(){return{createdAt:this.createdAt,lastLoginAt:this.lastLoginAt}}}/**
 * @license
 * Copyright 2019 Google LLC
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */async function bu(t){var e;const n=t.auth,r=await t.getIdToken(),i=await Zo(t,oP(n,{idToken:r}));Q(i==null?void 0:i.users.length,n,"internal-error");const s=i.users[0];t._notifyReloadListener(s);const o=!((e=s.providerUserInfo)===null||e===void 0)&&e.length?dP(s.providerUserInfo):[],a=hP(t.providerData,o),l=t.isAnonymous,u=!(t.email&&s.passwordHash)&&!(a!=null&&a.length),c=l?u:!1,h={uid:s.localId,displayName:s.displayName||null,photoURL:s.photoUrl||null,email:s.email||null,emailVerified:s.emailVerified||!1,phoneNumber:s.phoneNumber||null,tenantId:s.tenantId||null,providerData:a,metadata:new fw(s.createdAt,s.lastLoginAt),isAnonymous:c};Object.assign(t,h)}async function fP(t){const e=kn(t);await bu(e),await e.auth._persistUserIfCurrent(e),e.auth._notifyListenersIfCurrent(e)}function hP(t,e){return[...t.filter(r=>!e.some(i=>i.providerId===r.providerId)),...e]}function dP(t){return t.map(e=>{var{providerId:n}=e,r=qp(e,["providerId"]);return{providerId:n,uid:r.rawId||"",displayName:r.displayName||null,email:r.email||null,phoneNumber:r.phoneNumber||null,photoURL:r.photoUrl||null}})}/**
 * @license
 * Copyright 2020 Google LLC
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */async function pP(t,e){const n=await lw(t,{},async()=>{const r=Ra({grant_type:"refresh_token",refresh_token:e}).slice(1),{tokenApiHost:i,apiKey:s}=t.config,o=cw(t,i,"/v1/token",`key=${s}`),a=await t._getAdditionalHeaders();return a["Content-Type"]="application/x-www-form-urlencoded",aw.fetch()(o,{method:"POST",headers:a,body:r})});return{accessToken:n.access_token,expiresIn:n.expires_in,refreshToken:n.refresh_token}}/**
 * @license
 * Copyright 2020 Google LLC
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */class ea{constructor(){this.refreshToken=null,this.accessToken=null,this.expirationTime=null}get isExpired(){return!this.expirationTime||Date.now()>this.expirationTime-3e4}updateFromServerResponse(e){Q(e.idToken,"internal-error"),Q(typeof e.idToken<"u","internal-error"),Q(typeof e.refreshToken<"u","internal-error");const n="expiresIn"in e&&typeof e.expiresIn<"u"?Number(e.expiresIn):lP(e.idToken);this.updateTokensAndExpiration(e.idToken,e.refreshToken,n)}async getToken(e,n=!1){return Q(!this.accessToken||this.refreshToken,e,"user-token-expired"),!n&&this.accessToken&&!this.isExpired?this.accessToken:this.refreshToken?(await this.refresh(e,this.refreshToken),this.accessToken):null}clearRefreshToken(){this.refreshToken=null}async refresh(e,n){const{accessToken:r,refreshToken:i,expiresIn:s}=await pP(e,n);this.updateTokensAndExpiration(r,i,Number(s))}updateTokensAndExpiration(e,n,r){this.refreshToken=n||null,this.accessToken=e||null,this.expirationTime=Date.now()+r*1e3}static fromJSON(e,n){const{refreshToken:r,accessToken:i,expirationTime:s}=n,o=new ea;return r&&(Q(typeof r=="string","internal-error",{appName:e}),o.refreshToken=r),i&&(Q(typeof i=="string","internal-error",{appName:e}),o.accessToken=i),s&&(Q(typeof s=="number","internal-error",{appName:e}),o.expirationTime=s),o}toJSON(){return{refreshToken:this.refreshToken,accessToken:this.accessToken,expirationTime:this.expirationTime}}_assign(e){this.accessToken=e.accessToken,this.refreshToken=e.refreshToken,this.expirationTime=e.expirationTime}_clone(){return Object.assign(new ea,this.toJSON())}_performRefresh(){return Mn("not implemented")}}/**
 * @license
 * Copyright 2020 Google LLC
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */function ur(t,e){Q(typeof t=="string"||typeof t>"u","internal-error",{appName:e})}class ui{constructor(e){var{uid:n,auth:r,stsTokenManager:i}=e,s=qp(e,["uid","auth","stsTokenManager"]);this.providerId="firebase",this.proactiveRefresh=new cP(this),this.reloadUserInfo=null,this.reloadListener=null,this.uid=n,this.auth=r,this.stsTokenManager=i,this.accessToken=i.accessToken,this.displayName=s.displayName||null,this.email=s.email||null,this.emailVerified=s.emailVerified||!1,this.phoneNumber=s.phoneNumber||null,this.photoURL=s.photoURL||null,this.isAnonymous=s.isAnonymous||!1,this.tenantId=s.tenantId||null,this.providerData=s.providerData?[...s.providerData]:[],this.metadata=new fw(s.createdAt||void 0,s.lastLoginAt||void 0)}async getIdToken(e){const n=await Zo(this,this.stsTokenManager.getToken(this.auth,e));return Q(n,this.auth,"internal-error"),this.accessToken!==n&&(this.accessToken=n,await this.auth._persistUserIfCurrent(this),this.auth._notifyListenersIfCurrent(this)),n}getIdTokenResult(e){return aP(this,e)}reload(){return fP(this)}_assign(e){this!==e&&(Q(this.uid===e.uid,this.auth,"internal-error"),this.displayName=e.displayName,this.photoURL=e.photoURL,this.email=e.email,this.emailVerified=e.emailVerified,this.phoneNumber=e.phoneNumber,this.isAnonymous=e.isAnonymous,this.tenantId=e.tenantId,this.providerData=e.providerData.map(n=>Object.assign({},n)),this.metadata._copy(e.metadata),this.stsTokenManager._assign(e.stsTokenManager))}_clone(e){const n=new ui(Object.assign(Object.assign({},this),{auth:e,stsTokenManager:this.stsTokenManager._clone()}));return n.metadata._copy(this.metadata),n}_onReload(e){Q(!this.reloadListener,this.auth,"internal-error"),this.reloadListener=e,this.reloadUserInfo&&(this._notifyReloadListener(this.reloadUserInfo),this.reloadUserInfo=null)}_notifyReloadListener(e){this.reloadListener?this.reloadListener(e):this.reloadUserInfo=e}_startProactiveRefresh(){this.proactiveRefresh._start()}_stopProactiveRefresh(){this.proactiveRefresh._stop()}async _updateTokensIfNecessary(e,n=!1){let r=!1;e.idToken&&e.idToken!==this.stsTokenManager.accessToken&&(this.stsTokenManager.updateFromServerResponse(e),r=!0),n&&await bu(this),await this.auth._persistUserIfCurrent(this),r&&this.auth._notifyListenersIfCurrent(this)}async delete(){const e=await this.getIdToken();return await Zo(this,sP(this.auth,{idToken:e})),this.stsTokenManager.clearRefreshToken(),this.auth.signOut()}toJSON(){return Object.assign(Object.assign({uid:this.uid,email:this.email||void 0,emailVerified:this.emailVerified,displayName:this.displayName||void 0,isAnonymous:this.isAnonymous,photoURL:this.photoURL||void 0,phoneNumber:this.phoneNumber||void 0,tenantId:this.tenantId||void 0,providerData:this.providerData.map(e=>Object.assign({},e)),stsTokenManager:this.stsTokenManager.toJSON(),_redirectEventId:this._redirectEventId},this.metadata.toJSON()),{apiKey:this.auth.config.apiKey,appName:this.auth.name})}get refreshToken(){return this.stsTokenManager.refreshToken||""}static _fromJSON(e,n){var r,i,s,o,a,l,u,c;const h=(r=n.displayName)!==null&&r!==void 0?r:void 0,d=(i=n.email)!==null&&i!==void 0?i:void 0,y=(s=n.phoneNumber)!==null&&s!==void 0?s:void 0,_=(o=n.photoURL)!==null&&o!==void 0?o:void 0,w=(a=n.tenantId)!==null&&a!==void 0?a:void 0,T=(l=n._redirectEventId)!==null&&l!==void 0?l:void 0,g=(u=n.createdAt)!==null&&u!==void 0?u:void 0,p=(c=n.lastLoginAt)!==null&&c!==void 0?c:void 0,{uid:v,emailVerified:m,isAnonymous:S,providerData:P,stsTokenManager:A}=n;Q(v&&A,e,"internal-error");const O=ea.fromJSON(this.name,A);Q(typeof v=="string",e,"internal-error"),ur(h,e.name),ur(d,e.name),Q(typeof m=="boolean",e,"internal-error"),Q(typeof S=="boolean",e,"internal-error"),ur(y,e.name),ur(_,e.name),ur(w,e.name),ur(T,e.name),ur(g,e.name),ur(p,e.name);const B=new ui({uid:v,auth:e,email:d,emailVerified:m,displayName:h,isAnonymous:S,photoURL:_,phoneNumber:y,tenantId:w,stsTokenManager:O,createdAt:g,lastLoginAt:p});return P&&Array.isArray(P)&&(B.providerData=P.map(U=>Object.assign({},U))),T&&(B._redirectEventId=T),B}static async _fromIdTokenResponse(e,n,r=!1){const i=new ea;i.updateFromServerResponse(n);const s=new ui({uid:n.localId,auth:e,stsTokenManager:i,isAnonymous:r});return await bu(s),s}}/**
 * @license
 * Copyright 2020 Google LLC
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */const Wv=new Map;function Ln(t){Gn(t instanceof Function,"Expected a class definition");let e=Wv.get(t);return e?(Gn(e instanceof t,"Instance stored in cache mismatched with class"),e):(e=new t,Wv.set(t,e),e)}/**
 * @license
 * Copyright 2019 Google LLC
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */class hw{constructor(){this.type="NONE",this.storage={}}async _isAvailable(){return!0}async _set(e,n){this.storage[e]=n}async _get(e){const n=this.storage[e];return n===void 0?null:n}async _remove(e){delete this.storage[e]}_addListener(e,n){}_removeListener(e,n){}}hw.type="NONE";const Kv=hw;/**
 * @license
 * Copyright 2019 Google LLC
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */function Yl(t,e,n){return`firebase:${t}:${e}:${n}`}class cs{constructor(e,n,r){this.persistence=e,this.auth=n,this.userKey=r;const{config:i,name:s}=this.auth;this.fullUserKey=Yl(this.userKey,i.apiKey,s),this.fullPersistenceKey=Yl("persistence",i.apiKey,s),this.boundEventHandler=n._onStorageEvent.bind(n),this.persistence._addListener(this.fullUserKey,this.boundEventHandler)}setCurrentUser(e){return this.persistence._set(this.fullUserKey,e.toJSON())}async getCurrentUser(){const e=await this.persistence._get(this.fullUserKey);return e?ui._fromJSON(this.auth,e):null}removeCurrentUser(){return this.persistence._remove(this.fullUserKey)}savePersistenceForRedirect(){return this.persistence._set(this.fullPersistenceKey,this.persistence.type)}async setPersistence(e){if(this.persistence===e)return;const n=await this.getCurrentUser();if(await this.removeCurrentUser(),this.persistence=e,n)return this.setCurrentUser(n)}delete(){this.persistence._removeListener(this.fullUserKey,this.boundEventHandler)}static async create(e,n,r="authUser"){if(!n.length)return new cs(Ln(Kv),e,r);const i=(await Promise.all(n.map(async u=>{if(await u._isAvailable())return u}))).filter(u=>u);let s=i[0]||Ln(Kv);const o=Yl(r,e.config.apiKey,e.name);let a=null;for(const u of n)try{const c=await u._get(o);if(c){const h=ui._fromJSON(e,c);u!==s&&(a=h),s=u;break}}catch{}const l=i.filter(u=>u._shouldAllowMigration);return!s._shouldAllowMigration||!l.length?new cs(s,e,r):(s=l[0],a&&await s._set(o,a.toJSON()),await Promise.all(n.map(async u=>{if(u!==s)try{await u._remove(o)}catch{}})),new cs(s,e,r))}}/**
 * @license
 * Copyright 2020 Google LLC
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */function Gv(t){const e=t.toLowerCase();if(e.includes("opera/")||e.includes("opr/")||e.includes("opios/"))return"Opera";if(mw(e))return"IEMobile";if(e.includes("msie")||e.includes("trident/"))return"IE";if(e.includes("edge/"))return"Edge";if(dw(e))return"Firefox";if(e.includes("silk/"))return"Silk";if(vw(e))return"Blackberry";if(yw(e))return"Webos";if(Zp(e))return"Safari";if((e.includes("chrome/")||pw(e))&&!e.includes("edge/"))return"Chrome";if(gw(e))return"Android";{const n=/([a-zA-Z\d\.]+)\/[a-zA-Z\d\.]*$/,r=t.match(n);if((r==null?void 0:r.length)===2)return r[1]}return"Other"}function dw(t=_t()){return/firefox\//i.test(t)}function Zp(t=_t()){const e=t.toLowerCase();return e.includes("safari/")&&!e.includes("chrome/")&&!e.includes("crios/")&&!e.includes("android")}function pw(t=_t()){return/crios\//i.test(t)}function mw(t=_t()){return/iemobile/i.test(t)}function gw(t=_t()){return/android/i.test(t)}function vw(t=_t()){return/blackberry/i.test(t)}function yw(t=_t()){return/webos/i.test(t)}function Tc(t=_t()){return/iphone|ipad|ipod/i.test(t)||/macintosh/i.test(t)&&/mobile/i.test(t)}function mP(t=_t()){var e;return Tc(t)&&!!(!((e=window.navigator)===null||e===void 0)&&e.standalone)}function gP(){return MA()&&document.documentMode===10}function _w(t=_t()){return Tc(t)||gw(t)||yw(t)||vw(t)||/windows phone/i.test(t)||mw(t)}function vP(){try{return!!(window&&window!==window.top)}catch{return!1}}/**
 * @license
 * Copyright 2020 Google LLC
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */function ww(t,e=[]){let n;switch(t){case"Browser":n=Gv(_t());break;case"Worker":n=`${Gv(_t())}-${t}`;break;default:n=t}const r=e.length?e.join(","):"FirebaseCore-web";return`${n}/JsCore/${Vs}/${r}`}async function Ew(t,e){return Na(t,"GET","/v2/recaptchaConfig",Xp(t,e))}function qv(t){return t!==void 0&&t.enterprise!==void 0}class Tw{constructor(e){if(this.siteKey="",this.emailPasswordEnabled=!1,e.recaptchaKey===void 0)throw new Error("recaptchaKey undefined");this.siteKey=e.recaptchaKey.split("/")[3],this.emailPasswordEnabled=e.recaptchaEnforcementState.some(n=>n.provider==="EMAIL_PASSWORD_PROVIDER"&&n.enforcementState!=="OFF")}}/**
 * @license
 * Copyright 2020 Google LLC
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */function yP(){var t,e;return(e=(t=document.getElementsByTagName("head"))===null||t===void 0?void 0:t[0])!==null&&e!==void 0?e:document}function Iw(t){return new Promise((e,n)=>{const r=document.createElement("script");r.setAttribute("src",t),r.onload=e,r.onerror=i=>{const s=In("internal-error");s.customData=i,n(s)},r.type="text/javascript",r.charset="UTF-8",yP().appendChild(r)})}function _P(t){return`__${t}${Math.floor(Math.random()*1e6)}`}const wP="https://www.google.com/recaptcha/enterprise.js?render=",EP="recaptcha-enterprise",TP="NO_RECAPTCHA";class Sw{constructor(e){this.type=EP,this.auth=ba(e)}async verify(e="verify",n=!1){async function r(s){if(!n){if(s.tenantId==null&&s._agentRecaptchaConfig!=null)return s._agentRecaptchaConfig.siteKey;if(s.tenantId!=null&&s._tenantRecaptchaConfigs[s.tenantId]!==void 0)return s._tenantRecaptchaConfigs[s.tenantId].siteKey}return new Promise(async(o,a)=>{Ew(s,{clientType:"CLIENT_TYPE_WEB",version:"RECAPTCHA_ENTERPRISE"}).then(l=>{if(l.recaptchaKey===void 0)a(new Error("recaptcha Enterprise site key undefined"));else{const u=new Tw(l);return s.tenantId==null?s._agentRecaptchaConfig=u:s._tenantRecaptchaConfigs[s.tenantId]=u,o(u.siteKey)}}).catch(l=>{a(l)})})}function i(s,o,a){const l=window.grecaptcha;qv(l)?l.enterprise.ready(()=>{l.enterprise.execute(s,{action:e}).then(u=>{o(u)}).catch(()=>{o(TP)})}):a(Error("No reCAPTCHA enterprise script loaded."))}return new Promise((s,o)=>{r(this.auth).then(a=>{if(!n&&qv(window.grecaptcha))i(a,s,o);else{if(typeof window>"u"){o(new Error("RecaptchaVerifier is only supported in browser"));return}Iw(wP+a).then(()=>{i(a,s,o)}).catch(l=>{o(l)})}}).catch(a=>{o(a)})})}}async function Qv(t,e,n,r=!1){const i=new Sw(t);let s;try{s=await i.verify(n)}catch{s=await i.verify(n,!0)}const o=Object.assign({},e);return r?Object.assign(o,{captchaResp:s}):Object.assign(o,{captchaResponse:s}),Object.assign(o,{clientType:"CLIENT_TYPE_WEB"}),Object.assign(o,{recaptchaVersion:"RECAPTCHA_ENTERPRISE"}),o}/**
 * @license
 * Copyright 2022 Google LLC
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */class IP{constructor(e){this.auth=e,this.queue=[]}pushCallback(e,n){const r=s=>new Promise((o,a)=>{try{const l=e(s);o(l)}catch(l){a(l)}});r.onAbort=n,this.queue.push(r);const i=this.queue.length-1;return()=>{this.queue[i]=()=>Promise.resolve()}}async runMiddleware(e){if(this.auth.currentUser===e)return;const n=[];try{for(const r of this.queue)await r(e),r.onAbort&&n.push(r.onAbort)}catch(r){n.reverse();for(const i of n)try{i()}catch{}throw this.auth._errorFactory.create("login-blocked",{originalMessage:r==null?void 0:r.message})}}}/**
 * @license
 * Copyright 2020 Google LLC
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */class SP{constructor(e,n,r,i){this.app=e,this.heartbeatServiceProvider=n,this.appCheckServiceProvider=r,this.config=i,this.currentUser=null,this.emulatorConfig=null,this.operations=Promise.resolve(),this.authStateSubscription=new Yv(this),this.idTokenSubscription=new Yv(this),this.beforeStateQueue=new IP(this),this.redirectUser=null,this.isProactiveRefreshEnabled=!1,this._canInitEmulator=!0,this._isInitialized=!1,this._deleted=!1,this._initializationPromise=null,this._popupRedirectResolver=null,this._errorFactory=ow,this._agentRecaptchaConfig=null,this._tenantRecaptchaConfigs={},this.lastNotifiedUid=void 0,this.languageCode=null,this.tenantId=null,this.settings={appVerificationDisabledForTesting:!1},this.frameworks=[],this.name=e.name,this.clientVersion=i.sdkClientVersion}_initializeWithPersistence(e,n){return n&&(this._popupRedirectResolver=Ln(n)),this._initializationPromise=this.queue(async()=>{var r,i;if(!this._deleted&&(this.persistenceManager=await cs.create(this,e),!this._deleted)){if(!((r=this._popupRedirectResolver)===null||r===void 0)&&r._shouldInitProactively)try{await this._popupRedirectResolver._initialize(this)}catch{}await this.initializeCurrentUser(n),this.lastNotifiedUid=((i=this.currentUser)===null||i===void 0?void 0:i.uid)||null,!this._deleted&&(this._isInitialized=!0)}}),this._initializationPromise}async _onStorageEvent(){if(this._deleted)return;const e=await this.assertedPersistence.getCurrentUser();if(!(!this.currentUser&&!e)){if(this.currentUser&&e&&this.currentUser.uid===e.uid){this._currentUser._assign(e),await this.currentUser.getIdToken();return}await this._updateCurrentUser(e,!0)}}async initializeCurrentUser(e){var n;const r=await this.assertedPersistence.getCurrentUser();let i=r,s=!1;if(e&&this.config.authDomain){await this.getOrInitRedirectPersistenceManager();const o=(n=this.redirectUser)===null||n===void 0?void 0:n._redirectEventId,a=i==null?void 0:i._redirectEventId,l=await this.tryRedirectSignIn(e);(!o||o===a)&&(l!=null&&l.user)&&(i=l.user,s=!0)}if(!i)return this.directlySetCurrentUser(null);if(!i._redirectEventId){if(s)try{await this.beforeStateQueue.runMiddleware(i)}catch(o){i=r,this._popupRedirectResolver._overrideRedirectResult(this,()=>Promise.reject(o))}return i?this.reloadAndSetCurrentUserOrClear(i):this.directlySetCurrentUser(null)}return Q(this._popupRedirectResolver,this,"argument-error"),await this.getOrInitRedirectPersistenceManager(),this.redirectUser&&this.redirectUser._redirectEventId===i._redirectEventId?this.directlySetCurrentUser(i):this.reloadAndSetCurrentUserOrClear(i)}async tryRedirectSignIn(e){let n=null;try{n=await this._popupRedirectResolver._completeRedirectFn(this,e,!0)}catch{await this._setRedirectUser(null)}return n}async reloadAndSetCurrentUserOrClear(e){try{await bu(e)}catch(n){if((n==null?void 0:n.code)!=="auth/network-request-failed")return this.directlySetCurrentUser(null)}return this.directlySetCurrentUser(e)}useDeviceLanguage(){this.languageCode=tP()}async _delete(){this._deleted=!0}async updateCurrentUser(e){const n=e?kn(e):null;return n&&Q(n.auth.config.apiKey===this.config.apiKey,this,"invalid-user-token"),this._updateCurrentUser(n&&n._clone(this))}async _updateCurrentUser(e,n=!1){if(!this._deleted)return e&&Q(this.tenantId===e.tenantId,this,"tenant-id-mismatch"),n||await this.beforeStateQueue.runMiddleware(e),this.queue(async()=>{await this.directlySetCurrentUser(e),this.notifyAuthListeners()})}async signOut(){return await this.beforeStateQueue.runMiddleware(null),(this.redirectPersistenceManager||this._popupRedirectResolver)&&await this._setRedirectUser(null),this._updateCurrentUser(null,!0)}setPersistence(e){return this.queue(async()=>{await this.assertedPersistence.setPersistence(Ln(e))})}async initializeRecaptchaConfig(){const e=await Ew(this,{clientType:"CLIENT_TYPE_WEB",version:"RECAPTCHA_ENTERPRISE"}),n=new Tw(e);this.tenantId==null?this._agentRecaptchaConfig=n:this._tenantRecaptchaConfigs[this.tenantId]=n,n.emailPasswordEnabled&&new Sw(this).verify()}_getRecaptchaConfig(){return this.tenantId==null?this._agentRecaptchaConfig:this._tenantRecaptchaConfigs[this.tenantId]}_getPersistence(){return this.assertedPersistence.persistence.type}_updateErrorMap(e){this._errorFactory=new Pa("auth","Firebase",e())}onAuthStateChanged(e,n,r){return this.registerStateListener(this.authStateSubscription,e,n,r)}beforeAuthStateChanged(e,n){return this.beforeStateQueue.pushCallback(e,n)}onIdTokenChanged(e,n,r){return this.registerStateListener(this.idTokenSubscription,e,n,r)}authStateReady(){return new Promise((e,n)=>{if(this.currentUser)e();else{const r=this.onAuthStateChanged(()=>{r(),e()},n)}})}toJSON(){var e;return{apiKey:this.config.apiKey,authDomain:this.config.authDomain,appName:this.name,currentUser:(e=this._currentUser)===null||e===void 0?void 0:e.toJSON()}}async _setRedirectUser(e,n){const r=await this.getOrInitRedirectPersistenceManager(n);return e===null?r.removeCurrentUser():r.setCurrentUser(e)}async getOrInitRedirectPersistenceManager(e){if(!this.redirectPersistenceManager){const n=e&&Ln(e)||this._popupRedirectResolver;Q(n,this,"argument-error"),this.redirectPersistenceManager=await cs.create(this,[Ln(n._redirectPersistence)],"redirectUser"),this.redirectUser=await this.redirectPersistenceManager.getCurrentUser()}return this.redirectPersistenceManager}async _redirectUserForId(e){var n,r;return this._isInitialized&&await this.queue(async()=>{}),((n=this._currentUser)===null||n===void 0?void 0:n._redirectEventId)===e?this._currentUser:((r=this.redirectUser)===null||r===void 0?void 0:r._redirectEventId)===e?this.redirectUser:null}async _persistUserIfCurrent(e){if(e===this.currentUser)return this.queue(async()=>this.directlySetCurrentUser(e))}_notifyListenersIfCurrent(e){e===this.currentUser&&this.notifyAuthListeners()}_key(){return`${this.config.authDomain}:${this.config.apiKey}:${this.name}`}_startProactiveRefresh(){this.isProactiveRefreshEnabled=!0,this.currentUser&&this._currentUser._startProactiveRefresh()}_stopProactiveRefresh(){this.isProactiveRefreshEnabled=!1,this.currentUser&&this._currentUser._stopProactiveRefresh()}get _currentUser(){return this.currentUser}notifyAuthListeners(){var e,n;if(!this._isInitialized)return;this.idTokenSubscription.next(this.currentUser);const r=(n=(e=this.currentUser)===null||e===void 0?void 0:e.uid)!==null&&n!==void 0?n:null;this.lastNotifiedUid!==r&&(this.lastNotifiedUid=r,this.authStateSubscription.next(this.currentUser))}registerStateListener(e,n,r,i){if(this._deleted)return()=>{};const s=typeof n=="function"?n:n.next.bind(n),o=this._isInitialized?Promise.resolve():this._initializationPromise;return Q(o,this,"internal-error"),o.then(()=>s(this.currentUser)),typeof n=="function"?e.addObserver(n,r,i):e.addObserver(n)}async directlySetCurrentUser(e){this.currentUser&&this.currentUser!==e&&this._currentUser._stopProactiveRefresh(),e&&this.isProactiveRefreshEnabled&&e._startProactiveRefresh(),this.currentUser=e,e?await this.assertedPersistence.setCurrentUser(e):await this.assertedPersistence.removeCurrentUser()}queue(e){return this.operations=this.operations.then(e,e),this.operations}get assertedPersistence(){return Q(this.persistenceManager,this,"internal-error"),this.persistenceManager}_logFramework(e){!e||this.frameworks.includes(e)||(this.frameworks.push(e),this.frameworks.sort(),this.clientVersion=ww(this.config.clientPlatform,this._getFrameworks()))}_getFrameworks(){return this.frameworks}async _getAdditionalHeaders(){var e;const n={"X-Client-Version":this.clientVersion};this.app.options.appId&&(n["X-Firebase-gmpid"]=this.app.options.appId);const r=await((e=this.heartbeatServiceProvider.getImmediate({optional:!0}))===null||e===void 0?void 0:e.getHeartbeatsHeader());r&&(n["X-Firebase-Client"]=r);const i=await this._getAppCheckToken();return i&&(n["X-Firebase-AppCheck"]=i),n}async _getAppCheckToken(){var e;const n=await((e=this.appCheckServiceProvider.getImmediate({optional:!0}))===null||e===void 0?void 0:e.getToken());return n!=null&&n.error&&Xx(`Error while retrieving App Check token: ${n.error}`),n==null?void 0:n.token}}function ba(t){return kn(t)}class Yv{constructor(e){this.auth=e,this.observer=null,this.addObserver=zA(n=>this.observer=n)}get next(){return Q(this.observer,this.auth,"internal-error"),this.observer.next.bind(this.observer)}}/**
 * @license
 * Copyright 2020 Google LLC
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */function kP(t,e){const n=Gp(t,"auth");if(n.isInitialized()){const i=n.getImmediate(),s=n.getOptions();if(Ru(s,e??{}))return i;Kn(i,"already-initialized")}return n.initialize({options:e})}function AP(t,e){const n=(e==null?void 0:e.persistence)||[],r=(Array.isArray(n)?n:[n]).map(Ln);e!=null&&e.errorMap&&t._updateErrorMap(e.errorMap),t._initializeWithPersistence(r,e==null?void 0:e.popupRedirectResolver)}function xP(t,e,n){const r=ba(t);Q(r._canInitEmulator,r,"emulator-config-failed"),Q(/^https?:\/\//.test(e),r,"invalid-emulator-scheme");const i=!!(n!=null&&n.disableWarnings),s=kw(e),{host:o,port:a}=PP(e),l=a===null?"":`:${a}`;r.config.emulator={url:`${s}//${o}${l}/`},r.settings.appVerificationDisabledForTesting=!0,r.emulatorConfig=Object.freeze({host:o,port:a,protocol:s.replace(":",""),options:Object.freeze({disableWarnings:i})}),i||RP()}function kw(t){const e=t.indexOf(":");return e<0?"":t.substr(0,e+1)}function PP(t){const e=kw(t),n=/(\/\/)?([^?#/]+)/.exec(t.substr(e.length));if(!n)return{host:"",port:null};const r=n[2].split("@").pop()||"",i=/^(\[[^\]]+\])(:|$)/.exec(r);if(i){const s=i[1];return{host:s,port:Xv(r.substr(s.length+1))}}else{const[s,o]=r.split(":");return{host:s,port:Xv(o)}}}function Xv(t){if(!t)return null;const e=Number(t);return isNaN(e)?null:e}function RP(){function t(){const e=document.createElement("p"),n=e.style;e.innerText="Running in emulator mode. Do not use with production credentials.",n.position="fixed",n.width="100%",n.backgroundColor="#ffffff",n.border=".1em solid #000000",n.color="#b50000",n.bottom="0px",n.left="0px",n.margin="0px",n.zIndex="10000",n.textAlign="center",e.classList.add("firebase-emulator-warning"),document.body.appendChild(e)}typeof console<"u"&&typeof console.info=="function"&&console.info("WARNING: You are using the Auth Emulator, which is intended for local testing only.  Do not use with production credentials."),typeof window<"u"&&typeof document<"u"&&(document.readyState==="loading"?window.addEventListener("DOMContentLoaded",t):t())}/**
 * @license
 * Copyright 2020 Google LLC
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */class Aw{constructor(e,n){this.providerId=e,this.signInMethod=n}toJSON(){return Mn("not implemented")}_getIdTokenResponse(e){return Mn("not implemented")}_linkToIdToken(e,n){return Mn("not implemented")}_getReauthenticationResolver(e){return Mn("not implemented")}}/**
 * @license
 * Copyright 2020 Google LLC
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */async function fs(t,e){return uw(t,"POST","/v1/accounts:signInWithIdp",Xp(t,e))}/**
 * @license
 * Copyright 2020 Google LLC
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */const CP="http://localhost";class wi extends Aw{constructor(){super(...arguments),this.pendingToken=null}static _fromParams(e){const n=new wi(e.providerId,e.signInMethod);return e.idToken||e.accessToken?(e.idToken&&(n.idToken=e.idToken),e.accessToken&&(n.accessToken=e.accessToken),e.nonce&&!e.pendingToken&&(n.nonce=e.nonce),e.pendingToken&&(n.pendingToken=e.pendingToken)):e.oauthToken&&e.oauthTokenSecret?(n.accessToken=e.oauthToken,n.secret=e.oauthTokenSecret):Kn("argument-error"),n}toJSON(){return{idToken:this.idToken,accessToken:this.accessToken,secret:this.secret,nonce:this.nonce,pendingToken:this.pendingToken,providerId:this.providerId,signInMethod:this.signInMethod}}static fromJSON(e){const n=typeof e=="string"?JSON.parse(e):e,{providerId:r,signInMethod:i}=n,s=qp(n,["providerId","signInMethod"]);if(!r||!i)return null;const o=new wi(r,i);return o.idToken=s.idToken||void 0,o.accessToken=s.accessToken||void 0,o.secret=s.secret,o.nonce=s.nonce,o.pendingToken=s.pendingToken||null,o}_getIdTokenResponse(e){const n=this.buildRequest();return fs(e,n)}_linkToIdToken(e,n){const r=this.buildRequest();return r.idToken=n,fs(e,r)}_getReauthenticationResolver(e){const n=this.buildRequest();return n.autoCreate=!1,fs(e,n)}buildRequest(){const e={requestUri:CP,returnSecureToken:!0};if(this.pendingToken)e.pendingToken=this.pendingToken;else{const n={};this.idToken&&(n.id_token=this.idToken),this.accessToken&&(n.access_token=this.accessToken),this.secret&&(n.oauth_token_secret=this.secret),n.providerId=this.providerId,this.nonce&&!this.pendingToken&&(n.nonce=this.nonce),e.postBody=Ra(n)}return e}}/**
 * @license
 * Copyright 2020 Google LLC
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */class xw{constructor(e){this.providerId=e,this.defaultLanguageCode=null,this.customParameters={}}setDefaultLanguage(e){this.defaultLanguageCode=e}setCustomParameters(e){return this.customParameters=e,this}getCustomParameters(){return this.customParameters}}/**
 * @license
 * Copyright 2019 Google LLC
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */class Oa extends xw{constructor(){super(...arguments),this.scopes=[]}addScope(e){return this.scopes.includes(e)||this.scopes.push(e),this}getScopes(){return[...this.scopes]}}/**
 * @license
 * Copyright 2020 Google LLC
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */class mr extends Oa{constructor(){super("facebook.com")}static credential(e){return wi._fromParams({providerId:mr.PROVIDER_ID,signInMethod:mr.FACEBOOK_SIGN_IN_METHOD,accessToken:e})}static credentialFromResult(e){return mr.credentialFromTaggedObject(e)}static credentialFromError(e){return mr.credentialFromTaggedObject(e.customData||{})}static credentialFromTaggedObject({_tokenResponse:e}){if(!e||!("oauthAccessToken"in e)||!e.oauthAccessToken)return null;try{return mr.credential(e.oauthAccessToken)}catch{return null}}}mr.FACEBOOK_SIGN_IN_METHOD="facebook.com";mr.PROVIDER_ID="facebook.com";/**
 * @license
 * Copyright 2020 Google LLC
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */class gr extends Oa{constructor(){super("google.com"),this.addScope("profile")}static credential(e,n){return wi._fromParams({providerId:gr.PROVIDER_ID,signInMethod:gr.GOOGLE_SIGN_IN_METHOD,idToken:e,accessToken:n})}static credentialFromResult(e){return gr.credentialFromTaggedObject(e)}static credentialFromError(e){return gr.credentialFromTaggedObject(e.customData||{})}static credentialFromTaggedObject({_tokenResponse:e}){if(!e)return null;const{oauthIdToken:n,oauthAccessToken:r}=e;if(!n&&!r)return null;try{return gr.credential(n,r)}catch{return null}}}gr.GOOGLE_SIGN_IN_METHOD="google.com";gr.PROVIDER_ID="google.com";/**
 * @license
 * Copyright 2020 Google LLC
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */class vr extends Oa{constructor(){super("github.com")}static credential(e){return wi._fromParams({providerId:vr.PROVIDER_ID,signInMethod:vr.GITHUB_SIGN_IN_METHOD,accessToken:e})}static credentialFromResult(e){return vr.credentialFromTaggedObject(e)}static credentialFromError(e){return vr.credentialFromTaggedObject(e.customData||{})}static credentialFromTaggedObject({_tokenResponse:e}){if(!e||!("oauthAccessToken"in e)||!e.oauthAccessToken)return null;try{return vr.credential(e.oauthAccessToken)}catch{return null}}}vr.GITHUB_SIGN_IN_METHOD="github.com";vr.PROVIDER_ID="github.com";/**
 * @license
 * Copyright 2020 Google LLC
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */class yr extends Oa{constructor(){super("twitter.com")}static credential(e,n){return wi._fromParams({providerId:yr.PROVIDER_ID,signInMethod:yr.TWITTER_SIGN_IN_METHOD,oauthToken:e,oauthTokenSecret:n})}static credentialFromResult(e){return yr.credentialFromTaggedObject(e)}static credentialFromError(e){return yr.credentialFromTaggedObject(e.customData||{})}static credentialFromTaggedObject({_tokenResponse:e}){if(!e)return null;const{oauthAccessToken:n,oauthTokenSecret:r}=e;if(!n||!r)return null;try{return yr.credential(n,r)}catch{return null}}}yr.TWITTER_SIGN_IN_METHOD="twitter.com";yr.PROVIDER_ID="twitter.com";/**
 * @license
 * Copyright 2020 Google LLC
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */async function $f(t,e){return uw(t,"POST","/v1/accounts:signUp",Xp(t,e))}/**
 * @license
 * Copyright 2020 Google LLC
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */class Ei{constructor(e){this.user=e.user,this.providerId=e.providerId,this._tokenResponse=e._tokenResponse,this.operationType=e.operationType}static async _fromIdTokenResponse(e,n,r,i=!1){const s=await ui._fromIdTokenResponse(e,r,i),o=Jv(r);return new Ei({user:s,providerId:o,_tokenResponse:r,operationType:n})}static async _forOperation(e,n,r){await e._updateTokensIfNecessary(r,!0);const i=Jv(r);return new Ei({user:e,providerId:i,_tokenResponse:r,operationType:n})}}function Jv(t){return t.providerId?t.providerId:"phoneNumber"in t?"phone":null}/**
 * @license
 * Copyright 2020 Google LLC
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */class Ou extends tr{constructor(e,n,r,i){var s;super(n.code,n.message),this.operationType=r,this.user=i,Object.setPrototypeOf(this,Ou.prototype),this.customData={appName:e.name,tenantId:(s=e.tenantId)!==null&&s!==void 0?s:void 0,_serverResponse:n.customData._serverResponse,operationType:r}}static _fromErrorAndOperation(e,n,r,i){return new Ou(e,n,r,i)}}function Pw(t,e,n,r){return(e==="reauthenticate"?n._getReauthenticationResolver(t):n._getIdTokenResponse(t)).catch(s=>{throw s.code==="auth/multi-factor-auth-required"?Ou._fromErrorAndOperation(t,s,e,r):s})}async function NP(t,e,n=!1){const r=await Zo(t,e._linkToIdToken(t.auth,await t.getIdToken()),n);return Ei._forOperation(t,"link",r)}/**
 * @license
 * Copyright 2019 Google LLC
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */async function bP(t,e,n=!1){const{auth:r}=t,i="reauthenticate";try{const s=await Zo(t,Pw(r,i,e,t),n);Q(s.idToken,r,"internal-error");const o=Jp(s.idToken);Q(o,r,"internal-error");const{sub:a}=o;return Q(t.uid===a,r,"user-mismatch"),Ei._forOperation(t,i,s)}catch(s){throw(s==null?void 0:s.code)==="auth/user-not-found"&&Kn(r,"user-mismatch"),s}}/**
 * @license
 * Copyright 2020 Google LLC
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */async function OP(t,e,n=!1){const r="signIn",i=await Pw(t,r,e),s=await Ei._fromIdTokenResponse(t,r,i);return n||await t._updateCurrentUser(s.user),s}async function DP(t,e,n){var r;const i=ba(t),s={returnSecureToken:!0,email:e,password:n,clientType:"CLIENT_TYPE_WEB"};let o;if(!((r=i._getRecaptchaConfig())===null||r===void 0)&&r.emailPasswordEnabled){const u=await Qv(i,s,"signUpPassword");o=$f(i,u)}else o=$f(i,s).catch(async u=>{if(u.code==="auth/missing-recaptcha-token"){console.log("Sign-up is protected by reCAPTCHA for this project. Automatically triggering the reCAPTCHA flow and restarting the sign-up flow.");const c=await Qv(i,s,"signUpPassword");return $f(i,c)}else return Promise.reject(u)});const a=await o.catch(u=>Promise.reject(u)),l=await Ei._fromIdTokenResponse(i,"signIn",a);return await i._updateCurrentUser(l.user),l}function MP(t,e,n,r){return kn(t).onIdTokenChanged(e,n,r)}function LP(t,e,n){return kn(t).beforeAuthStateChanged(e,n)}const Du="__sak";/**
 * @license
 * Copyright 2019 Google LLC
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */class Rw{constructor(e,n){this.storageRetriever=e,this.type=n}_isAvailable(){try{return this.storage?(this.storage.setItem(Du,"1"),this.storage.removeItem(Du),Promise.resolve(!0)):Promise.resolve(!1)}catch{return Promise.resolve(!1)}}_set(e,n){return this.storage.setItem(e,JSON.stringify(n)),Promise.resolve()}_get(e){const n=this.storage.getItem(e);return Promise.resolve(n?JSON.parse(n):null)}_remove(e){return this.storage.removeItem(e),Promise.resolve()}get storage(){return this.storageRetriever()}}/**
 * @license
 * Copyright 2020 Google LLC
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */function jP(){const t=_t();return Zp(t)||Tc(t)}const VP=1e3,FP=10;class Cw extends Rw{constructor(){super(()=>window.localStorage,"LOCAL"),this.boundEventHandler=(e,n)=>this.onStorageEvent(e,n),this.listeners={},this.localCache={},this.pollTimer=null,this.safariLocalStorageNotSynced=jP()&&vP(),this.fallbackToPolling=_w(),this._shouldAllowMigration=!0}forAllChangedKeys(e){for(const n of Object.keys(this.listeners)){const r=this.storage.getItem(n),i=this.localCache[n];r!==i&&e(n,i,r)}}onStorageEvent(e,n=!1){if(!e.key){this.forAllChangedKeys((o,a,l)=>{this.notifyListeners(o,l)});return}const r=e.key;if(n?this.detachListener():this.stopPolling(),this.safariLocalStorageNotSynced){const o=this.storage.getItem(r);if(e.newValue!==o)e.newValue!==null?this.storage.setItem(r,e.newValue):this.storage.removeItem(r);else if(this.localCache[r]===e.newValue&&!n)return}const i=()=>{const o=this.storage.getItem(r);!n&&this.localCache[r]===o||this.notifyListeners(r,o)},s=this.storage.getItem(r);gP()&&s!==e.newValue&&e.newValue!==e.oldValue?setTimeout(i,FP):i()}notifyListeners(e,n){this.localCache[e]=n;const r=this.listeners[e];if(r)for(const i of Array.from(r))i(n&&JSON.parse(n))}startPolling(){this.stopPolling(),this.pollTimer=setInterval(()=>{this.forAllChangedKeys((e,n,r)=>{this.onStorageEvent(new StorageEvent("storage",{key:e,oldValue:n,newValue:r}),!0)})},VP)}stopPolling(){this.pollTimer&&(clearInterval(this.pollTimer),this.pollTimer=null)}attachListener(){window.addEventListener("storage",this.boundEventHandler)}detachListener(){window.removeEventListener("storage",this.boundEventHandler)}_addListener(e,n){Object.keys(this.listeners).length===0&&(this.fallbackToPolling?this.startPolling():this.attachListener()),this.listeners[e]||(this.listeners[e]=new Set,this.localCache[e]=this.storage.getItem(e)),this.listeners[e].add(n)}_removeListener(e,n){this.listeners[e]&&(this.listeners[e].delete(n),this.listeners[e].size===0&&delete this.listeners[e]),Object.keys(this.listeners).length===0&&(this.detachListener(),this.stopPolling())}async _set(e,n){await super._set(e,n),this.localCache[e]=JSON.stringify(n)}async _get(e){const n=await super._get(e);return this.localCache[e]=JSON.stringify(n),n}async _remove(e){await super._remove(e),delete this.localCache[e]}}Cw.type="LOCAL";const UP=Cw;/**
 * @license
 * Copyright 2020 Google LLC
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */class Nw extends Rw{constructor(){super(()=>window.sessionStorage,"SESSION")}_addListener(e,n){}_removeListener(e,n){}}Nw.type="SESSION";const bw=Nw;/**
 * @license
 * Copyright 2019 Google LLC
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */function $P(t){return Promise.all(t.map(async e=>{try{return{fulfilled:!0,value:await e}}catch(n){return{fulfilled:!1,reason:n}}}))}/**
 * @license
 * Copyright 2019 Google LLC
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */class Ic{constructor(e){this.eventTarget=e,this.handlersMap={},this.boundEventHandler=this.handleEvent.bind(this)}static _getInstance(e){const n=this.receivers.find(i=>i.isListeningto(e));if(n)return n;const r=new Ic(e);return this.receivers.push(r),r}isListeningto(e){return this.eventTarget===e}async handleEvent(e){const n=e,{eventId:r,eventType:i,data:s}=n.data,o=this.handlersMap[i];if(!(o!=null&&o.size))return;n.ports[0].postMessage({status:"ack",eventId:r,eventType:i});const a=Array.from(o).map(async u=>u(n.origin,s)),l=await $P(a);n.ports[0].postMessage({status:"done",eventId:r,eventType:i,response:l})}_subscribe(e,n){Object.keys(this.handlersMap).length===0&&this.eventTarget.addEventListener("message",this.boundEventHandler),this.handlersMap[e]||(this.handlersMap[e]=new Set),this.handlersMap[e].add(n)}_unsubscribe(e,n){this.handlersMap[e]&&n&&this.handlersMap[e].delete(n),(!n||this.handlersMap[e].size===0)&&delete this.handlersMap[e],Object.keys(this.handlersMap).length===0&&this.eventTarget.removeEventListener("message",this.boundEventHandler)}}Ic.receivers=[];/**
 * @license
 * Copyright 2020 Google LLC
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */function em(t="",e=10){let n="";for(let r=0;r<e;r++)n+=Math.floor(Math.random()*10);return t+n}/**
 * @license
 * Copyright 2019 Google LLC
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */class zP{constructor(e){this.target=e,this.handlers=new Set}removeMessageHandler(e){e.messageChannel&&(e.messageChannel.port1.removeEventListener("message",e.onMessage),e.messageChannel.port1.close()),this.handlers.delete(e)}async _send(e,n,r=50){const i=typeof MessageChannel<"u"?new MessageChannel:null;if(!i)throw new Error("connection_unavailable");let s,o;return new Promise((a,l)=>{const u=em("",20);i.port1.start();const c=setTimeout(()=>{l(new Error("unsupported_event"))},r);o={messageChannel:i,onMessage(h){const d=h;if(d.data.eventId===u)switch(d.data.status){case"ack":clearTimeout(c),s=setTimeout(()=>{l(new Error("timeout"))},3e3);break;case"done":clearTimeout(s),a(d.data.response);break;default:clearTimeout(c),clearTimeout(s),l(new Error("invalid_response"));break}}},this.handlers.add(o),i.port1.addEventListener("message",o.onMessage),this.target.postMessage({eventType:e,eventId:u,data:n},[i.port2])}).finally(()=>{o&&this.removeMessageHandler(o)})}}/**
 * @license
 * Copyright 2020 Google LLC
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */function Sn(){return window}function BP(t){Sn().location.href=t}/**
 * @license
 * Copyright 2020 Google LLC.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */function Ow(){return typeof Sn().WorkerGlobalScope<"u"&&typeof Sn().importScripts=="function"}async function HP(){if(!(navigator!=null&&navigator.serviceWorker))return null;try{return(await navigator.serviceWorker.ready).active}catch{return null}}function WP(){var t;return((t=navigator==null?void 0:navigator.serviceWorker)===null||t===void 0?void 0:t.controller)||null}function KP(){return Ow()?self:null}/**
 * @license
 * Copyright 2019 Google LLC
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */const Dw="firebaseLocalStorageDb",GP=1,Mu="firebaseLocalStorage",Mw="fbase_key";class Da{constructor(e){this.request=e}toPromise(){return new Promise((e,n)=>{this.request.addEventListener("success",()=>{e(this.request.result)}),this.request.addEventListener("error",()=>{n(this.request.error)})})}}function Sc(t,e){return t.transaction([Mu],e?"readwrite":"readonly").objectStore(Mu)}function qP(){const t=indexedDB.deleteDatabase(Dw);return new Da(t).toPromise()}function cd(){const t=indexedDB.open(Dw,GP);return new Promise((e,n)=>{t.addEventListener("error",()=>{n(t.error)}),t.addEventListener("upgradeneeded",()=>{const r=t.result;try{r.createObjectStore(Mu,{keyPath:Mw})}catch(i){n(i)}}),t.addEventListener("success",async()=>{const r=t.result;r.objectStoreNames.contains(Mu)?e(r):(r.close(),await qP(),e(await cd()))})})}async function Zv(t,e,n){const r=Sc(t,!0).put({[Mw]:e,value:n});return new Da(r).toPromise()}async function QP(t,e){const n=Sc(t,!1).get(e),r=await new Da(n).toPromise();return r===void 0?null:r.value}function ey(t,e){const n=Sc(t,!0).delete(e);return new Da(n).toPromise()}const YP=800,XP=3;class Lw{constructor(){this.type="LOCAL",this._shouldAllowMigration=!0,this.listeners={},this.localCache={},this.pollTimer=null,this.pendingWrites=0,this.receiver=null,this.sender=null,this.serviceWorkerReceiverAvailable=!1,this.activeServiceWorker=null,this._workerInitializationPromise=this.initializeServiceWorkerMessaging().then(()=>{},()=>{})}async _openDb(){return this.db?this.db:(this.db=await cd(),this.db)}async _withRetries(e){let n=0;for(;;)try{const r=await this._openDb();return await e(r)}catch(r){if(n++>XP)throw r;this.db&&(this.db.close(),this.db=void 0)}}async initializeServiceWorkerMessaging(){return Ow()?this.initializeReceiver():this.initializeSender()}async initializeReceiver(){this.receiver=Ic._getInstance(KP()),this.receiver._subscribe("keyChanged",async(e,n)=>({keyProcessed:(await this._poll()).includes(n.key)})),this.receiver._subscribe("ping",async(e,n)=>["keyChanged"])}async initializeSender(){var e,n;if(this.activeServiceWorker=await HP(),!this.activeServiceWorker)return;this.sender=new zP(this.activeServiceWorker);const r=await this.sender._send("ping",{},800);r&&!((e=r[0])===null||e===void 0)&&e.fulfilled&&!((n=r[0])===null||n===void 0)&&n.value.includes("keyChanged")&&(this.serviceWorkerReceiverAvailable=!0)}async notifyServiceWorker(e){if(!(!this.sender||!this.activeServiceWorker||WP()!==this.activeServiceWorker))try{await this.sender._send("keyChanged",{key:e},this.serviceWorkerReceiverAvailable?800:50)}catch{}}async _isAvailable(){try{if(!indexedDB)return!1;const e=await cd();return await Zv(e,Du,"1"),await ey(e,Du),!0}catch{}return!1}async _withPendingWrite(e){this.pendingWrites++;try{await e()}finally{this.pendingWrites--}}async _set(e,n){return this._withPendingWrite(async()=>(await this._withRetries(r=>Zv(r,e,n)),this.localCache[e]=n,this.notifyServiceWorker(e)))}async _get(e){const n=await this._withRetries(r=>QP(r,e));return this.localCache[e]=n,n}async _remove(e){return this._withPendingWrite(async()=>(await this._withRetries(n=>ey(n,e)),delete this.localCache[e],this.notifyServiceWorker(e)))}async _poll(){const e=await this._withRetries(i=>{const s=Sc(i,!1).getAll();return new Da(s).toPromise()});if(!e)return[];if(this.pendingWrites!==0)return[];const n=[],r=new Set;for(const{fbase_key:i,value:s}of e)r.add(i),JSON.stringify(this.localCache[i])!==JSON.stringify(s)&&(this.notifyListeners(i,s),n.push(i));for(const i of Object.keys(this.localCache))this.localCache[i]&&!r.has(i)&&(this.notifyListeners(i,null),n.push(i));return n}notifyListeners(e,n){this.localCache[e]=n;const r=this.listeners[e];if(r)for(const i of Array.from(r))i(n)}startPolling(){this.stopPolling(),this.pollTimer=setInterval(async()=>this._poll(),YP)}stopPolling(){this.pollTimer&&(clearInterval(this.pollTimer),this.pollTimer=null)}_addListener(e,n){Object.keys(this.listeners).length===0&&this.startPolling(),this.listeners[e]||(this.listeners[e]=new Set,this._get(e)),this.listeners[e].add(n)}_removeListener(e,n){this.listeners[e]&&(this.listeners[e].delete(n),this.listeners[e].size===0&&delete this.listeners[e]),Object.keys(this.listeners).length===0&&this.stopPolling()}}Lw.type="LOCAL";const JP=Lw;new Ca(3e4,6e4);/**
 * @license
 * Copyright 2021 Google LLC
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */function ZP(t,e){return e?Ln(e):(Q(t._popupRedirectResolver,t,"argument-error"),t._popupRedirectResolver)}/**
 * @license
 * Copyright 2019 Google LLC
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */class tm extends Aw{constructor(e){super("custom","custom"),this.params=e}_getIdTokenResponse(e){return fs(e,this._buildIdpRequest())}_linkToIdToken(e,n){return fs(e,this._buildIdpRequest(n))}_getReauthenticationResolver(e){return fs(e,this._buildIdpRequest())}_buildIdpRequest(e){const n={requestUri:this.params.requestUri,sessionId:this.params.sessionId,postBody:this.params.postBody,tenantId:this.params.tenantId,pendingToken:this.params.pendingToken,returnSecureToken:!0,returnIdpCredential:!0};return e&&(n.idToken=e),n}}function eR(t){return OP(t.auth,new tm(t),t.bypassAuthState)}function tR(t){const{auth:e,user:n}=t;return Q(n,e,"internal-error"),bP(n,new tm(t),t.bypassAuthState)}async function nR(t){const{auth:e,user:n}=t;return Q(n,e,"internal-error"),NP(n,new tm(t),t.bypassAuthState)}/**
 * @license
 * Copyright 2020 Google LLC
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */class jw{constructor(e,n,r,i,s=!1){this.auth=e,this.resolver=r,this.user=i,this.bypassAuthState=s,this.pendingPromise=null,this.eventManager=null,this.filter=Array.isArray(n)?n:[n]}execute(){return new Promise(async(e,n)=>{this.pendingPromise={resolve:e,reject:n};try{this.eventManager=await this.resolver._initialize(this.auth),await this.onExecution(),this.eventManager.registerConsumer(this)}catch(r){this.reject(r)}})}async onAuthEvent(e){const{urlResponse:n,sessionId:r,postBody:i,tenantId:s,error:o,type:a}=e;if(o){this.reject(o);return}const l={auth:this.auth,requestUri:n,sessionId:r,tenantId:s||void 0,postBody:i||void 0,user:this.user,bypassAuthState:this.bypassAuthState};try{this.resolve(await this.getIdpTask(a)(l))}catch(u){this.reject(u)}}onError(e){this.reject(e)}getIdpTask(e){switch(e){case"signInViaPopup":case"signInViaRedirect":return eR;case"linkViaPopup":case"linkViaRedirect":return nR;case"reauthViaPopup":case"reauthViaRedirect":return tR;default:Kn(this.auth,"internal-error")}}resolve(e){Gn(this.pendingPromise,"Pending promise was never set"),this.pendingPromise.resolve(e),this.unregisterAndCleanUp()}reject(e){Gn(this.pendingPromise,"Pending promise was never set"),this.pendingPromise.reject(e),this.unregisterAndCleanUp()}unregisterAndCleanUp(){this.eventManager&&this.eventManager.unregisterConsumer(this),this.pendingPromise=null,this.cleanUp()}}/**
 * @license
 * Copyright 2020 Google LLC
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */const rR=new Ca(2e3,1e4);class Ji extends jw{constructor(e,n,r,i,s){super(e,n,i,s),this.provider=r,this.authWindow=null,this.pollId=null,Ji.currentPopupAction&&Ji.currentPopupAction.cancel(),Ji.currentPopupAction=this}async executeNotNull(){const e=await this.execute();return Q(e,this.auth,"internal-error"),e}async onExecution(){Gn(this.filter.length===1,"Popup operations only handle one event");const e=em();this.authWindow=await this.resolver._openPopup(this.auth,this.provider,this.filter[0],e),this.authWindow.associatedEvent=e,this.resolver._originValidation(this.auth).catch(n=>{this.reject(n)}),this.resolver._isIframeWebStorageSupported(this.auth,n=>{n||this.reject(In(this.auth,"web-storage-unsupported"))}),this.pollUserCancellation()}get eventId(){var e;return((e=this.authWindow)===null||e===void 0?void 0:e.associatedEvent)||null}cancel(){this.reject(In(this.auth,"cancelled-popup-request"))}cleanUp(){this.authWindow&&this.authWindow.close(),this.pollId&&window.clearTimeout(this.pollId),this.authWindow=null,this.pollId=null,Ji.currentPopupAction=null}pollUserCancellation(){const e=()=>{var n,r;if(!((r=(n=this.authWindow)===null||n===void 0?void 0:n.window)===null||r===void 0)&&r.closed){this.pollId=window.setTimeout(()=>{this.pollId=null,this.reject(In(this.auth,"popup-closed-by-user"))},8e3);return}this.pollId=window.setTimeout(e,rR.get())};e()}}Ji.currentPopupAction=null;/**
 * @license
 * Copyright 2020 Google LLC
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */const iR="pendingRedirect",Xl=new Map;class sR extends jw{constructor(e,n,r=!1){super(e,["signInViaRedirect","linkViaRedirect","reauthViaRedirect","unknown"],n,void 0,r),this.eventId=null}async execute(){let e=Xl.get(this.auth._key());if(!e){try{const r=await oR(this.resolver,this.auth)?await super.execute():null;e=()=>Promise.resolve(r)}catch(n){e=()=>Promise.reject(n)}Xl.set(this.auth._key(),e)}return this.bypassAuthState||Xl.set(this.auth._key(),()=>Promise.resolve(null)),e()}async onAuthEvent(e){if(e.type==="signInViaRedirect")return super.onAuthEvent(e);if(e.type==="unknown"){this.resolve(null);return}if(e.eventId){const n=await this.auth._redirectUserForId(e.eventId);if(n)return this.user=n,super.onAuthEvent(e);this.resolve(null)}}async onExecution(){}cleanUp(){}}async function oR(t,e){const n=uR(e),r=lR(t);if(!await r._isAvailable())return!1;const i=await r._get(n)==="true";return await r._remove(n),i}function aR(t,e){Xl.set(t._key(),e)}function lR(t){return Ln(t._redirectPersistence)}function uR(t){return Yl(iR,t.config.apiKey,t.name)}async function cR(t,e,n=!1){const r=ba(t),i=ZP(r,e),o=await new sR(r,i,n).execute();return o&&!n&&(delete o.user._redirectEventId,await r._persistUserIfCurrent(o.user),await r._setRedirectUser(null,e)),o}/**
 * @license
 * Copyright 2020 Google LLC
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */const fR=10*60*1e3;class hR{constructor(e){this.auth=e,this.cachedEventUids=new Set,this.consumers=new Set,this.queuedRedirectEvent=null,this.hasHandledPotentialRedirect=!1,this.lastProcessedEventTime=Date.now()}registerConsumer(e){this.consumers.add(e),this.queuedRedirectEvent&&this.isEventForConsumer(this.queuedRedirectEvent,e)&&(this.sendToConsumer(this.queuedRedirectEvent,e),this.saveEventToCache(this.queuedRedirectEvent),this.queuedRedirectEvent=null)}unregisterConsumer(e){this.consumers.delete(e)}onEvent(e){if(this.hasEventBeenHandled(e))return!1;let n=!1;return this.consumers.forEach(r=>{this.isEventForConsumer(e,r)&&(n=!0,this.sendToConsumer(e,r),this.saveEventToCache(e))}),this.hasHandledPotentialRedirect||!dR(e)||(this.hasHandledPotentialRedirect=!0,n||(this.queuedRedirectEvent=e,n=!0)),n}sendToConsumer(e,n){var r;if(e.error&&!Vw(e)){const i=((r=e.error.code)===null||r===void 0?void 0:r.split("auth/")[1])||"internal-error";n.onError(In(this.auth,i))}else n.onAuthEvent(e)}isEventForConsumer(e,n){const r=n.eventId===null||!!e.eventId&&e.eventId===n.eventId;return n.filter.includes(e.type)&&r}hasEventBeenHandled(e){return Date.now()-this.lastProcessedEventTime>=fR&&this.cachedEventUids.clear(),this.cachedEventUids.has(ty(e))}saveEventToCache(e){this.cachedEventUids.add(ty(e)),this.lastProcessedEventTime=Date.now()}}function ty(t){return[t.type,t.eventId,t.sessionId,t.tenantId].filter(e=>e).join("-")}function Vw({type:t,error:e}){return t==="unknown"&&(e==null?void 0:e.code)==="auth/no-auth-event"}function dR(t){switch(t.type){case"signInViaRedirect":case"linkViaRedirect":case"reauthViaRedirect":return!0;case"unknown":return Vw(t);default:return!1}}/**
 * @license
 * Copyright 2020 Google LLC
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */async function pR(t,e={}){return Na(t,"GET","/v1/projects",e)}/**
 * @license
 * Copyright 2020 Google LLC
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */const mR=/^\d{1,3}\.\d{1,3}\.\d{1,3}\.\d{1,3}$/,gR=/^https?/;async function vR(t){if(t.config.emulator)return;const{authorizedDomains:e}=await pR(t);for(const n of e)try{if(yR(n))return}catch{}Kn(t,"unauthorized-domain")}function yR(t){const e=ud(),{protocol:n,hostname:r}=new URL(e);if(t.startsWith("chrome-extension://")){const o=new URL(t);return o.hostname===""&&r===""?n==="chrome-extension:"&&t.replace("chrome-extension://","")===e.replace("chrome-extension://",""):n==="chrome-extension:"&&o.hostname===r}if(!gR.test(n))return!1;if(mR.test(t))return r===t;const i=t.replace(/\./g,"\\.");return new RegExp("^(.+\\."+i+"|"+i+")$","i").test(r)}/**
 * @license
 * Copyright 2020 Google LLC.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */const _R=new Ca(3e4,6e4);function ny(){const t=Sn().___jsl;if(t!=null&&t.H){for(const e of Object.keys(t.H))if(t.H[e].r=t.H[e].r||[],t.H[e].L=t.H[e].L||[],t.H[e].r=[...t.H[e].L],t.CP)for(let n=0;n<t.CP.length;n++)t.CP[n]=null}}function wR(t){return new Promise((e,n)=>{var r,i,s;function o(){ny(),gapi.load("gapi.iframes",{callback:()=>{e(gapi.iframes.getContext())},ontimeout:()=>{ny(),n(In(t,"network-request-failed"))},timeout:_R.get()})}if(!((i=(r=Sn().gapi)===null||r===void 0?void 0:r.iframes)===null||i===void 0)&&i.Iframe)e(gapi.iframes.getContext());else if(!((s=Sn().gapi)===null||s===void 0)&&s.load)o();else{const a=_P("iframefcb");return Sn()[a]=()=>{gapi.load?o():n(In(t,"network-request-failed"))},Iw(`https://apis.google.com/js/api.js?onload=${a}`).catch(l=>n(l))}}).catch(e=>{throw Jl=null,e})}let Jl=null;function ER(t){return Jl=Jl||wR(t),Jl}/**
 * @license
 * Copyright 2020 Google LLC.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */const TR=new Ca(5e3,15e3),IR="__/auth/iframe",SR="emulator/auth/iframe",kR={style:{position:"absolute",top:"-100px",width:"1px",height:"1px"},"aria-hidden":"true",tabindex:"-1"},AR=new Map([["identitytoolkit.googleapis.com","p"],["staging-identitytoolkit.sandbox.googleapis.com","s"],["test-identitytoolkit.sandbox.googleapis.com","t"]]);function xR(t){const e=t.config;Q(e.authDomain,t,"auth-domain-config-required");const n=e.emulator?Yp(e,SR):`https://${t.config.authDomain}/${IR}`,r={apiKey:e.apiKey,appName:t.name,v:Vs},i=AR.get(t.config.apiHost);i&&(r.eid=i);const s=t._getFrameworks();return s.length&&(r.fw=s.join(",")),`${n}?${Ra(r).slice(1)}`}async function PR(t){const e=await ER(t),n=Sn().gapi;return Q(n,t,"internal-error"),e.open({where:document.body,url:xR(t),messageHandlersFilter:n.iframes.CROSS_ORIGIN_IFRAMES_FILTER,attributes:kR,dontclear:!0},r=>new Promise(async(i,s)=>{await r.restyle({setHideOnLeave:!1});const o=In(t,"network-request-failed"),a=Sn().setTimeout(()=>{s(o)},TR.get());function l(){Sn().clearTimeout(a),i(r)}r.ping(l).then(l,()=>{s(o)})}))}/**
 * @license
 * Copyright 2020 Google LLC.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */const RR={location:"yes",resizable:"yes",statusbar:"yes",toolbar:"no"},CR=500,NR=600,bR="_blank",OR="http://localhost";class ry{constructor(e){this.window=e,this.associatedEvent=null}close(){if(this.window)try{this.window.close()}catch{}}}function DR(t,e,n,r=CR,i=NR){const s=Math.max((window.screen.availHeight-i)/2,0).toString(),o=Math.max((window.screen.availWidth-r)/2,0).toString();let a="";const l=Object.assign(Object.assign({},RR),{width:r.toString(),height:i.toString(),top:s,left:o}),u=_t().toLowerCase();n&&(a=pw(u)?bR:n),dw(u)&&(e=e||OR,l.scrollbars="yes");const c=Object.entries(l).reduce((d,[y,_])=>`${d}${y}=${_},`,"");if(mP(u)&&a!=="_self")return MR(e||"",a),new ry(null);const h=window.open(e||"",a,c);Q(h,t,"popup-blocked");try{h.focus()}catch{}return new ry(h)}function MR(t,e){const n=document.createElement("a");n.href=t,n.target=e;const r=document.createEvent("MouseEvent");r.initMouseEvent("click",!0,!0,window,1,0,0,0,0,!1,!1,!1,!1,1,null),n.dispatchEvent(r)}/**
 * @license
 * Copyright 2021 Google LLC
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */const LR="__/auth/handler",jR="emulator/auth/handler",VR=encodeURIComponent("fac");async function iy(t,e,n,r,i,s){Q(t.config.authDomain,t,"auth-domain-config-required"),Q(t.config.apiKey,t,"invalid-api-key");const o={apiKey:t.config.apiKey,appName:t.name,authType:n,redirectUrl:r,v:Vs,eventId:i};if(e instanceof xw){e.setDefaultLanguage(t.languageCode),o.providerId=e.providerId||"",$A(e.getCustomParameters())||(o.customParameters=JSON.stringify(e.getCustomParameters()));for(const[c,h]of Object.entries(s||{}))o[c]=h}if(e instanceof Oa){const c=e.getScopes().filter(h=>h!=="");c.length>0&&(o.scopes=c.join(","))}t.tenantId&&(o.tid=t.tenantId);const a=o;for(const c of Object.keys(a))a[c]===void 0&&delete a[c];const l=await t._getAppCheckToken(),u=l?`#${VR}=${encodeURIComponent(l)}`:"";return`${FR(t)}?${Ra(a).slice(1)}${u}`}function FR({config:t}){return t.emulator?Yp(t,jR):`https://${t.authDomain}/${LR}`}/**
 * @license
 * Copyright 2020 Google LLC
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */const zf="webStorageSupport";class UR{constructor(){this.eventManagers={},this.iframes={},this.originValidationPromises={},this._redirectPersistence=bw,this._completeRedirectFn=cR,this._overrideRedirectResult=aR}async _openPopup(e,n,r,i){var s;Gn((s=this.eventManagers[e._key()])===null||s===void 0?void 0:s.manager,"_initialize() not called before _openPopup()");const o=await iy(e,n,r,ud(),i);return DR(e,o,em())}async _openRedirect(e,n,r,i){await this._originValidation(e);const s=await iy(e,n,r,ud(),i);return BP(s),new Promise(()=>{})}_initialize(e){const n=e._key();if(this.eventManagers[n]){const{manager:i,promise:s}=this.eventManagers[n];return i?Promise.resolve(i):(Gn(s,"If manager is not set, promise should be"),s)}const r=this.initAndGetManager(e);return this.eventManagers[n]={promise:r},r.catch(()=>{delete this.eventManagers[n]}),r}async initAndGetManager(e){const n=await PR(e),r=new hR(e);return n.register("authEvent",i=>(Q(i==null?void 0:i.authEvent,e,"invalid-auth-event"),{status:r.onEvent(i.authEvent)?"ACK":"ERROR"}),gapi.iframes.CROSS_ORIGIN_IFRAMES_FILTER),this.eventManagers[e._key()]={manager:r},this.iframes[e._key()]=n,r}_isIframeWebStorageSupported(e,n){this.iframes[e._key()].send(zf,{type:zf},i=>{var s;const o=(s=i==null?void 0:i[0])===null||s===void 0?void 0:s[zf];o!==void 0&&n(!!o),Kn(e,"internal-error")},gapi.iframes.CROSS_ORIGIN_IFRAMES_FILTER)}_originValidation(e){const n=e._key();return this.originValidationPromises[n]||(this.originValidationPromises[n]=vR(e)),this.originValidationPromises[n]}get _shouldInitProactively(){return _w()||Zp()||Tc()}}const $R=UR;var sy="@firebase/auth",oy="1.1.0";/**
 * @license
 * Copyright 2020 Google LLC
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */class zR{constructor(e){this.auth=e,this.internalListeners=new Map}getUid(){var e;return this.assertAuthConfigured(),((e=this.auth.currentUser)===null||e===void 0?void 0:e.uid)||null}async getToken(e){return this.assertAuthConfigured(),await this.auth._initializationPromise,this.auth.currentUser?{accessToken:await this.auth.currentUser.getIdToken(e)}:null}addAuthTokenListener(e){if(this.assertAuthConfigured(),this.internalListeners.has(e))return;const n=this.auth.onIdTokenChanged(r=>{e((r==null?void 0:r.stsTokenManager.accessToken)||null)});this.internalListeners.set(e,n),this.updateProactiveRefresh()}removeAuthTokenListener(e){this.assertAuthConfigured();const n=this.internalListeners.get(e);n&&(this.internalListeners.delete(e),n(),this.updateProactiveRefresh())}assertAuthConfigured(){Q(this.auth._initializationPromise,"dependent-sdk-initialized-before-auth")}updateProactiveRefresh(){this.internalListeners.size>0?this.auth._startProactiveRefresh():this.auth._stopProactiveRefresh()}}/**
 * @license
 * Copyright 2020 Google LLC
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */function BR(t){switch(t){case"Node":return"node";case"ReactNative":return"rn";case"Worker":return"webworker";case"Cordova":return"cordova";default:return}}function HR(t){ks(new yi("auth",(e,{options:n})=>{const r=e.getProvider("app").getImmediate(),i=e.getProvider("heartbeat"),s=e.getProvider("app-check-internal"),{apiKey:o,authDomain:a}=r.options;Q(o&&!o.includes(":"),"invalid-api-key",{appName:r.name});const l={apiKey:o,authDomain:a,clientPlatform:t,apiHost:"identitytoolkit.googleapis.com",tokenApiHost:"securetoken.googleapis.com",apiScheme:"https",sdkClientVersion:ww(t)},u=new SP(r,i,s,l);return AP(u,n),u},"PUBLIC").setInstantiationMode("EXPLICIT").setInstanceCreatedCallback((e,n,r)=>{e.getProvider("auth-internal").initialize()})),ks(new yi("auth-internal",e=>{const n=ba(e.getProvider("auth").getImmediate());return(r=>new zR(r))(n)},"PRIVATE").setInstantiationMode("EXPLICIT")),br(sy,oy,BR(t)),br(sy,oy,"esm2017")}/**
 * @license
 * Copyright 2021 Google LLC
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */const WR=5*60,KR=J_("authIdTokenMaxAge")||WR;let ay=null;const GR=t=>async e=>{const n=e&&await e.getIdTokenResult(),r=n&&(new Date().getTime()-Date.parse(n.issuedAtTime))/1e3;if(r&&r>KR)return;const i=n==null?void 0:n.token;ay!==i&&(ay=i,await fetch(t,{method:i?"POST":"DELETE",headers:i?{Authorization:`Bearer ${i}`}:{}}))};function qR(t=nw()){const e=Gp(t,"auth");if(e.isInitialized())return e.getImmediate();const n=kP(t,{popupRedirectResolver:$R,persistence:[JP,UP,bw]}),r=J_("authTokenSyncURL");if(r){const s=GR(r);LP(n,s,()=>s(n.currentUser)),MP(n,o=>s(o))}const i=Y_("auth");return i&&xP(n,`http://${i}`),n}HR("Browser");var QR="firebase",YR="10.1.0";/**
 * @license
 * Copyright 2020 Google LLC
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */br(QR,YR,"app");var XR=typeof globalThis<"u"?globalThis:typeof window<"u"?window:typeof global<"u"?global:typeof self<"u"?self:{},b,nm=nm||{},K=XR||self;function kc(t){var e=typeof t;return e=e!="object"?e:t?Array.isArray(t)?"array":e:"null",e=="array"||e=="object"&&typeof t.length=="number"}function Ma(t){var e=typeof t;return e=="object"&&t!=null||e=="function"}function JR(t){return Object.prototype.hasOwnProperty.call(t,Bf)&&t[Bf]||(t[Bf]=++ZR)}var Bf="closure_uid_"+(1e9*Math.random()>>>0),ZR=0;function eC(t,e,n){return t.call.apply(t.bind,arguments)}function tC(t,e,n){if(!t)throw Error();if(2<arguments.length){var r=Array.prototype.slice.call(arguments,2);return function(){var i=Array.prototype.slice.call(arguments);return Array.prototype.unshift.apply(i,r),t.apply(e,i)}}return function(){return t.apply(e,arguments)}}function gt(t,e,n){return Function.prototype.bind&&Function.prototype.bind.toString().indexOf("native code")!=-1?gt=eC:gt=tC,gt.apply(null,arguments)}function _l(t,e){var n=Array.prototype.slice.call(arguments,1);return function(){var r=n.slice();return r.push.apply(r,arguments),t.apply(this,r)}}function et(t,e){function n(){}n.prototype=e.prototype,t.$=e.prototype,t.prototype=new n,t.prototype.constructor=t,t.ac=function(r,i,s){for(var o=Array(arguments.length-2),a=2;a<arguments.length;a++)o[a-2]=arguments[a];return e.prototype[i].apply(r,o)}}function Wr(){this.s=this.s,this.o=this.o}var nC=0;Wr.prototype.s=!1;Wr.prototype.sa=function(){!this.s&&(this.s=!0,this.N(),nC!=0)&&JR(this)};Wr.prototype.N=function(){if(this.o)for(;this.o.length;)this.o.shift()()};const Fw=Array.prototype.indexOf?function(t,e){return Array.prototype.indexOf.call(t,e,void 0)}:function(t,e){if(typeof t=="string")return typeof e!="string"||e.length!=1?-1:t.indexOf(e,0);for(let n=0;n<t.length;n++)if(n in t&&t[n]===e)return n;return-1};function rm(t){const e=t.length;if(0<e){const n=Array(e);for(let r=0;r<e;r++)n[r]=t[r];return n}return[]}function ly(t,e){for(let n=1;n<arguments.length;n++){const r=arguments[n];if(kc(r)){const i=t.length||0,s=r.length||0;t.length=i+s;for(let o=0;o<s;o++)t[i+o]=r[o]}else t.push(r)}}function vt(t,e){this.type=t,this.g=this.target=e,this.defaultPrevented=!1}vt.prototype.h=function(){this.defaultPrevented=!0};var rC=function(){if(!K.addEventListener||!Object.defineProperty)return!1;var t=!1,e=Object.defineProperty({},"passive",{get:function(){t=!0}});try{K.addEventListener("test",()=>{},e),K.removeEventListener("test",()=>{},e)}catch{}return t}();function ta(t){return/^[\s\xa0]*$/.test(t)}function Ac(){var t=K.navigator;return t&&(t=t.userAgent)?t:""}function vn(t){return Ac().indexOf(t)!=-1}function im(t){return im[" "](t),t}im[" "]=function(){};function iC(t,e){var n=YC;return Object.prototype.hasOwnProperty.call(n,t)?n[t]:n[t]=e(t)}var sC=vn("Opera"),As=vn("Trident")||vn("MSIE"),Uw=vn("Edge"),fd=Uw||As,$w=vn("Gecko")&&!(Ac().toLowerCase().indexOf("webkit")!=-1&&!vn("Edge"))&&!(vn("Trident")||vn("MSIE"))&&!vn("Edge"),oC=Ac().toLowerCase().indexOf("webkit")!=-1&&!vn("Edge");function zw(){var t=K.document;return t?t.documentMode:void 0}var hd;e:{var Hf="",Wf=function(){var t=Ac();if($w)return/rv:([^\);]+)(\)|;)/.exec(t);if(Uw)return/Edge\/([\d\.]+)/.exec(t);if(As)return/\b(?:MSIE|rv)[: ]([^\);]+)(\)|;)/.exec(t);if(oC)return/WebKit\/(\S+)/.exec(t);if(sC)return/(?:Version)[ \/]?(\S+)/.exec(t)}();if(Wf&&(Hf=Wf?Wf[1]:""),As){var Kf=zw();if(Kf!=null&&Kf>parseFloat(Hf)){hd=String(Kf);break e}}hd=Hf}var dd;if(K.document&&As){var uy=zw();dd=uy||parseInt(hd,10)||void 0}else dd=void 0;var aC=dd;function na(t,e){if(vt.call(this,t?t.type:""),this.relatedTarget=this.g=this.target=null,this.button=this.screenY=this.screenX=this.clientY=this.clientX=0,this.key="",this.metaKey=this.shiftKey=this.altKey=this.ctrlKey=!1,this.state=null,this.pointerId=0,this.pointerType="",this.i=null,t){var n=this.type=t.type,r=t.changedTouches&&t.changedTouches.length?t.changedTouches[0]:null;if(this.target=t.target||t.srcElement,this.g=e,e=t.relatedTarget){if($w){e:{try{im(e.nodeName);var i=!0;break e}catch{}i=!1}i||(e=null)}}else n=="mouseover"?e=t.fromElement:n=="mouseout"&&(e=t.toElement);this.relatedTarget=e,r?(this.clientX=r.clientX!==void 0?r.clientX:r.pageX,this.clientY=r.clientY!==void 0?r.clientY:r.pageY,this.screenX=r.screenX||0,this.screenY=r.screenY||0):(this.clientX=t.clientX!==void 0?t.clientX:t.pageX,this.clientY=t.clientY!==void 0?t.clientY:t.pageY,this.screenX=t.screenX||0,this.screenY=t.screenY||0),this.button=t.button,this.key=t.key||"",this.ctrlKey=t.ctrlKey,this.altKey=t.altKey,this.shiftKey=t.shiftKey,this.metaKey=t.metaKey,this.pointerId=t.pointerId||0,this.pointerType=typeof t.pointerType=="string"?t.pointerType:lC[t.pointerType]||"",this.state=t.state,this.i=t,t.defaultPrevented&&na.$.h.call(this)}}et(na,vt);var lC={2:"touch",3:"pen",4:"mouse"};na.prototype.h=function(){na.$.h.call(this);var t=this.i;t.preventDefault?t.preventDefault():t.returnValue=!1};var La="closure_listenable_"+(1e6*Math.random()|0),uC=0;function cC(t,e,n,r,i){this.listener=t,this.proxy=null,this.src=e,this.type=n,this.capture=!!r,this.la=i,this.key=++uC,this.fa=this.ia=!1}function xc(t){t.fa=!0,t.listener=null,t.proxy=null,t.src=null,t.la=null}function sm(t,e,n){for(const r in t)e.call(n,t[r],r,t)}function fC(t,e){for(const n in t)e.call(void 0,t[n],n,t)}function Bw(t){const e={};for(const n in t)e[n]=t[n];return e}const cy="constructor hasOwnProperty isPrototypeOf propertyIsEnumerable toLocaleString toString valueOf".split(" ");function Hw(t,e){let n,r;for(let i=1;i<arguments.length;i++){r=arguments[i];for(n in r)t[n]=r[n];for(let s=0;s<cy.length;s++)n=cy[s],Object.prototype.hasOwnProperty.call(r,n)&&(t[n]=r[n])}}function Pc(t){this.src=t,this.g={},this.h=0}Pc.prototype.add=function(t,e,n,r,i){var s=t.toString();t=this.g[s],t||(t=this.g[s]=[],this.h++);var o=md(t,e,r,i);return-1<o?(e=t[o],n||(e.ia=!1)):(e=new cC(e,this.src,s,!!r,i),e.ia=n,t.push(e)),e};function pd(t,e){var n=e.type;if(n in t.g){var r=t.g[n],i=Fw(r,e),s;(s=0<=i)&&Array.prototype.splice.call(r,i,1),s&&(xc(e),t.g[n].length==0&&(delete t.g[n],t.h--))}}function md(t,e,n,r){for(var i=0;i<t.length;++i){var s=t[i];if(!s.fa&&s.listener==e&&s.capture==!!n&&s.la==r)return i}return-1}var om="closure_lm_"+(1e6*Math.random()|0),Gf={};function Ww(t,e,n,r,i){if(r&&r.once)return Gw(t,e,n,r,i);if(Array.isArray(e)){for(var s=0;s<e.length;s++)Ww(t,e[s],n,r,i);return null}return n=um(n),t&&t[La]?t.O(e,n,Ma(r)?!!r.capture:!!r,i):Kw(t,e,n,!1,r,i)}function Kw(t,e,n,r,i,s){if(!e)throw Error("Invalid event type");var o=Ma(i)?!!i.capture:!!i,a=lm(t);if(a||(t[om]=a=new Pc(t)),n=a.add(e,n,r,o,s),n.proxy)return n;if(r=hC(),n.proxy=r,r.src=t,r.listener=n,t.addEventListener)rC||(i=o),i===void 0&&(i=!1),t.addEventListener(e.toString(),r,i);else if(t.attachEvent)t.attachEvent(Qw(e.toString()),r);else if(t.addListener&&t.removeListener)t.addListener(r);else throw Error("addEventListener and attachEvent are unavailable.");return n}function hC(){function t(n){return e.call(t.src,t.listener,n)}const e=dC;return t}function Gw(t,e,n,r,i){if(Array.isArray(e)){for(var s=0;s<e.length;s++)Gw(t,e[s],n,r,i);return null}return n=um(n),t&&t[La]?t.P(e,n,Ma(r)?!!r.capture:!!r,i):Kw(t,e,n,!0,r,i)}function qw(t,e,n,r,i){if(Array.isArray(e))for(var s=0;s<e.length;s++)qw(t,e[s],n,r,i);else r=Ma(r)?!!r.capture:!!r,n=um(n),t&&t[La]?(t=t.i,e=String(e).toString(),e in t.g&&(s=t.g[e],n=md(s,n,r,i),-1<n&&(xc(s[n]),Array.prototype.splice.call(s,n,1),s.length==0&&(delete t.g[e],t.h--)))):t&&(t=lm(t))&&(e=t.g[e.toString()],t=-1,e&&(t=md(e,n,r,i)),(n=-1<t?e[t]:null)&&am(n))}function am(t){if(typeof t!="number"&&t&&!t.fa){var e=t.src;if(e&&e[La])pd(e.i,t);else{var n=t.type,r=t.proxy;e.removeEventListener?e.removeEventListener(n,r,t.capture):e.detachEvent?e.detachEvent(Qw(n),r):e.addListener&&e.removeListener&&e.removeListener(r),(n=lm(e))?(pd(n,t),n.h==0&&(n.src=null,e[om]=null)):xc(t)}}}function Qw(t){return t in Gf?Gf[t]:Gf[t]="on"+t}function dC(t,e){if(t.fa)t=!0;else{e=new na(e,this);var n=t.listener,r=t.la||t.src;t.ia&&am(t),t=n.call(r,e)}return t}function lm(t){return t=t[om],t instanceof Pc?t:null}var qf="__closure_events_fn_"+(1e9*Math.random()>>>0);function um(t){return typeof t=="function"?t:(t[qf]||(t[qf]=function(e){return t.handleEvent(e)}),t[qf])}function Je(){Wr.call(this),this.i=new Pc(this),this.S=this,this.J=null}et(Je,Wr);Je.prototype[La]=!0;Je.prototype.removeEventListener=function(t,e,n,r){qw(this,t,e,n,r)};function ot(t,e){var n,r=t.J;if(r)for(n=[];r;r=r.J)n.push(r);if(t=t.S,r=e.type||e,typeof e=="string")e=new vt(e,t);else if(e instanceof vt)e.target=e.target||t;else{var i=e;e=new vt(r,t),Hw(e,i)}if(i=!0,n)for(var s=n.length-1;0<=s;s--){var o=e.g=n[s];i=wl(o,r,!0,e)&&i}if(o=e.g=t,i=wl(o,r,!0,e)&&i,i=wl(o,r,!1,e)&&i,n)for(s=0;s<n.length;s++)o=e.g=n[s],i=wl(o,r,!1,e)&&i}Je.prototype.N=function(){if(Je.$.N.call(this),this.i){var t=this.i,e;for(e in t.g){for(var n=t.g[e],r=0;r<n.length;r++)xc(n[r]);delete t.g[e],t.h--}}this.J=null};Je.prototype.O=function(t,e,n,r){return this.i.add(String(t),e,!1,n,r)};Je.prototype.P=function(t,e,n,r){return this.i.add(String(t),e,!0,n,r)};function wl(t,e,n,r){if(e=t.i.g[String(e)],!e)return!0;e=e.concat();for(var i=!0,s=0;s<e.length;++s){var o=e[s];if(o&&!o.fa&&o.capture==n){var a=o.listener,l=o.la||o.src;o.ia&&pd(t.i,o),i=a.call(l,r)!==!1&&i}}return i&&!r.defaultPrevented}var cm=K.JSON.stringify;class pC{constructor(e,n){this.i=e,this.j=n,this.h=0,this.g=null}get(){let e;return 0<this.h?(this.h--,e=this.g,this.g=e.next,e.next=null):e=this.i(),e}}function mC(){var t=fm;let e=null;return t.g&&(e=t.g,t.g=t.g.next,t.g||(t.h=null),e.next=null),e}class gC{constructor(){this.h=this.g=null}add(e,n){const r=Yw.get();r.set(e,n),this.h?this.h.next=r:this.g=r,this.h=r}}var Yw=new pC(()=>new vC,t=>t.reset());class vC{constructor(){this.next=this.g=this.h=null}set(e,n){this.h=e,this.g=n,this.next=null}reset(){this.next=this.g=this.h=null}}function yC(t){var e=1;t=t.split(":");const n=[];for(;0<e&&t.length;)n.push(t.shift()),e--;return t.length&&n.push(t.join(":")),n}function _C(t){K.setTimeout(()=>{throw t},0)}let ra,ia=!1,fm=new gC,Xw=()=>{const t=K.Promise.resolve(void 0);ra=()=>{t.then(wC)}};var wC=()=>{for(var t;t=mC();){try{t.h.call(t.g)}catch(n){_C(n)}var e=Yw;e.j(t),100>e.h&&(e.h++,t.next=e.g,e.g=t)}ia=!1};function Rc(t,e){Je.call(this),this.h=t||1,this.g=e||K,this.j=gt(this.qb,this),this.l=Date.now()}et(Rc,Je);b=Rc.prototype;b.ga=!1;b.T=null;b.qb=function(){if(this.ga){var t=Date.now()-this.l;0<t&&t<.8*this.h?this.T=this.g.setTimeout(this.j,this.h-t):(this.T&&(this.g.clearTimeout(this.T),this.T=null),ot(this,"tick"),this.ga&&(hm(this),this.start()))}};b.start=function(){this.ga=!0,this.T||(this.T=this.g.setTimeout(this.j,this.h),this.l=Date.now())};function hm(t){t.ga=!1,t.T&&(t.g.clearTimeout(t.T),t.T=null)}b.N=function(){Rc.$.N.call(this),hm(this),delete this.g};function dm(t,e,n){if(typeof t=="function")n&&(t=gt(t,n));else if(t&&typeof t.handleEvent=="function")t=gt(t.handleEvent,t);else throw Error("Invalid listener argument");return 2147483647<Number(e)?-1:K.setTimeout(t,e||0)}function Jw(t){t.g=dm(()=>{t.g=null,t.i&&(t.i=!1,Jw(t))},t.j);const e=t.h;t.h=null,t.m.apply(null,e)}class EC extends Wr{constructor(e,n){super(),this.m=e,this.j=n,this.h=null,this.i=!1,this.g=null}l(e){this.h=arguments,this.g?this.i=!0:Jw(this)}N(){super.N(),this.g&&(K.clearTimeout(this.g),this.g=null,this.i=!1,this.h=null)}}function sa(t){Wr.call(this),this.h=t,this.g={}}et(sa,Wr);var fy=[];function Zw(t,e,n,r){Array.isArray(n)||(n&&(fy[0]=n.toString()),n=fy);for(var i=0;i<n.length;i++){var s=Ww(e,n[i],r||t.handleEvent,!1,t.h||t);if(!s)break;t.g[s.key]=s}}function eE(t){sm(t.g,function(e,n){this.g.hasOwnProperty(n)&&am(e)},t),t.g={}}sa.prototype.N=function(){sa.$.N.call(this),eE(this)};sa.prototype.handleEvent=function(){throw Error("EventHandler.handleEvent not implemented")};function Cc(){this.g=!0}Cc.prototype.Ea=function(){this.g=!1};function TC(t,e,n,r,i,s){t.info(function(){if(t.g)if(s)for(var o="",a=s.split("&"),l=0;l<a.length;l++){var u=a[l].split("=");if(1<u.length){var c=u[0];u=u[1];var h=c.split("_");o=2<=h.length&&h[1]=="type"?o+(c+"="+u+"&"):o+(c+"=redacted&")}}else o=null;else o=s;return"XMLHTTP REQ ("+r+") [attempt "+i+"]: "+e+`
`+n+`
`+o})}function IC(t,e,n,r,i,s,o){t.info(function(){return"XMLHTTP RESP ("+r+") [ attempt "+i+"]: "+e+`
`+n+`
`+s+" "+o})}function Zi(t,e,n,r){t.info(function(){return"XMLHTTP TEXT ("+e+"): "+kC(t,n)+(r?" "+r:"")})}function SC(t,e){t.info(function(){return"TIMEOUT: "+e})}Cc.prototype.info=function(){};function kC(t,e){if(!t.g)return e;if(!e)return null;try{var n=JSON.parse(e);if(n){for(t=0;t<n.length;t++)if(Array.isArray(n[t])){var r=n[t];if(!(2>r.length)){var i=r[1];if(Array.isArray(i)&&!(1>i.length)){var s=i[0];if(s!="noop"&&s!="stop"&&s!="close")for(var o=1;o<i.length;o++)i[o]=""}}}}return cm(n)}catch{return e}}var Ci={},hy=null;function Nc(){return hy=hy||new Je}Ci.Ta="serverreachability";function tE(t){vt.call(this,Ci.Ta,t)}et(tE,vt);function oa(t){const e=Nc();ot(e,new tE(e))}Ci.STAT_EVENT="statevent";function nE(t,e){vt.call(this,Ci.STAT_EVENT,t),this.stat=e}et(nE,vt);function Et(t){const e=Nc();ot(e,new nE(e,t))}Ci.Ua="timingevent";function rE(t,e){vt.call(this,Ci.Ua,t),this.size=e}et(rE,vt);function ja(t,e){if(typeof t!="function")throw Error("Fn must not be null and must be a function");return K.setTimeout(function(){t()},e)}var bc={NO_ERROR:0,rb:1,Eb:2,Db:3,yb:4,Cb:5,Fb:6,Qa:7,TIMEOUT:8,Ib:9},iE={wb:"complete",Sb:"success",Ra:"error",Qa:"abort",Kb:"ready",Lb:"readystatechange",TIMEOUT:"timeout",Gb:"incrementaldata",Jb:"progress",zb:"downloadprogress",$b:"uploadprogress"};function pm(){}pm.prototype.h=null;function dy(t){return t.h||(t.h=t.i())}function sE(){}var Va={OPEN:"a",vb:"b",Ra:"c",Hb:"d"};function mm(){vt.call(this,"d")}et(mm,vt);function gm(){vt.call(this,"c")}et(gm,vt);var gd;function Oc(){}et(Oc,pm);Oc.prototype.g=function(){return new XMLHttpRequest};Oc.prototype.i=function(){return{}};gd=new Oc;function Fa(t,e,n,r){this.l=t,this.j=e,this.m=n,this.W=r||1,this.U=new sa(this),this.P=AC,t=fd?125:void 0,this.V=new Rc(t),this.I=null,this.i=!1,this.s=this.A=this.v=this.L=this.G=this.Y=this.B=null,this.F=[],this.g=null,this.C=0,this.o=this.u=null,this.ca=-1,this.J=!1,this.O=0,this.M=null,this.ba=this.K=this.aa=this.S=!1,this.h=new oE}function oE(){this.i=null,this.g="",this.h=!1}var AC=45e3,vd={},Lu={};b=Fa.prototype;b.setTimeout=function(t){this.P=t};function yd(t,e,n){t.L=1,t.v=Mc(qn(e)),t.s=n,t.S=!0,aE(t,null)}function aE(t,e){t.G=Date.now(),Ua(t),t.A=qn(t.v);var n=t.A,r=t.W;Array.isArray(r)||(r=[String(r)]),mE(n.i,"t",r),t.C=0,n=t.l.J,t.h=new oE,t.g=LE(t.l,n?e:null,!t.s),0<t.O&&(t.M=new EC(gt(t.Pa,t,t.g),t.O)),Zw(t.U,t.g,"readystatechange",t.nb),e=t.I?Bw(t.I):{},t.s?(t.u||(t.u="POST"),e["Content-Type"]="application/x-www-form-urlencoded",t.g.ha(t.A,t.u,t.s,e)):(t.u="GET",t.g.ha(t.A,t.u,null,e)),oa(),TC(t.j,t.u,t.A,t.m,t.W,t.s)}b.nb=function(t){t=t.target;const e=this.M;e&&yn(t)==3?e.l():this.Pa(t)};b.Pa=function(t){try{if(t==this.g)e:{const c=yn(this.g);var e=this.g.Ia();const h=this.g.da();if(!(3>c)&&(c!=3||fd||this.g&&(this.h.h||this.g.ja()||vy(this.g)))){this.J||c!=4||e==7||(e==8||0>=h?oa(3):oa(2)),Dc(this);var n=this.g.da();this.ca=n;t:if(lE(this)){var r=vy(this.g);t="";var i=r.length,s=yn(this.g)==4;if(!this.h.i){if(typeof TextDecoder>"u"){ni(this),Ao(this);var o="";break t}this.h.i=new K.TextDecoder}for(e=0;e<i;e++)this.h.h=!0,t+=this.h.i.decode(r[e],{stream:s&&e==i-1});r.splice(0,i),this.h.g+=t,this.C=0,o=this.h.g}else o=this.g.ja();if(this.i=n==200,IC(this.j,this.u,this.A,this.m,this.W,c,n),this.i){if(this.aa&&!this.K){t:{if(this.g){var a,l=this.g;if((a=l.g?l.g.getResponseHeader("X-HTTP-Initial-Response"):null)&&!ta(a)){var u=a;break t}}u=null}if(n=u)Zi(this.j,this.m,n,"Initial handshake response via X-HTTP-Initial-Response"),this.K=!0,_d(this,n);else{this.i=!1,this.o=3,Et(12),ni(this),Ao(this);break e}}this.S?(uE(this,c,o),fd&&this.i&&c==3&&(Zw(this.U,this.V,"tick",this.mb),this.V.start())):(Zi(this.j,this.m,o,null),_d(this,o)),c==4&&ni(this),this.i&&!this.J&&(c==4?bE(this.l,this):(this.i=!1,Ua(this)))}else GC(this.g),n==400&&0<o.indexOf("Unknown SID")?(this.o=3,Et(12)):(this.o=0,Et(13)),ni(this),Ao(this)}}}catch{}finally{}};function lE(t){return t.g?t.u=="GET"&&t.L!=2&&t.l.Ha:!1}function uE(t,e,n){let r=!0,i;for(;!t.J&&t.C<n.length;)if(i=xC(t,n),i==Lu){e==4&&(t.o=4,Et(14),r=!1),Zi(t.j,t.m,null,"[Incomplete Response]");break}else if(i==vd){t.o=4,Et(15),Zi(t.j,t.m,n,"[Invalid Chunk]"),r=!1;break}else Zi(t.j,t.m,i,null),_d(t,i);lE(t)&&i!=Lu&&i!=vd&&(t.h.g="",t.C=0),e!=4||n.length!=0||t.h.h||(t.o=1,Et(16),r=!1),t.i=t.i&&r,r?0<n.length&&!t.ba&&(t.ba=!0,e=t.l,e.g==t&&e.ca&&!e.M&&(e.l.info("Great, no buffering proxy detected. Bytes received: "+n.length),Tm(e),e.M=!0,Et(11))):(Zi(t.j,t.m,n,"[Invalid Chunked Response]"),ni(t),Ao(t))}b.mb=function(){if(this.g){var t=yn(this.g),e=this.g.ja();this.C<e.length&&(Dc(this),uE(this,t,e),this.i&&t!=4&&Ua(this))}};function xC(t,e){var n=t.C,r=e.indexOf(`
`,n);return r==-1?Lu:(n=Number(e.substring(n,r)),isNaN(n)?vd:(r+=1,r+n>e.length?Lu:(e=e.slice(r,r+n),t.C=r+n,e)))}b.cancel=function(){this.J=!0,ni(this)};function Ua(t){t.Y=Date.now()+t.P,cE(t,t.P)}function cE(t,e){if(t.B!=null)throw Error("WatchDog timer not null");t.B=ja(gt(t.lb,t),e)}function Dc(t){t.B&&(K.clearTimeout(t.B),t.B=null)}b.lb=function(){this.B=null;const t=Date.now();0<=t-this.Y?(SC(this.j,this.A),this.L!=2&&(oa(),Et(17)),ni(this),this.o=2,Ao(this)):cE(this,this.Y-t)};function Ao(t){t.l.H==0||t.J||bE(t.l,t)}function ni(t){Dc(t);var e=t.M;e&&typeof e.sa=="function"&&e.sa(),t.M=null,hm(t.V),eE(t.U),t.g&&(e=t.g,t.g=null,e.abort(),e.sa())}function _d(t,e){try{var n=t.l;if(n.H!=0&&(n.g==t||wd(n.i,t))){if(!t.K&&wd(n.i,t)&&n.H==3){try{var r=n.Ja.g.parse(e)}catch{r=null}if(Array.isArray(r)&&r.length==3){var i=r;if(i[0]==0){e:if(!n.u){if(n.g)if(n.g.G+3e3<t.G)Fu(n),Vc(n);else break e;Em(n),Et(18)}}else n.Fa=i[1],0<n.Fa-n.V&&37500>i[2]&&n.G&&n.A==0&&!n.v&&(n.v=ja(gt(n.ib,n),6e3));if(1>=yE(n.i)&&n.oa){try{n.oa()}catch{}n.oa=void 0}}else ri(n,11)}else if((t.K||n.g==t)&&Fu(n),!ta(e))for(i=n.Ja.g.parse(e),e=0;e<i.length;e++){let u=i[e];if(n.V=u[0],u=u[1],n.H==2)if(u[0]=="c"){n.K=u[1],n.pa=u[2];const c=u[3];c!=null&&(n.ra=c,n.l.info("VER="+n.ra));const h=u[4];h!=null&&(n.Ga=h,n.l.info("SVER="+n.Ga));const d=u[5];d!=null&&typeof d=="number"&&0<d&&(r=1.5*d,n.L=r,n.l.info("backChannelRequestTimeoutMs_="+r)),r=n;const y=t.g;if(y){const _=y.g?y.g.getResponseHeader("X-Client-Wire-Protocol"):null;if(_){var s=r.i;s.g||_.indexOf("spdy")==-1&&_.indexOf("quic")==-1&&_.indexOf("h2")==-1||(s.j=s.l,s.g=new Set,s.h&&(vm(s,s.h),s.h=null))}if(r.F){const w=y.g?y.g.getResponseHeader("X-HTTP-Session-Id"):null;w&&(r.Da=w,ve(r.I,r.F,w))}}n.H=3,n.h&&n.h.Ba(),n.ca&&(n.S=Date.now()-t.G,n.l.info("Handshake RTT: "+n.S+"ms")),r=n;var o=t;if(r.wa=ME(r,r.J?r.pa:null,r.Y),o.K){_E(r.i,o);var a=o,l=r.L;l&&a.setTimeout(l),a.B&&(Dc(a),Ua(a)),r.g=o}else CE(r);0<n.j.length&&Fc(n)}else u[0]!="stop"&&u[0]!="close"||ri(n,7);else n.H==3&&(u[0]=="stop"||u[0]=="close"?u[0]=="stop"?ri(n,7):wm(n):u[0]!="noop"&&n.h&&n.h.Aa(u),n.A=0)}}oa(4)}catch{}}function PC(t){if(t.Z&&typeof t.Z=="function")return t.Z();if(typeof Map<"u"&&t instanceof Map||typeof Set<"u"&&t instanceof Set)return Array.from(t.values());if(typeof t=="string")return t.split("");if(kc(t)){for(var e=[],n=t.length,r=0;r<n;r++)e.push(t[r]);return e}e=[],n=0;for(r in t)e[n++]=t[r];return e}function RC(t){if(t.ta&&typeof t.ta=="function")return t.ta();if(!t.Z||typeof t.Z!="function"){if(typeof Map<"u"&&t instanceof Map)return Array.from(t.keys());if(!(typeof Set<"u"&&t instanceof Set)){if(kc(t)||typeof t=="string"){var e=[];t=t.length;for(var n=0;n<t;n++)e.push(n);return e}e=[],n=0;for(const r in t)e[n++]=r;return e}}}function fE(t,e){if(t.forEach&&typeof t.forEach=="function")t.forEach(e,void 0);else if(kc(t)||typeof t=="string")Array.prototype.forEach.call(t,e,void 0);else for(var n=RC(t),r=PC(t),i=r.length,s=0;s<i;s++)e.call(void 0,r[s],n&&n[s],t)}var hE=RegExp("^(?:([^:/?#.]+):)?(?://(?:([^\\\\/?#]*)@)?([^\\\\/?#]*?)(?::([0-9]+))?(?=[\\\\/?#]|$))?([^?#]+)?(?:\\?([^#]*))?(?:#([\\s\\S]*))?$");function CC(t,e){if(t){t=t.split("&");for(var n=0;n<t.length;n++){var r=t[n].indexOf("="),i=null;if(0<=r){var s=t[n].substring(0,r);i=t[n].substring(r+1)}else s=t[n];e(s,i?decodeURIComponent(i.replace(/\+/g," ")):"")}}}function ci(t){if(this.g=this.s=this.j="",this.m=null,this.o=this.l="",this.h=!1,t instanceof ci){this.h=t.h,ju(this,t.j),this.s=t.s,this.g=t.g,Vu(this,t.m),this.l=t.l;var e=t.i,n=new aa;n.i=e.i,e.g&&(n.g=new Map(e.g),n.h=e.h),py(this,n),this.o=t.o}else t&&(e=String(t).match(hE))?(this.h=!1,ju(this,e[1]||"",!0),this.s=fo(e[2]||""),this.g=fo(e[3]||"",!0),Vu(this,e[4]),this.l=fo(e[5]||"",!0),py(this,e[6]||"",!0),this.o=fo(e[7]||"")):(this.h=!1,this.i=new aa(null,this.h))}ci.prototype.toString=function(){var t=[],e=this.j;e&&t.push(ho(e,my,!0),":");var n=this.g;return(n||e=="file")&&(t.push("//"),(e=this.s)&&t.push(ho(e,my,!0),"@"),t.push(encodeURIComponent(String(n)).replace(/%25([0-9a-fA-F]{2})/g,"%$1")),n=this.m,n!=null&&t.push(":",String(n))),(n=this.l)&&(this.g&&n.charAt(0)!="/"&&t.push("/"),t.push(ho(n,n.charAt(0)=="/"?OC:bC,!0))),(n=this.i.toString())&&t.push("?",n),(n=this.o)&&t.push("#",ho(n,MC)),t.join("")};function qn(t){return new ci(t)}function ju(t,e,n){t.j=n?fo(e,!0):e,t.j&&(t.j=t.j.replace(/:$/,""))}function Vu(t,e){if(e){if(e=Number(e),isNaN(e)||0>e)throw Error("Bad port number "+e);t.m=e}else t.m=null}function py(t,e,n){e instanceof aa?(t.i=e,LC(t.i,t.h)):(n||(e=ho(e,DC)),t.i=new aa(e,t.h))}function ve(t,e,n){t.i.set(e,n)}function Mc(t){return ve(t,"zx",Math.floor(2147483648*Math.random()).toString(36)+Math.abs(Math.floor(2147483648*Math.random())^Date.now()).toString(36)),t}function fo(t,e){return t?e?decodeURI(t.replace(/%25/g,"%2525")):decodeURIComponent(t):""}function ho(t,e,n){return typeof t=="string"?(t=encodeURI(t).replace(e,NC),n&&(t=t.replace(/%25([0-9a-fA-F]{2})/g,"%$1")),t):null}function NC(t){return t=t.charCodeAt(0),"%"+(t>>4&15).toString(16)+(t&15).toString(16)}var my=/[#\/\?@]/g,bC=/[#\?:]/g,OC=/[#\?]/g,DC=/[#\?@]/g,MC=/#/g;function aa(t,e){this.h=this.g=null,this.i=t||null,this.j=!!e}function Kr(t){t.g||(t.g=new Map,t.h=0,t.i&&CC(t.i,function(e,n){t.add(decodeURIComponent(e.replace(/\+/g," ")),n)}))}b=aa.prototype;b.add=function(t,e){Kr(this),this.i=null,t=Fs(this,t);var n=this.g.get(t);return n||this.g.set(t,n=[]),n.push(e),this.h+=1,this};function dE(t,e){Kr(t),e=Fs(t,e),t.g.has(e)&&(t.i=null,t.h-=t.g.get(e).length,t.g.delete(e))}function pE(t,e){return Kr(t),e=Fs(t,e),t.g.has(e)}b.forEach=function(t,e){Kr(this),this.g.forEach(function(n,r){n.forEach(function(i){t.call(e,i,r,this)},this)},this)};b.ta=function(){Kr(this);const t=Array.from(this.g.values()),e=Array.from(this.g.keys()),n=[];for(let r=0;r<e.length;r++){const i=t[r];for(let s=0;s<i.length;s++)n.push(e[r])}return n};b.Z=function(t){Kr(this);let e=[];if(typeof t=="string")pE(this,t)&&(e=e.concat(this.g.get(Fs(this,t))));else{t=Array.from(this.g.values());for(let n=0;n<t.length;n++)e=e.concat(t[n])}return e};b.set=function(t,e){return Kr(this),this.i=null,t=Fs(this,t),pE(this,t)&&(this.h-=this.g.get(t).length),this.g.set(t,[e]),this.h+=1,this};b.get=function(t,e){return t?(t=this.Z(t),0<t.length?String(t[0]):e):e};function mE(t,e,n){dE(t,e),0<n.length&&(t.i=null,t.g.set(Fs(t,e),rm(n)),t.h+=n.length)}b.toString=function(){if(this.i)return this.i;if(!this.g)return"";const t=[],e=Array.from(this.g.keys());for(var n=0;n<e.length;n++){var r=e[n];const s=encodeURIComponent(String(r)),o=this.Z(r);for(r=0;r<o.length;r++){var i=s;o[r]!==""&&(i+="="+encodeURIComponent(String(o[r]))),t.push(i)}}return this.i=t.join("&")};function Fs(t,e){return e=String(e),t.j&&(e=e.toLowerCase()),e}function LC(t,e){e&&!t.j&&(Kr(t),t.i=null,t.g.forEach(function(n,r){var i=r.toLowerCase();r!=i&&(dE(this,r),mE(this,i,n))},t)),t.j=e}var jC=class{constructor(t,e){this.g=t,this.map=e}};function gE(t){this.l=t||VC,K.PerformanceNavigationTiming?(t=K.performance.getEntriesByType("navigation"),t=0<t.length&&(t[0].nextHopProtocol=="hq"||t[0].nextHopProtocol=="h2")):t=!!(K.g&&K.g.Ka&&K.g.Ka()&&K.g.Ka().ec),this.j=t?this.l:1,this.g=null,1<this.j&&(this.g=new Set),this.h=null,this.i=[]}var VC=10;function vE(t){return t.h?!0:t.g?t.g.size>=t.j:!1}function yE(t){return t.h?1:t.g?t.g.size:0}function wd(t,e){return t.h?t.h==e:t.g?t.g.has(e):!1}function vm(t,e){t.g?t.g.add(e):t.h=e}function _E(t,e){t.h&&t.h==e?t.h=null:t.g&&t.g.has(e)&&t.g.delete(e)}gE.prototype.cancel=function(){if(this.i=wE(this),this.h)this.h.cancel(),this.h=null;else if(this.g&&this.g.size!==0){for(const t of this.g.values())t.cancel();this.g.clear()}};function wE(t){if(t.h!=null)return t.i.concat(t.h.F);if(t.g!=null&&t.g.size!==0){let e=t.i;for(const n of t.g.values())e=e.concat(n.F);return e}return rm(t.i)}var FC=class{stringify(t){return K.JSON.stringify(t,void 0)}parse(t){return K.JSON.parse(t,void 0)}};function UC(){this.g=new FC}function $C(t,e,n){const r=n||"";try{fE(t,function(i,s){let o=i;Ma(i)&&(o=cm(i)),e.push(r+s+"="+encodeURIComponent(o))})}catch(i){throw e.push(r+"type="+encodeURIComponent("_badmap")),i}}function zC(t,e){const n=new Cc;if(K.Image){const r=new Image;r.onload=_l(El,n,r,"TestLoadImage: loaded",!0,e),r.onerror=_l(El,n,r,"TestLoadImage: error",!1,e),r.onabort=_l(El,n,r,"TestLoadImage: abort",!1,e),r.ontimeout=_l(El,n,r,"TestLoadImage: timeout",!1,e),K.setTimeout(function(){r.ontimeout&&r.ontimeout()},1e4),r.src=t}else e(!1)}function El(t,e,n,r,i){try{e.onload=null,e.onerror=null,e.onabort=null,e.ontimeout=null,i(r)}catch{}}function $a(t){this.l=t.fc||null,this.j=t.ob||!1}et($a,pm);$a.prototype.g=function(){return new Lc(this.l,this.j)};$a.prototype.i=function(t){return function(){return t}}({});function Lc(t,e){Je.call(this),this.F=t,this.u=e,this.m=void 0,this.readyState=ym,this.status=0,this.responseType=this.responseText=this.response=this.statusText="",this.onreadystatechange=null,this.v=new Headers,this.h=null,this.C="GET",this.B="",this.g=!1,this.A=this.j=this.l=null}et(Lc,Je);var ym=0;b=Lc.prototype;b.open=function(t,e){if(this.readyState!=ym)throw this.abort(),Error("Error reopening a connection");this.C=t,this.B=e,this.readyState=1,la(this)};b.send=function(t){if(this.readyState!=1)throw this.abort(),Error("need to call open() first. ");this.g=!0;const e={headers:this.v,method:this.C,credentials:this.m,cache:void 0};t&&(e.body=t),(this.F||K).fetch(new Request(this.B,e)).then(this.$a.bind(this),this.ka.bind(this))};b.abort=function(){this.response=this.responseText="",this.v=new Headers,this.status=0,this.j&&this.j.cancel("Request was aborted.").catch(()=>{}),1<=this.readyState&&this.g&&this.readyState!=4&&(this.g=!1,za(this)),this.readyState=ym};b.$a=function(t){if(this.g&&(this.l=t,this.h||(this.status=this.l.status,this.statusText=this.l.statusText,this.h=t.headers,this.readyState=2,la(this)),this.g&&(this.readyState=3,la(this),this.g)))if(this.responseType==="arraybuffer")t.arrayBuffer().then(this.Ya.bind(this),this.ka.bind(this));else if(typeof K.ReadableStream<"u"&&"body"in t){if(this.j=t.body.getReader(),this.u){if(this.responseType)throw Error('responseType must be empty for "streamBinaryChunks" mode responses.');this.response=[]}else this.response=this.responseText="",this.A=new TextDecoder;EE(this)}else t.text().then(this.Za.bind(this),this.ka.bind(this))};function EE(t){t.j.read().then(t.Xa.bind(t)).catch(t.ka.bind(t))}b.Xa=function(t){if(this.g){if(this.u&&t.value)this.response.push(t.value);else if(!this.u){var e=t.value?t.value:new Uint8Array(0);(e=this.A.decode(e,{stream:!t.done}))&&(this.response=this.responseText+=e)}t.done?za(this):la(this),this.readyState==3&&EE(this)}};b.Za=function(t){this.g&&(this.response=this.responseText=t,za(this))};b.Ya=function(t){this.g&&(this.response=t,za(this))};b.ka=function(){this.g&&za(this)};function za(t){t.readyState=4,t.l=null,t.j=null,t.A=null,la(t)}b.setRequestHeader=function(t,e){this.v.append(t,e)};b.getResponseHeader=function(t){return this.h&&this.h.get(t.toLowerCase())||""};b.getAllResponseHeaders=function(){if(!this.h)return"";const t=[],e=this.h.entries();for(var n=e.next();!n.done;)n=n.value,t.push(n[0]+": "+n[1]),n=e.next();return t.join(`\r
`)};function la(t){t.onreadystatechange&&t.onreadystatechange.call(t)}Object.defineProperty(Lc.prototype,"withCredentials",{get:function(){return this.m==="include"},set:function(t){this.m=t?"include":"same-origin"}});var BC=K.JSON.parse;function Ce(t){Je.call(this),this.headers=new Map,this.u=t||null,this.h=!1,this.C=this.g=null,this.I="",this.m=0,this.j="",this.l=this.G=this.v=this.F=!1,this.B=0,this.A=null,this.K=TE,this.L=this.M=!1}et(Ce,Je);var TE="",HC=/^https?$/i,WC=["POST","PUT"];b=Ce.prototype;b.Oa=function(t){this.M=t};b.ha=function(t,e,n,r){if(this.g)throw Error("[goog.net.XhrIo] Object is active with another request="+this.I+"; newUri="+t);e=e?e.toUpperCase():"GET",this.I=t,this.j="",this.m=0,this.F=!1,this.h=!0,this.g=this.u?this.u.g():gd.g(),this.C=this.u?dy(this.u):dy(gd),this.g.onreadystatechange=gt(this.La,this);try{this.G=!0,this.g.open(e,String(t),!0),this.G=!1}catch(s){gy(this,s);return}if(t=n||"",n=new Map(this.headers),r)if(Object.getPrototypeOf(r)===Object.prototype)for(var i in r)n.set(i,r[i]);else if(typeof r.keys=="function"&&typeof r.get=="function")for(const s of r.keys())n.set(s,r.get(s));else throw Error("Unknown input type for opt_headers: "+String(r));r=Array.from(n.keys()).find(s=>s.toLowerCase()=="content-type"),i=K.FormData&&t instanceof K.FormData,!(0<=Fw(WC,e))||r||i||n.set("Content-Type","application/x-www-form-urlencoded;charset=utf-8");for(const[s,o]of n)this.g.setRequestHeader(s,o);this.K&&(this.g.responseType=this.K),"withCredentials"in this.g&&this.g.withCredentials!==this.M&&(this.g.withCredentials=this.M);try{kE(this),0<this.B&&((this.L=KC(this.g))?(this.g.timeout=this.B,this.g.ontimeout=gt(this.ua,this)):this.A=dm(this.ua,this.B,this)),this.v=!0,this.g.send(t),this.v=!1}catch(s){gy(this,s)}};function KC(t){return As&&typeof t.timeout=="number"&&t.ontimeout!==void 0}b.ua=function(){typeof nm<"u"&&this.g&&(this.j="Timed out after "+this.B+"ms, aborting",this.m=8,ot(this,"timeout"),this.abort(8))};function gy(t,e){t.h=!1,t.g&&(t.l=!0,t.g.abort(),t.l=!1),t.j=e,t.m=5,IE(t),jc(t)}function IE(t){t.F||(t.F=!0,ot(t,"complete"),ot(t,"error"))}b.abort=function(t){this.g&&this.h&&(this.h=!1,this.l=!0,this.g.abort(),this.l=!1,this.m=t||7,ot(this,"complete"),ot(this,"abort"),jc(this))};b.N=function(){this.g&&(this.h&&(this.h=!1,this.l=!0,this.g.abort(),this.l=!1),jc(this,!0)),Ce.$.N.call(this)};b.La=function(){this.s||(this.G||this.v||this.l?SE(this):this.kb())};b.kb=function(){SE(this)};function SE(t){if(t.h&&typeof nm<"u"&&(!t.C[1]||yn(t)!=4||t.da()!=2)){if(t.v&&yn(t)==4)dm(t.La,0,t);else if(ot(t,"readystatechange"),yn(t)==4){t.h=!1;try{const o=t.da();e:switch(o){case 200:case 201:case 202:case 204:case 206:case 304:case 1223:var e=!0;break e;default:e=!1}var n;if(!(n=e)){var r;if(r=o===0){var i=String(t.I).match(hE)[1]||null;!i&&K.self&&K.self.location&&(i=K.self.location.protocol.slice(0,-1)),r=!HC.test(i?i.toLowerCase():"")}n=r}if(n)ot(t,"complete"),ot(t,"success");else{t.m=6;try{var s=2<yn(t)?t.g.statusText:""}catch{s=""}t.j=s+" ["+t.da()+"]",IE(t)}}finally{jc(t)}}}}function jc(t,e){if(t.g){kE(t);const n=t.g,r=t.C[0]?()=>{}:null;t.g=null,t.C=null,e||ot(t,"ready");try{n.onreadystatechange=r}catch{}}}function kE(t){t.g&&t.L&&(t.g.ontimeout=null),t.A&&(K.clearTimeout(t.A),t.A=null)}b.isActive=function(){return!!this.g};function yn(t){return t.g?t.g.readyState:0}b.da=function(){try{return 2<yn(this)?this.g.status:-1}catch{return-1}};b.ja=function(){try{return this.g?this.g.responseText:""}catch{return""}};b.Wa=function(t){if(this.g){var e=this.g.responseText;return t&&e.indexOf(t)==0&&(e=e.substring(t.length)),BC(e)}};function vy(t){try{if(!t.g)return null;if("response"in t.g)return t.g.response;switch(t.K){case TE:case"text":return t.g.responseText;case"arraybuffer":if("mozResponseArrayBuffer"in t.g)return t.g.mozResponseArrayBuffer}return null}catch{return null}}function GC(t){const e={};t=(t.g&&2<=yn(t)&&t.g.getAllResponseHeaders()||"").split(`\r
`);for(let r=0;r<t.length;r++){if(ta(t[r]))continue;var n=yC(t[r]);const i=n[0];if(n=n[1],typeof n!="string")continue;n=n.trim();const s=e[i]||[];e[i]=s,s.push(n)}fC(e,function(r){return r.join(", ")})}b.Ia=function(){return this.m};b.Sa=function(){return typeof this.j=="string"?this.j:String(this.j)};function AE(t){let e="";return sm(t,function(n,r){e+=r,e+=":",e+=n,e+=`\r
`}),e}function _m(t,e,n){e:{for(r in n){var r=!1;break e}r=!0}r||(n=AE(n),typeof t=="string"?n!=null&&encodeURIComponent(String(n)):ve(t,e,n))}function so(t,e,n){return n&&n.internalChannelParams&&n.internalChannelParams[t]||e}function xE(t){this.Ga=0,this.j=[],this.l=new Cc,this.pa=this.wa=this.I=this.Y=this.g=this.Da=this.F=this.na=this.o=this.U=this.s=null,this.fb=this.W=0,this.cb=so("failFast",!1,t),this.G=this.v=this.u=this.m=this.h=null,this.aa=!0,this.Fa=this.V=-1,this.ba=this.A=this.C=0,this.ab=so("baseRetryDelayMs",5e3,t),this.hb=so("retryDelaySeedMs",1e4,t),this.eb=so("forwardChannelMaxRetries",2,t),this.xa=so("forwardChannelRequestTimeoutMs",2e4,t),this.va=t&&t.xmlHttpFactory||void 0,this.Ha=t&&t.dc||!1,this.L=void 0,this.J=t&&t.supportsCrossDomainXhr||!1,this.K="",this.i=new gE(t&&t.concurrentRequestLimit),this.Ja=new UC,this.P=t&&t.fastHandshake||!1,this.O=t&&t.encodeInitMessageHeaders||!1,this.P&&this.O&&(this.O=!1),this.bb=t&&t.bc||!1,t&&t.Ea&&this.l.Ea(),t&&t.forceLongPolling&&(this.aa=!1),this.ca=!this.P&&this.aa&&t&&t.detectBufferingProxy||!1,this.qa=void 0,t&&t.longPollingTimeout&&0<t.longPollingTimeout&&(this.qa=t.longPollingTimeout),this.oa=void 0,this.S=0,this.M=!1,this.ma=this.B=null}b=xE.prototype;b.ra=8;b.H=1;function wm(t){if(PE(t),t.H==3){var e=t.W++,n=qn(t.I);if(ve(n,"SID",t.K),ve(n,"RID",e),ve(n,"TYPE","terminate"),Ba(t,n),e=new Fa(t,t.l,e),e.L=2,e.v=Mc(qn(n)),n=!1,K.navigator&&K.navigator.sendBeacon)try{n=K.navigator.sendBeacon(e.v.toString(),"")}catch{}!n&&K.Image&&(new Image().src=e.v,n=!0),n||(e.g=LE(e.l,null),e.g.ha(e.v)),e.G=Date.now(),Ua(e)}DE(t)}function Vc(t){t.g&&(Tm(t),t.g.cancel(),t.g=null)}function PE(t){Vc(t),t.u&&(K.clearTimeout(t.u),t.u=null),Fu(t),t.i.cancel(),t.m&&(typeof t.m=="number"&&K.clearTimeout(t.m),t.m=null)}function Fc(t){if(!vE(t.i)&&!t.m){t.m=!0;var e=t.Na;ra||Xw(),ia||(ra(),ia=!0),fm.add(e,t),t.C=0}}function qC(t,e){return yE(t.i)>=t.i.j-(t.m?1:0)?!1:t.m?(t.j=e.F.concat(t.j),!0):t.H==1||t.H==2||t.C>=(t.cb?0:t.eb)?!1:(t.m=ja(gt(t.Na,t,e),OE(t,t.C)),t.C++,!0)}b.Na=function(t){if(this.m)if(this.m=null,this.H==1){if(!t){this.W=Math.floor(1e5*Math.random()),t=this.W++;const i=new Fa(this,this.l,t);let s=this.s;if(this.U&&(s?(s=Bw(s),Hw(s,this.U)):s=this.U),this.o!==null||this.O||(i.I=s,s=null),this.P)e:{for(var e=0,n=0;n<this.j.length;n++){t:{var r=this.j[n];if("__data__"in r.map&&(r=r.map.__data__,typeof r=="string")){r=r.length;break t}r=void 0}if(r===void 0)break;if(e+=r,4096<e){e=n;break e}if(e===4096||n===this.j.length-1){e=n+1;break e}}e=1e3}else e=1e3;e=RE(this,i,e),n=qn(this.I),ve(n,"RID",t),ve(n,"CVER",22),this.F&&ve(n,"X-HTTP-Session-Id",this.F),Ba(this,n),s&&(this.O?e="headers="+encodeURIComponent(String(AE(s)))+"&"+e:this.o&&_m(n,this.o,s)),vm(this.i,i),this.bb&&ve(n,"TYPE","init"),this.P?(ve(n,"$req",e),ve(n,"SID","null"),i.aa=!0,yd(i,n,null)):yd(i,n,e),this.H=2}}else this.H==3&&(t?yy(this,t):this.j.length==0||vE(this.i)||yy(this))};function yy(t,e){var n;e?n=e.m:n=t.W++;const r=qn(t.I);ve(r,"SID",t.K),ve(r,"RID",n),ve(r,"AID",t.V),Ba(t,r),t.o&&t.s&&_m(r,t.o,t.s),n=new Fa(t,t.l,n,t.C+1),t.o===null&&(n.I=t.s),e&&(t.j=e.F.concat(t.j)),e=RE(t,n,1e3),n.setTimeout(Math.round(.5*t.xa)+Math.round(.5*t.xa*Math.random())),vm(t.i,n),yd(n,r,e)}function Ba(t,e){t.na&&sm(t.na,function(n,r){ve(e,r,n)}),t.h&&fE({},function(n,r){ve(e,r,n)})}function RE(t,e,n){n=Math.min(t.j.length,n);var r=t.h?gt(t.h.Va,t.h,t):null;e:{var i=t.j;let s=-1;for(;;){const o=["count="+n];s==-1?0<n?(s=i[0].g,o.push("ofs="+s)):s=0:o.push("ofs="+s);let a=!0;for(let l=0;l<n;l++){let u=i[l].g;const c=i[l].map;if(u-=s,0>u)s=Math.max(0,i[l].g-100),a=!1;else try{$C(c,o,"req"+u+"_")}catch{r&&r(c)}}if(a){r=o.join("&");break e}}}return t=t.j.splice(0,n),e.F=t,r}function CE(t){if(!t.g&&!t.u){t.ba=1;var e=t.Ma;ra||Xw(),ia||(ra(),ia=!0),fm.add(e,t),t.A=0}}function Em(t){return t.g||t.u||3<=t.A?!1:(t.ba++,t.u=ja(gt(t.Ma,t),OE(t,t.A)),t.A++,!0)}b.Ma=function(){if(this.u=null,NE(this),this.ca&&!(this.M||this.g==null||0>=this.S)){var t=2*this.S;this.l.info("BP detection timer enabled: "+t),this.B=ja(gt(this.jb,this),t)}};b.jb=function(){this.B&&(this.B=null,this.l.info("BP detection timeout reached."),this.l.info("Buffering proxy detected and switch to long-polling!"),this.G=!1,this.M=!0,Et(10),Vc(this),NE(this))};function Tm(t){t.B!=null&&(K.clearTimeout(t.B),t.B=null)}function NE(t){t.g=new Fa(t,t.l,"rpc",t.ba),t.o===null&&(t.g.I=t.s),t.g.O=0;var e=qn(t.wa);ve(e,"RID","rpc"),ve(e,"SID",t.K),ve(e,"AID",t.V),ve(e,"CI",t.G?"0":"1"),!t.G&&t.qa&&ve(e,"TO",t.qa),ve(e,"TYPE","xmlhttp"),Ba(t,e),t.o&&t.s&&_m(e,t.o,t.s),t.L&&t.g.setTimeout(t.L);var n=t.g;t=t.pa,n.L=1,n.v=Mc(qn(e)),n.s=null,n.S=!0,aE(n,t)}b.ib=function(){this.v!=null&&(this.v=null,Vc(this),Em(this),Et(19))};function Fu(t){t.v!=null&&(K.clearTimeout(t.v),t.v=null)}function bE(t,e){var n=null;if(t.g==e){Fu(t),Tm(t),t.g=null;var r=2}else if(wd(t.i,e))n=e.F,_E(t.i,e),r=1;else return;if(t.H!=0){if(e.i)if(r==1){n=e.s?e.s.length:0,e=Date.now()-e.G;var i=t.C;r=Nc(),ot(r,new rE(r,n)),Fc(t)}else CE(t);else if(i=e.o,i==3||i==0&&0<e.ca||!(r==1&&qC(t,e)||r==2&&Em(t)))switch(n&&0<n.length&&(e=t.i,e.i=e.i.concat(n)),i){case 1:ri(t,5);break;case 4:ri(t,10);break;case 3:ri(t,6);break;default:ri(t,2)}}}function OE(t,e){let n=t.ab+Math.floor(Math.random()*t.hb);return t.isActive()||(n*=2),n*e}function ri(t,e){if(t.l.info("Error code "+e),e==2){var n=null;t.h&&(n=null);var r=gt(t.pb,t);n||(n=new ci("//www.google.com/images/cleardot.gif"),K.location&&K.location.protocol=="http"||ju(n,"https"),Mc(n)),zC(n.toString(),r)}else Et(2);t.H=0,t.h&&t.h.za(e),DE(t),PE(t)}b.pb=function(t){t?(this.l.info("Successfully pinged google.com"),Et(2)):(this.l.info("Failed to ping google.com"),Et(1))};function DE(t){if(t.H=0,t.ma=[],t.h){const e=wE(t.i);(e.length!=0||t.j.length!=0)&&(ly(t.ma,e),ly(t.ma,t.j),t.i.i.length=0,rm(t.j),t.j.length=0),t.h.ya()}}function ME(t,e,n){var r=n instanceof ci?qn(n):new ci(n);if(r.g!="")e&&(r.g=e+"."+r.g),Vu(r,r.m);else{var i=K.location;r=i.protocol,e=e?e+"."+i.hostname:i.hostname,i=+i.port;var s=new ci(null);r&&ju(s,r),e&&(s.g=e),i&&Vu(s,i),n&&(s.l=n),r=s}return n=t.F,e=t.Da,n&&e&&ve(r,n,e),ve(r,"VER",t.ra),Ba(t,r),r}function LE(t,e,n){if(e&&!t.J)throw Error("Can't create secondary domain capable XhrIo object.");return e=n&&t.Ha&&!t.va?new Ce(new $a({ob:!0})):new Ce(t.va),e.Oa(t.J),e}b.isActive=function(){return!!this.h&&this.h.isActive(this)};function jE(){}b=jE.prototype;b.Ba=function(){};b.Aa=function(){};b.za=function(){};b.ya=function(){};b.isActive=function(){return!0};b.Va=function(){};function Uu(){if(As&&!(10<=Number(aC)))throw Error("Environmental error: no available transport.")}Uu.prototype.g=function(t,e){return new Vt(t,e)};function Vt(t,e){Je.call(this),this.g=new xE(e),this.l=t,this.h=e&&e.messageUrlParams||null,t=e&&e.messageHeaders||null,e&&e.clientProtocolHeaderRequired&&(t?t["X-Client-Protocol"]="webchannel":t={"X-Client-Protocol":"webchannel"}),this.g.s=t,t=e&&e.initMessageHeaders||null,e&&e.messageContentType&&(t?t["X-WebChannel-Content-Type"]=e.messageContentType:t={"X-WebChannel-Content-Type":e.messageContentType}),e&&e.Ca&&(t?t["X-WebChannel-Client-Profile"]=e.Ca:t={"X-WebChannel-Client-Profile":e.Ca}),this.g.U=t,(t=e&&e.cc)&&!ta(t)&&(this.g.o=t),this.A=e&&e.supportsCrossDomainXhr||!1,this.v=e&&e.sendRawJson||!1,(e=e&&e.httpSessionIdParam)&&!ta(e)&&(this.g.F=e,t=this.h,t!==null&&e in t&&(t=this.h,e in t&&delete t[e])),this.j=new Us(this)}et(Vt,Je);Vt.prototype.m=function(){this.g.h=this.j,this.A&&(this.g.J=!0);var t=this.g,e=this.l,n=this.h||void 0;Et(0),t.Y=e,t.na=n||{},t.G=t.aa,t.I=ME(t,null,t.Y),Fc(t)};Vt.prototype.close=function(){wm(this.g)};Vt.prototype.u=function(t){var e=this.g;if(typeof t=="string"){var n={};n.__data__=t,t=n}else this.v&&(n={},n.__data__=cm(t),t=n);e.j.push(new jC(e.fb++,t)),e.H==3&&Fc(e)};Vt.prototype.N=function(){this.g.h=null,delete this.j,wm(this.g),delete this.g,Vt.$.N.call(this)};function VE(t){mm.call(this),t.__headers__&&(this.headers=t.__headers__,this.statusCode=t.__status__,delete t.__headers__,delete t.__status__);var e=t.__sm__;if(e){e:{for(const n in e){t=n;break e}t=void 0}(this.i=t)&&(t=this.i,e=e!==null&&t in e?e[t]:void 0),this.data=e}else this.data=t}et(VE,mm);function FE(){gm.call(this),this.status=1}et(FE,gm);function Us(t){this.g=t}et(Us,jE);Us.prototype.Ba=function(){ot(this.g,"a")};Us.prototype.Aa=function(t){ot(this.g,new VE(t))};Us.prototype.za=function(t){ot(this.g,new FE)};Us.prototype.ya=function(){ot(this.g,"b")};function QC(){this.blockSize=-1}function An(){this.blockSize=-1,this.blockSize=64,this.g=Array(4),this.m=Array(this.blockSize),this.i=this.h=0,this.reset()}et(An,QC);An.prototype.reset=function(){this.g[0]=1732584193,this.g[1]=4023233417,this.g[2]=2562383102,this.g[3]=271733878,this.i=this.h=0};function Qf(t,e,n){n||(n=0);var r=Array(16);if(typeof e=="string")for(var i=0;16>i;++i)r[i]=e.charCodeAt(n++)|e.charCodeAt(n++)<<8|e.charCodeAt(n++)<<16|e.charCodeAt(n++)<<24;else for(i=0;16>i;++i)r[i]=e[n++]|e[n++]<<8|e[n++]<<16|e[n++]<<24;e=t.g[0],n=t.g[1],i=t.g[2];var s=t.g[3],o=e+(s^n&(i^s))+r[0]+3614090360&4294967295;e=n+(o<<7&4294967295|o>>>25),o=s+(i^e&(n^i))+r[1]+3905402710&4294967295,s=e+(o<<12&4294967295|o>>>20),o=i+(n^s&(e^n))+r[2]+606105819&4294967295,i=s+(o<<17&4294967295|o>>>15),o=n+(e^i&(s^e))+r[3]+3250441966&4294967295,n=i+(o<<22&4294967295|o>>>10),o=e+(s^n&(i^s))+r[4]+4118548399&4294967295,e=n+(o<<7&4294967295|o>>>25),o=s+(i^e&(n^i))+r[5]+1200080426&4294967295,s=e+(o<<12&4294967295|o>>>20),o=i+(n^s&(e^n))+r[6]+2821735955&4294967295,i=s+(o<<17&4294967295|o>>>15),o=n+(e^i&(s^e))+r[7]+4249261313&4294967295,n=i+(o<<22&4294967295|o>>>10),o=e+(s^n&(i^s))+r[8]+1770035416&4294967295,e=n+(o<<7&4294967295|o>>>25),o=s+(i^e&(n^i))+r[9]+2336552879&4294967295,s=e+(o<<12&4294967295|o>>>20),o=i+(n^s&(e^n))+r[10]+4294925233&4294967295,i=s+(o<<17&4294967295|o>>>15),o=n+(e^i&(s^e))+r[11]+2304563134&4294967295,n=i+(o<<22&4294967295|o>>>10),o=e+(s^n&(i^s))+r[12]+1804603682&4294967295,e=n+(o<<7&4294967295|o>>>25),o=s+(i^e&(n^i))+r[13]+4254626195&4294967295,s=e+(o<<12&4294967295|o>>>20),o=i+(n^s&(e^n))+r[14]+2792965006&4294967295,i=s+(o<<17&4294967295|o>>>15),o=n+(e^i&(s^e))+r[15]+1236535329&4294967295,n=i+(o<<22&4294967295|o>>>10),o=e+(i^s&(n^i))+r[1]+4129170786&4294967295,e=n+(o<<5&4294967295|o>>>27),o=s+(n^i&(e^n))+r[6]+3225465664&4294967295,s=e+(o<<9&4294967295|o>>>23),o=i+(e^n&(s^e))+r[11]+643717713&4294967295,i=s+(o<<14&4294967295|o>>>18),o=n+(s^e&(i^s))+r[0]+3921069994&4294967295,n=i+(o<<20&4294967295|o>>>12),o=e+(i^s&(n^i))+r[5]+3593408605&4294967295,e=n+(o<<5&4294967295|o>>>27),o=s+(n^i&(e^n))+r[10]+38016083&4294967295,s=e+(o<<9&4294967295|o>>>23),o=i+(e^n&(s^e))+r[15]+3634488961&4294967295,i=s+(o<<14&4294967295|o>>>18),o=n+(s^e&(i^s))+r[4]+3889429448&4294967295,n=i+(o<<20&4294967295|o>>>12),o=e+(i^s&(n^i))+r[9]+568446438&4294967295,e=n+(o<<5&4294967295|o>>>27),o=s+(n^i&(e^n))+r[14]+3275163606&4294967295,s=e+(o<<9&4294967295|o>>>23),o=i+(e^n&(s^e))+r[3]+4107603335&4294967295,i=s+(o<<14&4294967295|o>>>18),o=n+(s^e&(i^s))+r[8]+1163531501&4294967295,n=i+(o<<20&4294967295|o>>>12),o=e+(i^s&(n^i))+r[13]+2850285829&4294967295,e=n+(o<<5&4294967295|o>>>27),o=s+(n^i&(e^n))+r[2]+4243563512&4294967295,s=e+(o<<9&4294967295|o>>>23),o=i+(e^n&(s^e))+r[7]+1735328473&4294967295,i=s+(o<<14&4294967295|o>>>18),o=n+(s^e&(i^s))+r[12]+2368359562&4294967295,n=i+(o<<20&4294967295|o>>>12),o=e+(n^i^s)+r[5]+4294588738&4294967295,e=n+(o<<4&4294967295|o>>>28),o=s+(e^n^i)+r[8]+2272392833&4294967295,s=e+(o<<11&4294967295|o>>>21),o=i+(s^e^n)+r[11]+1839030562&4294967295,i=s+(o<<16&4294967295|o>>>16),o=n+(i^s^e)+r[14]+4259657740&4294967295,n=i+(o<<23&4294967295|o>>>9),o=e+(n^i^s)+r[1]+2763975236&4294967295,e=n+(o<<4&4294967295|o>>>28),o=s+(e^n^i)+r[4]+1272893353&4294967295,s=e+(o<<11&4294967295|o>>>21),o=i+(s^e^n)+r[7]+4139469664&4294967295,i=s+(o<<16&4294967295|o>>>16),o=n+(i^s^e)+r[10]+3200236656&4294967295,n=i+(o<<23&4294967295|o>>>9),o=e+(n^i^s)+r[13]+681279174&4294967295,e=n+(o<<4&4294967295|o>>>28),o=s+(e^n^i)+r[0]+3936430074&4294967295,s=e+(o<<11&4294967295|o>>>21),o=i+(s^e^n)+r[3]+3572445317&4294967295,i=s+(o<<16&4294967295|o>>>16),o=n+(i^s^e)+r[6]+76029189&4294967295,n=i+(o<<23&4294967295|o>>>9),o=e+(n^i^s)+r[9]+3654602809&4294967295,e=n+(o<<4&4294967295|o>>>28),o=s+(e^n^i)+r[12]+3873151461&4294967295,s=e+(o<<11&4294967295|o>>>21),o=i+(s^e^n)+r[15]+530742520&4294967295,i=s+(o<<16&4294967295|o>>>16),o=n+(i^s^e)+r[2]+3299628645&4294967295,n=i+(o<<23&4294967295|o>>>9),o=e+(i^(n|~s))+r[0]+4096336452&4294967295,e=n+(o<<6&4294967295|o>>>26),o=s+(n^(e|~i))+r[7]+1126891415&4294967295,s=e+(o<<10&4294967295|o>>>22),o=i+(e^(s|~n))+r[14]+2878612391&4294967295,i=s+(o<<15&4294967295|o>>>17),o=n+(s^(i|~e))+r[5]+4237533241&4294967295,n=i+(o<<21&4294967295|o>>>11),o=e+(i^(n|~s))+r[12]+1700485571&4294967295,e=n+(o<<6&4294967295|o>>>26),o=s+(n^(e|~i))+r[3]+2399980690&4294967295,s=e+(o<<10&4294967295|o>>>22),o=i+(e^(s|~n))+r[10]+4293915773&4294967295,i=s+(o<<15&4294967295|o>>>17),o=n+(s^(i|~e))+r[1]+2240044497&4294967295,n=i+(o<<21&4294967295|o>>>11),o=e+(i^(n|~s))+r[8]+1873313359&4294967295,e=n+(o<<6&4294967295|o>>>26),o=s+(n^(e|~i))+r[15]+4264355552&4294967295,s=e+(o<<10&4294967295|o>>>22),o=i+(e^(s|~n))+r[6]+2734768916&4294967295,i=s+(o<<15&4294967295|o>>>17),o=n+(s^(i|~e))+r[13]+1309151649&4294967295,n=i+(o<<21&4294967295|o>>>11),o=e+(i^(n|~s))+r[4]+4149444226&4294967295,e=n+(o<<6&4294967295|o>>>26),o=s+(n^(e|~i))+r[11]+3174756917&4294967295,s=e+(o<<10&4294967295|o>>>22),o=i+(e^(s|~n))+r[2]+718787259&4294967295,i=s+(o<<15&4294967295|o>>>17),o=n+(s^(i|~e))+r[9]+3951481745&4294967295,t.g[0]=t.g[0]+e&4294967295,t.g[1]=t.g[1]+(i+(o<<21&4294967295|o>>>11))&4294967295,t.g[2]=t.g[2]+i&4294967295,t.g[3]=t.g[3]+s&4294967295}An.prototype.j=function(t,e){e===void 0&&(e=t.length);for(var n=e-this.blockSize,r=this.m,i=this.h,s=0;s<e;){if(i==0)for(;s<=n;)Qf(this,t,s),s+=this.blockSize;if(typeof t=="string"){for(;s<e;)if(r[i++]=t.charCodeAt(s++),i==this.blockSize){Qf(this,r),i=0;break}}else for(;s<e;)if(r[i++]=t[s++],i==this.blockSize){Qf(this,r),i=0;break}}this.h=i,this.i+=e};An.prototype.l=function(){var t=Array((56>this.h?this.blockSize:2*this.blockSize)-this.h);t[0]=128;for(var e=1;e<t.length-8;++e)t[e]=0;var n=8*this.i;for(e=t.length-8;e<t.length;++e)t[e]=n&255,n/=256;for(this.j(t),t=Array(16),e=n=0;4>e;++e)for(var r=0;32>r;r+=8)t[n++]=this.g[e]>>>r&255;return t};function ae(t,e){this.h=e;for(var n=[],r=!0,i=t.length-1;0<=i;i--){var s=t[i]|0;r&&s==e||(n[i]=s,r=!1)}this.g=n}var YC={};function Im(t){return-128<=t&&128>t?iC(t,function(e){return new ae([e|0],0>e?-1:0)}):new ae([t|0],0>t?-1:0)}function _n(t){if(isNaN(t)||!isFinite(t))return hs;if(0>t)return it(_n(-t));for(var e=[],n=1,r=0;t>=n;r++)e[r]=t/n|0,n*=Ed;return new ae(e,0)}function UE(t,e){if(t.length==0)throw Error("number format error: empty string");if(e=e||10,2>e||36<e)throw Error("radix out of range: "+e);if(t.charAt(0)=="-")return it(UE(t.substring(1),e));if(0<=t.indexOf("-"))throw Error('number format error: interior "-" character');for(var n=_n(Math.pow(e,8)),r=hs,i=0;i<t.length;i+=8){var s=Math.min(8,t.length-i),o=parseInt(t.substring(i,i+s),e);8>s?(s=_n(Math.pow(e,s)),r=r.R(s).add(_n(o))):(r=r.R(n),r=r.add(_n(o)))}return r}var Ed=4294967296,hs=Im(0),Td=Im(1),_y=Im(16777216);b=ae.prototype;b.ea=function(){if(Wt(this))return-it(this).ea();for(var t=0,e=1,n=0;n<this.g.length;n++){var r=this.D(n);t+=(0<=r?r:Ed+r)*e,e*=Ed}return t};b.toString=function(t){if(t=t||10,2>t||36<t)throw Error("radix out of range: "+t);if(jn(this))return"0";if(Wt(this))return"-"+it(this).toString(t);for(var e=_n(Math.pow(t,6)),n=this,r="";;){var i=zu(n,e).g;n=$u(n,i.R(e));var s=((0<n.g.length?n.g[0]:n.h)>>>0).toString(t);if(n=i,jn(n))return s+r;for(;6>s.length;)s="0"+s;r=s+r}};b.D=function(t){return 0>t?0:t<this.g.length?this.g[t]:this.h};function jn(t){if(t.h!=0)return!1;for(var e=0;e<t.g.length;e++)if(t.g[e]!=0)return!1;return!0}function Wt(t){return t.h==-1}b.X=function(t){return t=$u(this,t),Wt(t)?-1:jn(t)?0:1};function it(t){for(var e=t.g.length,n=[],r=0;r<e;r++)n[r]=~t.g[r];return new ae(n,~t.h).add(Td)}b.abs=function(){return Wt(this)?it(this):this};b.add=function(t){for(var e=Math.max(this.g.length,t.g.length),n=[],r=0,i=0;i<=e;i++){var s=r+(this.D(i)&65535)+(t.D(i)&65535),o=(s>>>16)+(this.D(i)>>>16)+(t.D(i)>>>16);r=o>>>16,s&=65535,o&=65535,n[i]=o<<16|s}return new ae(n,n[n.length-1]&-2147483648?-1:0)};function $u(t,e){return t.add(it(e))}b.R=function(t){if(jn(this)||jn(t))return hs;if(Wt(this))return Wt(t)?it(this).R(it(t)):it(it(this).R(t));if(Wt(t))return it(this.R(it(t)));if(0>this.X(_y)&&0>t.X(_y))return _n(this.ea()*t.ea());for(var e=this.g.length+t.g.length,n=[],r=0;r<2*e;r++)n[r]=0;for(r=0;r<this.g.length;r++)for(var i=0;i<t.g.length;i++){var s=this.D(r)>>>16,o=this.D(r)&65535,a=t.D(i)>>>16,l=t.D(i)&65535;n[2*r+2*i]+=o*l,Tl(n,2*r+2*i),n[2*r+2*i+1]+=s*l,Tl(n,2*r+2*i+1),n[2*r+2*i+1]+=o*a,Tl(n,2*r+2*i+1),n[2*r+2*i+2]+=s*a,Tl(n,2*r+2*i+2)}for(r=0;r<e;r++)n[r]=n[2*r+1]<<16|n[2*r];for(r=e;r<2*e;r++)n[r]=0;return new ae(n,0)};function Tl(t,e){for(;(t[e]&65535)!=t[e];)t[e+1]+=t[e]>>>16,t[e]&=65535,e++}function oo(t,e){this.g=t,this.h=e}function zu(t,e){if(jn(e))throw Error("division by zero");if(jn(t))return new oo(hs,hs);if(Wt(t))return e=zu(it(t),e),new oo(it(e.g),it(e.h));if(Wt(e))return e=zu(t,it(e)),new oo(it(e.g),e.h);if(30<t.g.length){if(Wt(t)||Wt(e))throw Error("slowDivide_ only works with positive integers.");for(var n=Td,r=e;0>=r.X(t);)n=wy(n),r=wy(r);var i=Mi(n,1),s=Mi(r,1);for(r=Mi(r,2),n=Mi(n,2);!jn(r);){var o=s.add(r);0>=o.X(t)&&(i=i.add(n),s=o),r=Mi(r,1),n=Mi(n,1)}return e=$u(t,i.R(e)),new oo(i,e)}for(i=hs;0<=t.X(e);){for(n=Math.max(1,Math.floor(t.ea()/e.ea())),r=Math.ceil(Math.log(n)/Math.LN2),r=48>=r?1:Math.pow(2,r-48),s=_n(n),o=s.R(e);Wt(o)||0<o.X(t);)n-=r,s=_n(n),o=s.R(e);jn(s)&&(s=Td),i=i.add(s),t=$u(t,o)}return new oo(i,t)}b.gb=function(t){return zu(this,t).h};b.and=function(t){for(var e=Math.max(this.g.length,t.g.length),n=[],r=0;r<e;r++)n[r]=this.D(r)&t.D(r);return new ae(n,this.h&t.h)};b.or=function(t){for(var e=Math.max(this.g.length,t.g.length),n=[],r=0;r<e;r++)n[r]=this.D(r)|t.D(r);return new ae(n,this.h|t.h)};b.xor=function(t){for(var e=Math.max(this.g.length,t.g.length),n=[],r=0;r<e;r++)n[r]=this.D(r)^t.D(r);return new ae(n,this.h^t.h)};function wy(t){for(var e=t.g.length+1,n=[],r=0;r<e;r++)n[r]=t.D(r)<<1|t.D(r-1)>>>31;return new ae(n,t.h)}function Mi(t,e){var n=e>>5;e%=32;for(var r=t.g.length-n,i=[],s=0;s<r;s++)i[s]=0<e?t.D(s+n)>>>e|t.D(s+n+1)<<32-e:t.D(s+n);return new ae(i,t.h)}Uu.prototype.createWebChannel=Uu.prototype.g;Vt.prototype.send=Vt.prototype.u;Vt.prototype.open=Vt.prototype.m;Vt.prototype.close=Vt.prototype.close;bc.NO_ERROR=0;bc.TIMEOUT=8;bc.HTTP_ERROR=6;iE.COMPLETE="complete";sE.EventType=Va;Va.OPEN="a";Va.CLOSE="b";Va.ERROR="c";Va.MESSAGE="d";Je.prototype.listen=Je.prototype.O;Ce.prototype.listenOnce=Ce.prototype.P;Ce.prototype.getLastError=Ce.prototype.Sa;Ce.prototype.getLastErrorCode=Ce.prototype.Ia;Ce.prototype.getStatus=Ce.prototype.da;Ce.prototype.getResponseJson=Ce.prototype.Wa;Ce.prototype.getResponseText=Ce.prototype.ja;Ce.prototype.send=Ce.prototype.ha;Ce.prototype.setWithCredentials=Ce.prototype.Oa;An.prototype.digest=An.prototype.l;An.prototype.reset=An.prototype.reset;An.prototype.update=An.prototype.j;ae.prototype.add=ae.prototype.add;ae.prototype.multiply=ae.prototype.R;ae.prototype.modulo=ae.prototype.gb;ae.prototype.compare=ae.prototype.X;ae.prototype.toNumber=ae.prototype.ea;ae.prototype.toString=ae.prototype.toString;ae.prototype.getBits=ae.prototype.D;ae.fromNumber=_n;ae.fromString=UE;var XC=function(){return new Uu},JC=function(){return Nc()},Yf=bc,ZC=iE,eN=Ci,Ey={xb:0,Ab:1,Bb:2,Ub:3,Zb:4,Wb:5,Xb:6,Vb:7,Tb:8,Yb:9,PROXY:10,NOPROXY:11,Rb:12,Nb:13,Ob:14,Mb:15,Pb:16,Qb:17,tb:18,sb:19,ub:20},tN=$a,Il=sE,nN=Ce,rN=ae;const Ty="@firebase/firestore";/**
 * @license
 * Copyright 2017 Google LLC
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */class ht{constructor(e){this.uid=e}isAuthenticated(){return this.uid!=null}toKey(){return this.isAuthenticated()?"uid:"+this.uid:"anonymous-user"}isEqual(e){return e.uid===this.uid}}ht.UNAUTHENTICATED=new ht(null),ht.GOOGLE_CREDENTIALS=new ht("google-credentials-uid"),ht.FIRST_PARTY=new ht("first-party-uid"),ht.MOCK_USER=new ht("mock-user");/**
 * @license
 * Copyright 2017 Google LLC
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */let $s="10.1.0";/**
 * @license
 * Copyright 2017 Google LLC
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */const Ti=new Wp("@firebase/firestore");function Iy(){return Ti.logLevel}function $(t,...e){if(Ti.logLevel<=se.DEBUG){const n=e.map(Sm);Ti.debug(`Firestore (${$s}): ${t}`,...n)}}function Ii(t,...e){if(Ti.logLevel<=se.ERROR){const n=e.map(Sm);Ti.error(`Firestore (${$s}): ${t}`,...n)}}function Bu(t,...e){if(Ti.logLevel<=se.WARN){const n=e.map(Sm);Ti.warn(`Firestore (${$s}): ${t}`,...n)}}function Sm(t){if(typeof t=="string")return t;try{/**
* @license
* Copyright 2020 Google LLC
*
* Licensed under the Apache License, Version 2.0 (the "License");
* you may not use this file except in compliance with the License.
* You may obtain a copy of the License at
*
*   http://www.apache.org/licenses/LICENSE-2.0
*
* Unless required by applicable law or agreed to in writing, software
* distributed under the License is distributed on an "AS IS" BASIS,
* WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
* See the License for the specific language governing permissions and
* limitations under the License.
*/return function(n){return JSON.stringify(n)}(t)}catch{return t}}/**
 * @license
 * Copyright 2017 Google LLC
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */function ne(t="Unexpected state"){const e=`FIRESTORE (${$s}) INTERNAL ASSERTION FAILED: `+t;throw Ii(e),new Error(e)}function Ze(t,e){t||ne()}function we(t,e){return t}/**
 * @license
 * Copyright 2017 Google LLC
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */const R={OK:"ok",CANCELLED:"cancelled",UNKNOWN:"unknown",INVALID_ARGUMENT:"invalid-argument",DEADLINE_EXCEEDED:"deadline-exceeded",NOT_FOUND:"not-found",ALREADY_EXISTS:"already-exists",PERMISSION_DENIED:"permission-denied",UNAUTHENTICATED:"unauthenticated",RESOURCE_EXHAUSTED:"resource-exhausted",FAILED_PRECONDITION:"failed-precondition",ABORTED:"aborted",OUT_OF_RANGE:"out-of-range",UNIMPLEMENTED:"unimplemented",INTERNAL:"internal",UNAVAILABLE:"unavailable",DATA_LOSS:"data-loss"};class G extends tr{constructor(e,n){super(e,n),this.code=e,this.message=n,this.toString=()=>`${this.name}: [code=${this.code}]: ${this.message}`}}/**
 * @license
 * Copyright 2017 Google LLC
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */class fi{constructor(){this.promise=new Promise((e,n)=>{this.resolve=e,this.reject=n})}}/**
 * @license
 * Copyright 2017 Google LLC
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */class $E{constructor(e,n){this.user=n,this.type="OAuth",this.headers=new Map,this.headers.set("Authorization",`Bearer ${e}`)}}class iN{getToken(){return Promise.resolve(null)}invalidateToken(){}start(e,n){e.enqueueRetryable(()=>n(ht.UNAUTHENTICATED))}shutdown(){}}class sN{constructor(e){this.token=e,this.changeListener=null}getToken(){return Promise.resolve(this.token)}invalidateToken(){}start(e,n){this.changeListener=n,e.enqueueRetryable(()=>n(this.token.user))}shutdown(){this.changeListener=null}}class oN{constructor(e){this.t=e,this.currentUser=ht.UNAUTHENTICATED,this.i=0,this.forceRefresh=!1,this.auth=null}start(e,n){let r=this.i;const i=l=>this.i!==r?(r=this.i,n(l)):Promise.resolve();let s=new fi;this.o=()=>{this.i++,this.currentUser=this.u(),s.resolve(),s=new fi,e.enqueueRetryable(()=>i(this.currentUser))};const o=()=>{const l=s;e.enqueueRetryable(async()=>{await l.promise,await i(this.currentUser)})},a=l=>{$("FirebaseAuthCredentialsProvider","Auth detected"),this.auth=l,this.auth.addAuthTokenListener(this.o),o()};this.t.onInit(l=>a(l)),setTimeout(()=>{if(!this.auth){const l=this.t.getImmediate({optional:!0});l?a(l):($("FirebaseAuthCredentialsProvider","Auth not yet detected"),s.resolve(),s=new fi)}},0),o()}getToken(){const e=this.i,n=this.forceRefresh;return this.forceRefresh=!1,this.auth?this.auth.getToken(n).then(r=>this.i!==e?($("FirebaseAuthCredentialsProvider","getToken aborted due to token change."),this.getToken()):r?(Ze(typeof r.accessToken=="string"),new $E(r.accessToken,this.currentUser)):null):Promise.resolve(null)}invalidateToken(){this.forceRefresh=!0}shutdown(){this.auth&&this.auth.removeAuthTokenListener(this.o)}u(){const e=this.auth&&this.auth.getUid();return Ze(e===null||typeof e=="string"),new ht(e)}}class aN{constructor(e,n,r){this.l=e,this.h=n,this.P=r,this.type="FirstParty",this.user=ht.FIRST_PARTY,this.I=new Map}T(){return this.P?this.P():null}get headers(){this.I.set("X-Goog-AuthUser",this.l);const e=this.T();return e&&this.I.set("Authorization",e),this.h&&this.I.set("X-Goog-Iam-Authorization-Token",this.h),this.I}}class lN{constructor(e,n,r){this.l=e,this.h=n,this.P=r}getToken(){return Promise.resolve(new aN(this.l,this.h,this.P))}start(e,n){e.enqueueRetryable(()=>n(ht.FIRST_PARTY))}shutdown(){}invalidateToken(){}}class uN{constructor(e){this.value=e,this.type="AppCheck",this.headers=new Map,e&&e.length>0&&this.headers.set("x-firebase-appcheck",this.value)}}class cN{constructor(e){this.A=e,this.forceRefresh=!1,this.appCheck=null,this.R=null}start(e,n){const r=s=>{s.error!=null&&$("FirebaseAppCheckTokenProvider",`Error getting App Check token; using placeholder token instead. Error: ${s.error.message}`);const o=s.token!==this.R;return this.R=s.token,$("FirebaseAppCheckTokenProvider",`Received ${o?"new":"existing"} token.`),o?n(s.token):Promise.resolve()};this.o=s=>{e.enqueueRetryable(()=>r(s))};const i=s=>{$("FirebaseAppCheckTokenProvider","AppCheck detected"),this.appCheck=s,this.appCheck.addTokenListener(this.o)};this.A.onInit(s=>i(s)),setTimeout(()=>{if(!this.appCheck){const s=this.A.getImmediate({optional:!0});s?i(s):$("FirebaseAppCheckTokenProvider","AppCheck not yet detected")}},0)}getToken(){const e=this.forceRefresh;return this.forceRefresh=!1,this.appCheck?this.appCheck.getToken(e).then(n=>n?(Ze(typeof n.token=="string"),this.R=n.token,new uN(n.token)):null):Promise.resolve(null)}invalidateToken(){this.forceRefresh=!0}shutdown(){this.appCheck&&this.appCheck.removeTokenListener(this.o)}}/**
 * @license
 * Copyright 2020 Google LLC
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */function fN(t){const e=typeof self<"u"&&(self.crypto||self.msCrypto),n=new Uint8Array(t);if(e&&typeof e.getRandomValues=="function")e.getRandomValues(n);else for(let r=0;r<t;r++)n[r]=Math.floor(256*Math.random());return n}/**
 * @license
 * Copyright 2017 Google LLC
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */class zE{static V(){const e="ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz0123456789",n=Math.floor(256/e.length)*e.length;let r="";for(;r.length<20;){const i=fN(40);for(let s=0;s<i.length;++s)r.length<20&&i[s]<n&&(r+=e.charAt(i[s]%e.length))}return r}}function de(t,e){return t<e?-1:t>e?1:0}function xs(t,e,n){return t.length===e.length&&t.every((r,i)=>n(r,e[i]))}/**
 * @license
 * Copyright 2017 Google LLC
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */class ze{constructor(e,n){if(this.seconds=e,this.nanoseconds=n,n<0)throw new G(R.INVALID_ARGUMENT,"Timestamp nanoseconds out of range: "+n);if(n>=1e9)throw new G(R.INVALID_ARGUMENT,"Timestamp nanoseconds out of range: "+n);if(e<-62135596800)throw new G(R.INVALID_ARGUMENT,"Timestamp seconds out of range: "+e);if(e>=253402300800)throw new G(R.INVALID_ARGUMENT,"Timestamp seconds out of range: "+e)}static now(){return ze.fromMillis(Date.now())}static fromDate(e){return ze.fromMillis(e.getTime())}static fromMillis(e){const n=Math.floor(e/1e3),r=Math.floor(1e6*(e-1e3*n));return new ze(n,r)}toDate(){return new Date(this.toMillis())}toMillis(){return 1e3*this.seconds+this.nanoseconds/1e6}_compareTo(e){return this.seconds===e.seconds?de(this.nanoseconds,e.nanoseconds):de(this.seconds,e.seconds)}isEqual(e){return e.seconds===this.seconds&&e.nanoseconds===this.nanoseconds}toString(){return"Timestamp(seconds="+this.seconds+", nanoseconds="+this.nanoseconds+")"}toJSON(){return{seconds:this.seconds,nanoseconds:this.nanoseconds}}valueOf(){const e=this.seconds- -62135596800;return String(e).padStart(12,"0")+"."+String(this.nanoseconds).padStart(9,"0")}}/**
 * @license
 * Copyright 2017 Google LLC
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */class Se{constructor(e){this.timestamp=e}static fromTimestamp(e){return new Se(e)}static min(){return new Se(new ze(0,0))}static max(){return new Se(new ze(253402300799,999999999))}compareTo(e){return this.timestamp._compareTo(e.timestamp)}isEqual(e){return this.timestamp.isEqual(e.timestamp)}toMicroseconds(){return 1e6*this.timestamp.seconds+this.timestamp.nanoseconds/1e3}toString(){return"SnapshotVersion("+this.timestamp.toString()+")"}toTimestamp(){return this.timestamp}}/**
 * @license
 * Copyright 2017 Google LLC
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */class ua{constructor(e,n,r){n===void 0?n=0:n>e.length&&ne(),r===void 0?r=e.length-n:r>e.length-n&&ne(),this.segments=e,this.offset=n,this.len=r}get length(){return this.len}isEqual(e){return ua.comparator(this,e)===0}child(e){const n=this.segments.slice(this.offset,this.limit());return e instanceof ua?e.forEach(r=>{n.push(r)}):n.push(e),this.construct(n)}limit(){return this.offset+this.length}popFirst(e){return e=e===void 0?1:e,this.construct(this.segments,this.offset+e,this.length-e)}popLast(){return this.construct(this.segments,this.offset,this.length-1)}firstSegment(){return this.segments[this.offset]}lastSegment(){return this.get(this.length-1)}get(e){return this.segments[this.offset+e]}isEmpty(){return this.length===0}isPrefixOf(e){if(e.length<this.length)return!1;for(let n=0;n<this.length;n++)if(this.get(n)!==e.get(n))return!1;return!0}isImmediateParentOf(e){if(this.length+1!==e.length)return!1;for(let n=0;n<this.length;n++)if(this.get(n)!==e.get(n))return!1;return!0}forEach(e){for(let n=this.offset,r=this.limit();n<r;n++)e(this.segments[n])}toArray(){return this.segments.slice(this.offset,this.limit())}static comparator(e,n){const r=Math.min(e.length,n.length);for(let i=0;i<r;i++){const s=e.get(i),o=n.get(i);if(s<o)return-1;if(s>o)return 1}return e.length<n.length?-1:e.length>n.length?1:0}}class Le extends ua{construct(e,n,r){return new Le(e,n,r)}canonicalString(){return this.toArray().join("/")}toString(){return this.canonicalString()}static fromString(...e){const n=[];for(const r of e){if(r.indexOf("//")>=0)throw new G(R.INVALID_ARGUMENT,`Invalid segment (${r}). Paths must not contain // in them.`);n.push(...r.split("/").filter(i=>i.length>0))}return new Le(n)}static emptyPath(){return new Le([])}}const hN=/^[_a-zA-Z][_a-zA-Z0-9]*$/;class mt extends ua{construct(e,n,r){return new mt(e,n,r)}static isValidIdentifier(e){return hN.test(e)}canonicalString(){return this.toArray().map(e=>(e=e.replace(/\\/g,"\\\\").replace(/`/g,"\\`"),mt.isValidIdentifier(e)||(e="`"+e+"`"),e)).join(".")}toString(){return this.canonicalString()}isKeyField(){return this.length===1&&this.get(0)==="__name__"}static keyField(){return new mt(["__name__"])}static fromServerFormat(e){const n=[];let r="",i=0;const s=()=>{if(r.length===0)throw new G(R.INVALID_ARGUMENT,`Invalid field path (${e}). Paths must not be empty, begin with '.', end with '.', or contain '..'`);n.push(r),r=""};let o=!1;for(;i<e.length;){const a=e[i];if(a==="\\"){if(i+1===e.length)throw new G(R.INVALID_ARGUMENT,"Path has trailing escape character: "+e);const l=e[i+1];if(l!=="\\"&&l!=="."&&l!=="`")throw new G(R.INVALID_ARGUMENT,"Path has invalid escape sequence: "+e);r+=l,i+=2}else a==="`"?(o=!o,i++):a!=="."||o?(r+=a,i++):(s(),i++)}if(s(),o)throw new G(R.INVALID_ARGUMENT,"Unterminated ` in path: "+e);return new mt(n)}static emptyPath(){return new mt([])}}/**
 * @license
 * Copyright 2017 Google LLC
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */class Z{constructor(e){this.path=e}static fromPath(e){return new Z(Le.fromString(e))}static fromName(e){return new Z(Le.fromString(e).popFirst(5))}static empty(){return new Z(Le.emptyPath())}get collectionGroup(){return this.path.popLast().lastSegment()}hasCollectionId(e){return this.path.length>=2&&this.path.get(this.path.length-2)===e}getCollectionGroup(){return this.path.get(this.path.length-2)}getCollectionPath(){return this.path.popLast()}isEqual(e){return e!==null&&Le.comparator(this.path,e.path)===0}toString(){return this.path.toString()}static comparator(e,n){return Le.comparator(e.path,n.path)}static isDocumentKey(e){return e.length%2==0}static fromSegments(e){return new Z(new Le(e.slice()))}}function dN(t,e){const n=t.toTimestamp().seconds,r=t.toTimestamp().nanoseconds+1,i=Se.fromTimestamp(r===1e9?new ze(n+1,0):new ze(n,r));return new Mr(i,Z.empty(),e)}function pN(t){return new Mr(t.readTime,t.key,-1)}class Mr{constructor(e,n,r){this.readTime=e,this.documentKey=n,this.largestBatchId=r}static min(){return new Mr(Se.min(),Z.empty(),-1)}static max(){return new Mr(Se.max(),Z.empty(),-1)}}function mN(t,e){let n=t.readTime.compareTo(e.readTime);return n!==0?n:(n=Z.comparator(t.documentKey,e.documentKey),n!==0?n:de(t.largestBatchId,e.largestBatchId))}/**
 * @license
 * Copyright 2020 Google LLC
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */const gN="The current tab is not in the required state to perform this operation. It might be necessary to refresh the browser tab.";class vN{constructor(){this.onCommittedListeners=[]}addOnCommittedListener(e){this.onCommittedListeners.push(e)}raiseOnCommittedEvent(){this.onCommittedListeners.forEach(e=>e())}}/**
 * @license
 * Copyright 2017 Google LLC
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */async function BE(t){if(t.code!==R.FAILED_PRECONDITION||t.message!==gN)throw t;$("LocalStore","Unexpectedly lost primary lease")}/**
 * @license
 * Copyright 2017 Google LLC
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */class C{constructor(e){this.nextCallback=null,this.catchCallback=null,this.result=void 0,this.error=void 0,this.isDone=!1,this.callbackAttached=!1,e(n=>{this.isDone=!0,this.result=n,this.nextCallback&&this.nextCallback(n)},n=>{this.isDone=!0,this.error=n,this.catchCallback&&this.catchCallback(n)})}catch(e){return this.next(void 0,e)}next(e,n){return this.callbackAttached&&ne(),this.callbackAttached=!0,this.isDone?this.error?this.wrapFailure(n,this.error):this.wrapSuccess(e,this.result):new C((r,i)=>{this.nextCallback=s=>{this.wrapSuccess(e,s).next(r,i)},this.catchCallback=s=>{this.wrapFailure(n,s).next(r,i)}})}toPromise(){return new Promise((e,n)=>{this.next(e,n)})}wrapUserFunction(e){try{const n=e();return n instanceof C?n:C.resolve(n)}catch(n){return C.reject(n)}}wrapSuccess(e,n){return e?this.wrapUserFunction(()=>e(n)):C.resolve(n)}wrapFailure(e,n){return e?this.wrapUserFunction(()=>e(n)):C.reject(n)}static resolve(e){return new C((n,r)=>{n(e)})}static reject(e){return new C((n,r)=>{r(e)})}static waitFor(e){return new C((n,r)=>{let i=0,s=0,o=!1;e.forEach(a=>{++i,a.next(()=>{++s,o&&s===i&&n()},l=>r(l))}),o=!0,s===i&&n()})}static or(e){let n=C.resolve(!1);for(const r of e)n=n.next(i=>i?C.resolve(i):r());return n}static forEach(e,n){const r=[];return e.forEach((i,s)=>{r.push(n.call(this,i,s))}),this.waitFor(r)}static mapArray(e,n){return new C((r,i)=>{const s=e.length,o=new Array(s);let a=0;for(let l=0;l<s;l++){const u=l;n(e[u]).next(c=>{o[u]=c,++a,a===s&&r(o)},c=>i(c))}})}static doWhile(e,n){return new C((r,i)=>{const s=()=>{e()===!0?n().next(()=>{s()},i):r()};s()})}}function Uc(t){return t.name==="IndexedDbTransactionError"}/**
 * @license
 * Copyright 2018 Google LLC
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */class HE{constructor(e,n){this.previousValue=e,n&&(n.sequenceNumberHandler=r=>this.oe(r),this._e=r=>n.writeSequenceNumber(r))}oe(e){return this.previousValue=Math.max(e,this.previousValue),this.previousValue}next(){const e=++this.previousValue;return this._e&&this._e(e),e}}HE.ae=-1;function km(t){return t==null}function Hu(t){return t===0&&1/t==-1/0}function yN(t){return typeof t=="number"&&Number.isInteger(t)&&!Hu(t)&&t<=Number.MAX_SAFE_INTEGER&&t>=Number.MIN_SAFE_INTEGER}/**
 * @license
 * Copyright 2017 Google LLC
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */function Sy(t){let e=0;for(const n in t)Object.prototype.hasOwnProperty.call(t,n)&&e++;return e}function Ha(t,e){for(const n in t)Object.prototype.hasOwnProperty.call(t,n)&&e(n,t[n])}function WE(t){for(const e in t)if(Object.prototype.hasOwnProperty.call(t,e))return!1;return!0}/**
 * @license
 * Copyright 2017 Google LLC
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */class bt{constructor(e,n){this.comparator=e,this.root=n||nt.EMPTY}insert(e,n){return new bt(this.comparator,this.root.insert(e,n,this.comparator).copy(null,null,nt.BLACK,null,null))}remove(e){return new bt(this.comparator,this.root.remove(e,this.comparator).copy(null,null,nt.BLACK,null,null))}get(e){let n=this.root;for(;!n.isEmpty();){const r=this.comparator(e,n.key);if(r===0)return n.value;r<0?n=n.left:r>0&&(n=n.right)}return null}indexOf(e){let n=0,r=this.root;for(;!r.isEmpty();){const i=this.comparator(e,r.key);if(i===0)return n+r.left.size;i<0?r=r.left:(n+=r.left.size+1,r=r.right)}return-1}isEmpty(){return this.root.isEmpty()}get size(){return this.root.size}minKey(){return this.root.minKey()}maxKey(){return this.root.maxKey()}inorderTraversal(e){return this.root.inorderTraversal(e)}forEach(e){this.inorderTraversal((n,r)=>(e(n,r),!1))}toString(){const e=[];return this.inorderTraversal((n,r)=>(e.push(`${n}:${r}`),!1)),`{${e.join(", ")}}`}reverseTraversal(e){return this.root.reverseTraversal(e)}getIterator(){return new Sl(this.root,null,this.comparator,!1)}getIteratorFrom(e){return new Sl(this.root,e,this.comparator,!1)}getReverseIterator(){return new Sl(this.root,null,this.comparator,!0)}getReverseIteratorFrom(e){return new Sl(this.root,e,this.comparator,!0)}}class Sl{constructor(e,n,r,i){this.isReverse=i,this.nodeStack=[];let s=1;for(;!e.isEmpty();)if(s=n?r(e.key,n):1,n&&i&&(s*=-1),s<0)e=this.isReverse?e.left:e.right;else{if(s===0){this.nodeStack.push(e);break}this.nodeStack.push(e),e=this.isReverse?e.right:e.left}}getNext(){let e=this.nodeStack.pop();const n={key:e.key,value:e.value};if(this.isReverse)for(e=e.left;!e.isEmpty();)this.nodeStack.push(e),e=e.right;else for(e=e.right;!e.isEmpty();)this.nodeStack.push(e),e=e.left;return n}hasNext(){return this.nodeStack.length>0}peek(){if(this.nodeStack.length===0)return null;const e=this.nodeStack[this.nodeStack.length-1];return{key:e.key,value:e.value}}}class nt{constructor(e,n,r,i,s){this.key=e,this.value=n,this.color=r??nt.RED,this.left=i??nt.EMPTY,this.right=s??nt.EMPTY,this.size=this.left.size+1+this.right.size}copy(e,n,r,i,s){return new nt(e??this.key,n??this.value,r??this.color,i??this.left,s??this.right)}isEmpty(){return!1}inorderTraversal(e){return this.left.inorderTraversal(e)||e(this.key,this.value)||this.right.inorderTraversal(e)}reverseTraversal(e){return this.right.reverseTraversal(e)||e(this.key,this.value)||this.left.reverseTraversal(e)}min(){return this.left.isEmpty()?this:this.left.min()}minKey(){return this.min().key}maxKey(){return this.right.isEmpty()?this.key:this.right.maxKey()}insert(e,n,r){let i=this;const s=r(e,i.key);return i=s<0?i.copy(null,null,null,i.left.insert(e,n,r),null):s===0?i.copy(null,n,null,null,null):i.copy(null,null,null,null,i.right.insert(e,n,r)),i.fixUp()}removeMin(){if(this.left.isEmpty())return nt.EMPTY;let e=this;return e.left.isRed()||e.left.left.isRed()||(e=e.moveRedLeft()),e=e.copy(null,null,null,e.left.removeMin(),null),e.fixUp()}remove(e,n){let r,i=this;if(n(e,i.key)<0)i.left.isEmpty()||i.left.isRed()||i.left.left.isRed()||(i=i.moveRedLeft()),i=i.copy(null,null,null,i.left.remove(e,n),null);else{if(i.left.isRed()&&(i=i.rotateRight()),i.right.isEmpty()||i.right.isRed()||i.right.left.isRed()||(i=i.moveRedRight()),n(e,i.key)===0){if(i.right.isEmpty())return nt.EMPTY;r=i.right.min(),i=i.copy(r.key,r.value,null,null,i.right.removeMin())}i=i.copy(null,null,null,null,i.right.remove(e,n))}return i.fixUp()}isRed(){return this.color}fixUp(){let e=this;return e.right.isRed()&&!e.left.isRed()&&(e=e.rotateLeft()),e.left.isRed()&&e.left.left.isRed()&&(e=e.rotateRight()),e.left.isRed()&&e.right.isRed()&&(e=e.colorFlip()),e}moveRedLeft(){let e=this.colorFlip();return e.right.left.isRed()&&(e=e.copy(null,null,null,null,e.right.rotateRight()),e=e.rotateLeft(),e=e.colorFlip()),e}moveRedRight(){let e=this.colorFlip();return e.left.left.isRed()&&(e=e.rotateRight(),e=e.colorFlip()),e}rotateLeft(){const e=this.copy(null,null,nt.RED,null,this.right.left);return this.right.copy(null,null,this.color,e,null)}rotateRight(){const e=this.copy(null,null,nt.RED,this.left.right,null);return this.left.copy(null,null,this.color,null,e)}colorFlip(){const e=this.left.copy(null,null,!this.left.color,null,null),n=this.right.copy(null,null,!this.right.color,null,null);return this.copy(null,null,!this.color,e,n)}checkMaxDepth(){const e=this.check();return Math.pow(2,e)<=this.size+1}check(){if(this.isRed()&&this.left.isRed()||this.right.isRed())throw ne();const e=this.left.check();if(e!==this.right.check())throw ne();return e+(this.isRed()?0:1)}}nt.EMPTY=null,nt.RED=!0,nt.BLACK=!1;nt.EMPTY=new class{constructor(){this.size=0}get key(){throw ne()}get value(){throw ne()}get color(){throw ne()}get left(){throw ne()}get right(){throw ne()}copy(e,n,r,i,s){return this}insert(e,n,r){return new nt(e,n)}remove(e,n){return this}isEmpty(){return!0}inorderTraversal(e){return!1}reverseTraversal(e){return!1}minKey(){return null}maxKey(){return null}isRed(){return!1}checkMaxDepth(){return!0}check(){return 0}};/**
 * @license
 * Copyright 2017 Google LLC
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */class It{constructor(e){this.comparator=e,this.data=new bt(this.comparator)}has(e){return this.data.get(e)!==null}first(){return this.data.minKey()}last(){return this.data.maxKey()}get size(){return this.data.size}indexOf(e){return this.data.indexOf(e)}forEach(e){this.data.inorderTraversal((n,r)=>(e(n),!1))}forEachInRange(e,n){const r=this.data.getIteratorFrom(e[0]);for(;r.hasNext();){const i=r.getNext();if(this.comparator(i.key,e[1])>=0)return;n(i.key)}}forEachWhile(e,n){let r;for(r=n!==void 0?this.data.getIteratorFrom(n):this.data.getIterator();r.hasNext();)if(!e(r.getNext().key))return}firstAfterOrEqual(e){const n=this.data.getIteratorFrom(e);return n.hasNext()?n.getNext().key:null}getIterator(){return new ky(this.data.getIterator())}getIteratorFrom(e){return new ky(this.data.getIteratorFrom(e))}add(e){return this.copy(this.data.remove(e).insert(e,!0))}delete(e){return this.has(e)?this.copy(this.data.remove(e)):this}isEmpty(){return this.data.isEmpty()}unionWith(e){let n=this;return n.size<e.size&&(n=e,e=this),e.forEach(r=>{n=n.add(r)}),n}isEqual(e){if(!(e instanceof It)||this.size!==e.size)return!1;const n=this.data.getIterator(),r=e.data.getIterator();for(;n.hasNext();){const i=n.getNext().key,s=r.getNext().key;if(this.comparator(i,s)!==0)return!1}return!0}toArray(){const e=[];return this.forEach(n=>{e.push(n)}),e}toString(){const e=[];return this.forEach(n=>e.push(n)),"SortedSet("+e.toString()+")"}copy(e){const n=new It(this.comparator);return n.data=e,n}}class ky{constructor(e){this.iter=e}getNext(){return this.iter.getNext().key}hasNext(){return this.iter.hasNext()}}/**
 * @license
 * Copyright 2020 Google LLC
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */class on{constructor(e){this.fields=e,e.sort(mt.comparator)}static empty(){return new on([])}unionWith(e){let n=new It(mt.comparator);for(const r of this.fields)n=n.add(r);for(const r of e)n=n.add(r);return new on(n.toArray())}covers(e){for(const n of this.fields)if(n.isPrefixOf(e))return!0;return!1}isEqual(e){return xs(this.fields,e.fields,(n,r)=>n.isEqual(r))}}/**
 * @license
 * Copyright 2023 Google LLC
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */class _N extends Error{constructor(){super(...arguments),this.name="Base64DecodeError"}}/**
 * @license
 * Copyright 2020 Google LLC
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */class Qn{constructor(e){this.binaryString=e}static fromBase64String(e){const n=function(i){try{return atob(i)}catch(s){throw typeof DOMException<"u"&&s instanceof DOMException?new _N("Invalid base64 string: "+s):s}}(e);return new Qn(n)}static fromUint8Array(e){const n=function(i){let s="";for(let o=0;o<i.length;++o)s+=String.fromCharCode(i[o]);return s}(e);return new Qn(n)}[Symbol.iterator](){let e=0;return{next:()=>e<this.binaryString.length?{value:this.binaryString.charCodeAt(e++),done:!1}:{value:void 0,done:!0}}}toBase64(){return function(n){return btoa(n)}(this.binaryString)}toUint8Array(){return function(n){const r=new Uint8Array(n.length);for(let i=0;i<n.length;i++)r[i]=n.charCodeAt(i);return r}(this.binaryString)}approximateByteSize(){return 2*this.binaryString.length}compareTo(e){return de(this.binaryString,e.binaryString)}isEqual(e){return this.binaryString===e.binaryString}}Qn.EMPTY_BYTE_STRING=new Qn("");const wN=new RegExp(/^\d{4}-\d\d-\d\dT\d\d:\d\d:\d\d(?:\.(\d+))?Z$/);function Si(t){if(Ze(!!t),typeof t=="string"){let e=0;const n=wN.exec(t);if(Ze(!!n),n[1]){let i=n[1];i=(i+"000000000").substr(0,9),e=Number(i)}const r=new Date(t);return{seconds:Math.floor(r.getTime()/1e3),nanos:e}}return{seconds:rt(t.seconds),nanos:rt(t.nanos)}}function rt(t){return typeof t=="number"?t:typeof t=="string"?Number(t):0}function ca(t){return typeof t=="string"?Qn.fromBase64String(t):Qn.fromUint8Array(t)}/**
 * @license
 * Copyright 2020 Google LLC
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */function Am(t){var e,n;return((n=(((e=t==null?void 0:t.mapValue)===null||e===void 0?void 0:e.fields)||{}).__type__)===null||n===void 0?void 0:n.stringValue)==="server_timestamp"}function KE(t){const e=t.mapValue.fields.__previous_value__;return Am(e)?KE(e):e}function Wu(t){const e=Si(t.mapValue.fields.__local_write_time__.timestampValue);return new ze(e.seconds,e.nanos)}/**
 * @license
 * Copyright 2017 Google LLC
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */class EN{constructor(e,n,r,i,s,o,a,l,u){this.databaseId=e,this.appId=n,this.persistenceKey=r,this.host=i,this.ssl=s,this.forceLongPolling=o,this.autoDetectLongPolling=a,this.longPollingOptions=l,this.useFetchStreams=u}}class Ku{constructor(e,n){this.projectId=e,this.database=n||"(default)"}static empty(){return new Ku("","")}get isDefaultDatabase(){return this.database==="(default)"}isEqual(e){return e instanceof Ku&&e.projectId===this.projectId&&e.database===this.database}}/**
 * @license
 * Copyright 2020 Google LLC
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */const kl={mapValue:{fields:{__type__:{stringValue:"__max__"}}}};function Ps(t){return"nullValue"in t?0:"booleanValue"in t?1:"integerValue"in t||"doubleValue"in t?2:"timestampValue"in t?3:"stringValue"in t?5:"bytesValue"in t?6:"referenceValue"in t?7:"geoPointValue"in t?8:"arrayValue"in t?9:"mapValue"in t?Am(t)?4:TN(t)?9007199254740991:10:ne()}function xn(t,e){if(t===e)return!0;const n=Ps(t);if(n!==Ps(e))return!1;switch(n){case 0:case 9007199254740991:return!0;case 1:return t.booleanValue===e.booleanValue;case 4:return Wu(t).isEqual(Wu(e));case 3:return function(i,s){if(typeof i.timestampValue=="string"&&typeof s.timestampValue=="string"&&i.timestampValue.length===s.timestampValue.length)return i.timestampValue===s.timestampValue;const o=Si(i.timestampValue),a=Si(s.timestampValue);return o.seconds===a.seconds&&o.nanos===a.nanos}(t,e);case 5:return t.stringValue===e.stringValue;case 6:return function(i,s){return ca(i.bytesValue).isEqual(ca(s.bytesValue))}(t,e);case 7:return t.referenceValue===e.referenceValue;case 8:return function(i,s){return rt(i.geoPointValue.latitude)===rt(s.geoPointValue.latitude)&&rt(i.geoPointValue.longitude)===rt(s.geoPointValue.longitude)}(t,e);case 2:return function(i,s){if("integerValue"in i&&"integerValue"in s)return rt(i.integerValue)===rt(s.integerValue);if("doubleValue"in i&&"doubleValue"in s){const o=rt(i.doubleValue),a=rt(s.doubleValue);return o===a?Hu(o)===Hu(a):isNaN(o)&&isNaN(a)}return!1}(t,e);case 9:return xs(t.arrayValue.values||[],e.arrayValue.values||[],xn);case 10:return function(i,s){const o=i.mapValue.fields||{},a=s.mapValue.fields||{};if(Sy(o)!==Sy(a))return!1;for(const l in o)if(o.hasOwnProperty(l)&&(a[l]===void 0||!xn(o[l],a[l])))return!1;return!0}(t,e);default:return ne()}}function fa(t,e){return(t.values||[]).find(n=>xn(n,e))!==void 0}function Rs(t,e){if(t===e)return 0;const n=Ps(t),r=Ps(e);if(n!==r)return de(n,r);switch(n){case 0:case 9007199254740991:return 0;case 1:return de(t.booleanValue,e.booleanValue);case 2:return function(s,o){const a=rt(s.integerValue||s.doubleValue),l=rt(o.integerValue||o.doubleValue);return a<l?-1:a>l?1:a===l?0:isNaN(a)?isNaN(l)?0:-1:1}(t,e);case 3:return Ay(t.timestampValue,e.timestampValue);case 4:return Ay(Wu(t),Wu(e));case 5:return de(t.stringValue,e.stringValue);case 6:return function(s,o){const a=ca(s),l=ca(o);return a.compareTo(l)}(t.bytesValue,e.bytesValue);case 7:return function(s,o){const a=s.split("/"),l=o.split("/");for(let u=0;u<a.length&&u<l.length;u++){const c=de(a[u],l[u]);if(c!==0)return c}return de(a.length,l.length)}(t.referenceValue,e.referenceValue);case 8:return function(s,o){const a=de(rt(s.latitude),rt(o.latitude));return a!==0?a:de(rt(s.longitude),rt(o.longitude))}(t.geoPointValue,e.geoPointValue);case 9:return function(s,o){const a=s.values||[],l=o.values||[];for(let u=0;u<a.length&&u<l.length;++u){const c=Rs(a[u],l[u]);if(c)return c}return de(a.length,l.length)}(t.arrayValue,e.arrayValue);case 10:return function(s,o){if(s===kl.mapValue&&o===kl.mapValue)return 0;if(s===kl.mapValue)return 1;if(o===kl.mapValue)return-1;const a=s.fields||{},l=Object.keys(a),u=o.fields||{},c=Object.keys(u);l.sort(),c.sort();for(let h=0;h<l.length&&h<c.length;++h){const d=de(l[h],c[h]);if(d!==0)return d;const y=Rs(a[l[h]],u[c[h]]);if(y!==0)return y}return de(l.length,c.length)}(t.mapValue,e.mapValue);default:throw ne()}}function Ay(t,e){if(typeof t=="string"&&typeof e=="string"&&t.length===e.length)return de(t,e);const n=Si(t),r=Si(e),i=de(n.seconds,r.seconds);return i!==0?i:de(n.nanos,r.nanos)}function Cs(t){return Id(t)}function Id(t){return"nullValue"in t?"null":"booleanValue"in t?""+t.booleanValue:"integerValue"in t?""+t.integerValue:"doubleValue"in t?""+t.doubleValue:"timestampValue"in t?function(n){const r=Si(n);return`time(${r.seconds},${r.nanos})`}(t.timestampValue):"stringValue"in t?t.stringValue:"bytesValue"in t?function(n){return ca(n).toBase64()}(t.bytesValue):"referenceValue"in t?function(n){return Z.fromName(n).toString()}(t.referenceValue):"geoPointValue"in t?function(n){return`geo(${n.latitude},${n.longitude})`}(t.geoPointValue):"arrayValue"in t?function(n){let r="[",i=!0;for(const s of n.values||[])i?i=!1:r+=",",r+=Id(s);return r+"]"}(t.arrayValue):"mapValue"in t?function(n){const r=Object.keys(n.fields||{}).sort();let i="{",s=!0;for(const o of r)s?s=!1:i+=",",i+=`${o}:${Id(n.fields[o])}`;return i+"}"}(t.mapValue):ne()}function Sd(t){return!!t&&"integerValue"in t}function xm(t){return!!t&&"arrayValue"in t}function Zl(t){return!!t&&"mapValue"in t}function xo(t){if(t.geoPointValue)return{geoPointValue:Object.assign({},t.geoPointValue)};if(t.timestampValue&&typeof t.timestampValue=="object")return{timestampValue:Object.assign({},t.timestampValue)};if(t.mapValue){const e={mapValue:{fields:{}}};return Ha(t.mapValue.fields,(n,r)=>e.mapValue.fields[n]=xo(r)),e}if(t.arrayValue){const e={arrayValue:{values:[]}};for(let n=0;n<(t.arrayValue.values||[]).length;++n)e.arrayValue.values[n]=xo(t.arrayValue.values[n]);return e}return Object.assign({},t)}function TN(t){return(((t.mapValue||{}).fields||{}).__type__||{}).stringValue==="__max__"}/**
 * @license
 * Copyright 2017 Google LLC
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */class sn{constructor(e){this.value=e}static empty(){return new sn({mapValue:{}})}field(e){if(e.isEmpty())return this.value;{let n=this.value;for(let r=0;r<e.length-1;++r)if(n=(n.mapValue.fields||{})[e.get(r)],!Zl(n))return null;return n=(n.mapValue.fields||{})[e.lastSegment()],n||null}}set(e,n){this.getFieldsMap(e.popLast())[e.lastSegment()]=xo(n)}setAll(e){let n=mt.emptyPath(),r={},i=[];e.forEach((o,a)=>{if(!n.isImmediateParentOf(a)){const l=this.getFieldsMap(n);this.applyChanges(l,r,i),r={},i=[],n=a.popLast()}o?r[a.lastSegment()]=xo(o):i.push(a.lastSegment())});const s=this.getFieldsMap(n);this.applyChanges(s,r,i)}delete(e){const n=this.field(e.popLast());Zl(n)&&n.mapValue.fields&&delete n.mapValue.fields[e.lastSegment()]}isEqual(e){return xn(this.value,e.value)}getFieldsMap(e){let n=this.value;n.mapValue.fields||(n.mapValue={fields:{}});for(let r=0;r<e.length;++r){let i=n.mapValue.fields[e.get(r)];Zl(i)&&i.mapValue.fields||(i={mapValue:{fields:{}}},n.mapValue.fields[e.get(r)]=i),n=i}return n.mapValue.fields}applyChanges(e,n,r){Ha(n,(i,s)=>e[i]=s);for(const i of r)delete e[i]}clone(){return new sn(xo(this.value))}}function GE(t){const e=[];return Ha(t.fields,(n,r)=>{const i=new mt([n]);if(Zl(r)){const s=GE(r.mapValue).fields;if(s.length===0)e.push(i);else for(const o of s)e.push(i.child(o))}else e.push(i)}),new on(e)}/**
 * @license
 * Copyright 2017 Google LLC
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */class rn{constructor(e,n,r,i,s,o,a){this.key=e,this.documentType=n,this.version=r,this.readTime=i,this.createTime=s,this.data=o,this.documentState=a}static newInvalidDocument(e){return new rn(e,0,Se.min(),Se.min(),Se.min(),sn.empty(),0)}static newFoundDocument(e,n,r,i){return new rn(e,1,n,Se.min(),r,i,0)}static newNoDocument(e,n){return new rn(e,2,n,Se.min(),Se.min(),sn.empty(),0)}static newUnknownDocument(e,n){return new rn(e,3,n,Se.min(),Se.min(),sn.empty(),2)}convertToFoundDocument(e,n){return!this.createTime.isEqual(Se.min())||this.documentType!==2&&this.documentType!==0||(this.createTime=e),this.version=e,this.documentType=1,this.data=n,this.documentState=0,this}convertToNoDocument(e){return this.version=e,this.documentType=2,this.data=sn.empty(),this.documentState=0,this}convertToUnknownDocument(e){return this.version=e,this.documentType=3,this.data=sn.empty(),this.documentState=2,this}setHasCommittedMutations(){return this.documentState=2,this}setHasLocalMutations(){return this.documentState=1,this.version=Se.min(),this}setReadTime(e){return this.readTime=e,this}get hasLocalMutations(){return this.documentState===1}get hasCommittedMutations(){return this.documentState===2}get hasPendingWrites(){return this.hasLocalMutations||this.hasCommittedMutations}isValidDocument(){return this.documentType!==0}isFoundDocument(){return this.documentType===1}isNoDocument(){return this.documentType===2}isUnknownDocument(){return this.documentType===3}isEqual(e){return e instanceof rn&&this.key.isEqual(e.key)&&this.version.isEqual(e.version)&&this.documentType===e.documentType&&this.documentState===e.documentState&&this.data.isEqual(e.data)}mutableCopy(){return new rn(this.key,this.documentType,this.version,this.readTime,this.createTime,this.data.clone(),this.documentState)}toString(){return`Document(${this.key}, ${this.version}, ${JSON.stringify(this.data.value)}, {createTime: ${this.createTime}}), {documentType: ${this.documentType}}), {documentState: ${this.documentState}})`}}/**
 * @license
 * Copyright 2022 Google LLC
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */class Gu{constructor(e,n){this.position=e,this.inclusive=n}}function xy(t,e,n){let r=0;for(let i=0;i<t.position.length;i++){const s=e[i],o=t.position[i];if(s.field.isKeyField()?r=Z.comparator(Z.fromName(o.referenceValue),n.key):r=Rs(o,n.data.field(s.field)),s.dir==="desc"&&(r*=-1),r!==0)break}return r}function Py(t,e){if(t===null)return e===null;if(e===null||t.inclusive!==e.inclusive||t.position.length!==e.position.length)return!1;for(let n=0;n<t.position.length;n++)if(!xn(t.position[n],e.position[n]))return!1;return!0}/**
 * @license
 * Copyright 2022 Google LLC
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */class Po{constructor(e,n="asc"){this.field=e,this.dir=n}}function IN(t,e){return t.dir===e.dir&&t.field.isEqual(e.field)}/**
 * @license
 * Copyright 2022 Google LLC
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */class qE{}class Ye extends qE{constructor(e,n,r){super(),this.field=e,this.op=n,this.value=r}static create(e,n,r){return e.isKeyField()?n==="in"||n==="not-in"?this.createKeyFieldInFilter(e,n,r):new kN(e,n,r):n==="array-contains"?new PN(e,r):n==="in"?new RN(e,r):n==="not-in"?new CN(e,r):n==="array-contains-any"?new NN(e,r):new Ye(e,n,r)}static createKeyFieldInFilter(e,n,r){return n==="in"?new AN(e,r):new xN(e,r)}matches(e){const n=e.data.field(this.field);return this.op==="!="?n!==null&&this.matchesComparison(Rs(n,this.value)):n!==null&&Ps(this.value)===Ps(n)&&this.matchesComparison(Rs(n,this.value))}matchesComparison(e){switch(this.op){case"<":return e<0;case"<=":return e<=0;case"==":return e===0;case"!=":return e!==0;case">":return e>0;case">=":return e>=0;default:return ne()}}isInequality(){return["<","<=",">",">=","!=","not-in"].indexOf(this.op)>=0}getFlattenedFilters(){return[this]}getFilters(){return[this]}getFirstInequalityField(){return this.isInequality()?this.field:null}}class Lr extends qE{constructor(e,n){super(),this.filters=e,this.op=n,this.ce=null}static create(e,n){return new Lr(e,n)}matches(e){return QE(this)?this.filters.find(n=>!n.matches(e))===void 0:this.filters.find(n=>n.matches(e))!==void 0}getFlattenedFilters(){return this.ce!==null||(this.ce=this.filters.reduce((e,n)=>e.concat(n.getFlattenedFilters()),[])),this.ce}getFilters(){return Object.assign([],this.filters)}getFirstInequalityField(){const e=this.le(n=>n.isInequality());return e!==null?e.field:null}le(e){for(const n of this.getFlattenedFilters())if(e(n))return n;return null}}function QE(t){return t.op==="and"}function YE(t){return SN(t)&&QE(t)}function SN(t){for(const e of t.filters)if(e instanceof Lr)return!1;return!0}function kd(t){if(t instanceof Ye)return t.field.canonicalString()+t.op.toString()+Cs(t.value);if(YE(t))return t.filters.map(e=>kd(e)).join(",");{const e=t.filters.map(n=>kd(n)).join(",");return`${t.op}(${e})`}}function XE(t,e){return t instanceof Ye?function(r,i){return i instanceof Ye&&r.op===i.op&&r.field.isEqual(i.field)&&xn(r.value,i.value)}(t,e):t instanceof Lr?function(r,i){return i instanceof Lr&&r.op===i.op&&r.filters.length===i.filters.length?r.filters.reduce((s,o,a)=>s&&XE(o,i.filters[a]),!0):!1}(t,e):void ne()}function JE(t){return t instanceof Ye?function(n){return`${n.field.canonicalString()} ${n.op} ${Cs(n.value)}`}(t):t instanceof Lr?function(n){return n.op.toString()+" {"+n.getFilters().map(JE).join(" ,")+"}"}(t):"Filter"}class kN extends Ye{constructor(e,n,r){super(e,n,r),this.key=Z.fromName(r.referenceValue)}matches(e){const n=Z.comparator(e.key,this.key);return this.matchesComparison(n)}}class AN extends Ye{constructor(e,n){super(e,"in",n),this.keys=ZE("in",n)}matches(e){return this.keys.some(n=>n.isEqual(e.key))}}class xN extends Ye{constructor(e,n){super(e,"not-in",n),this.keys=ZE("not-in",n)}matches(e){return!this.keys.some(n=>n.isEqual(e.key))}}function ZE(t,e){var n;return(((n=e.arrayValue)===null||n===void 0?void 0:n.values)||[]).map(r=>Z.fromName(r.referenceValue))}class PN extends Ye{constructor(e,n){super(e,"array-contains",n)}matches(e){const n=e.data.field(this.field);return xm(n)&&fa(n.arrayValue,this.value)}}class RN extends Ye{constructor(e,n){super(e,"in",n)}matches(e){const n=e.data.field(this.field);return n!==null&&fa(this.value.arrayValue,n)}}class CN extends Ye{constructor(e,n){super(e,"not-in",n)}matches(e){if(fa(this.value.arrayValue,{nullValue:"NULL_VALUE"}))return!1;const n=e.data.field(this.field);return n!==null&&!fa(this.value.arrayValue,n)}}class NN extends Ye{constructor(e,n){super(e,"array-contains-any",n)}matches(e){const n=e.data.field(this.field);return!(!xm(n)||!n.arrayValue.values)&&n.arrayValue.values.some(r=>fa(this.value.arrayValue,r))}}/**
 * @license
 * Copyright 2019 Google LLC
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */class bN{constructor(e,n=null,r=[],i=[],s=null,o=null,a=null){this.path=e,this.collectionGroup=n,this.orderBy=r,this.filters=i,this.limit=s,this.startAt=o,this.endAt=a,this.he=null}}function Ry(t,e=null,n=[],r=[],i=null,s=null,o=null){return new bN(t,e,n,r,i,s,o)}function Pm(t){const e=we(t);if(e.he===null){let n=e.path.canonicalString();e.collectionGroup!==null&&(n+="|cg:"+e.collectionGroup),n+="|f:",n+=e.filters.map(r=>kd(r)).join(","),n+="|ob:",n+=e.orderBy.map(r=>function(s){return s.field.canonicalString()+s.dir}(r)).join(","),km(e.limit)||(n+="|l:",n+=e.limit),e.startAt&&(n+="|lb:",n+=e.startAt.inclusive?"b:":"a:",n+=e.startAt.position.map(r=>Cs(r)).join(",")),e.endAt&&(n+="|ub:",n+=e.endAt.inclusive?"a:":"b:",n+=e.endAt.position.map(r=>Cs(r)).join(",")),e.he=n}return e.he}function Rm(t,e){if(t.limit!==e.limit||t.orderBy.length!==e.orderBy.length)return!1;for(let n=0;n<t.orderBy.length;n++)if(!IN(t.orderBy[n],e.orderBy[n]))return!1;if(t.filters.length!==e.filters.length)return!1;for(let n=0;n<t.filters.length;n++)if(!XE(t.filters[n],e.filters[n]))return!1;return t.collectionGroup===e.collectionGroup&&!!t.path.isEqual(e.path)&&!!Py(t.startAt,e.startAt)&&Py(t.endAt,e.endAt)}/**
 * @license
 * Copyright 2017 Google LLC
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */class $c{constructor(e,n=null,r=[],i=[],s=null,o="F",a=null,l=null){this.path=e,this.collectionGroup=n,this.explicitOrderBy=r,this.filters=i,this.limit=s,this.limitType=o,this.startAt=a,this.endAt=l,this.Pe=null,this.Ie=null,this.startAt,this.endAt}}function ON(t,e,n,r,i,s,o,a){return new $c(t,e,n,r,i,s,o,a)}function DN(t){return new $c(t)}function Cy(t){return t.filters.length===0&&t.limit===null&&t.startAt==null&&t.endAt==null&&(t.explicitOrderBy.length===0||t.explicitOrderBy.length===1&&t.explicitOrderBy[0].field.isKeyField())}function MN(t){return t.explicitOrderBy.length>0?t.explicitOrderBy[0].field:null}function LN(t){for(const e of t.filters){const n=e.getFirstInequalityField();if(n!==null)return n}return null}function jN(t){return t.collectionGroup!==null}function ds(t){const e=we(t);if(e.Pe===null){e.Pe=[];const n=LN(e),r=MN(e);if(n!==null&&r===null)n.isKeyField()||e.Pe.push(new Po(n)),e.Pe.push(new Po(mt.keyField(),"asc"));else{let i=!1;for(const s of e.explicitOrderBy)e.Pe.push(s),s.field.isKeyField()&&(i=!0);if(!i){const s=e.explicitOrderBy.length>0?e.explicitOrderBy[e.explicitOrderBy.length-1].dir:"asc";e.Pe.push(new Po(mt.keyField(),s))}}}return e.Pe}function Ns(t){const e=we(t);if(!e.Ie)if(e.limitType==="F")e.Ie=Ry(e.path,e.collectionGroup,ds(e),e.filters,e.limit,e.startAt,e.endAt);else{const n=[];for(const s of ds(e)){const o=s.dir==="desc"?"asc":"desc";n.push(new Po(s.field,o))}const r=e.endAt?new Gu(e.endAt.position,e.endAt.inclusive):null,i=e.startAt?new Gu(e.startAt.position,e.startAt.inclusive):null;e.Ie=Ry(e.path,e.collectionGroup,n,e.filters,e.limit,r,i)}return e.Ie}function Ad(t,e,n){return new $c(t.path,t.collectionGroup,t.explicitOrderBy.slice(),t.filters.slice(),e,n,t.startAt,t.endAt)}function eT(t,e){return Rm(Ns(t),Ns(e))&&t.limitType===e.limitType}function tT(t){return`${Pm(Ns(t))}|lt:${t.limitType}`}function Ny(t){return`Query(target=${function(n){let r=n.path.canonicalString();return n.collectionGroup!==null&&(r+=" collectionGroup="+n.collectionGroup),n.filters.length>0&&(r+=`, filters: [${n.filters.map(i=>JE(i)).join(", ")}]`),km(n.limit)||(r+=", limit: "+n.limit),n.orderBy.length>0&&(r+=`, orderBy: [${n.orderBy.map(i=>function(o){return`${o.field.canonicalString()} (${o.dir})`}(i)).join(", ")}]`),n.startAt&&(r+=", startAt: ",r+=n.startAt.inclusive?"b:":"a:",r+=n.startAt.position.map(i=>Cs(i)).join(",")),n.endAt&&(r+=", endAt: ",r+=n.endAt.inclusive?"a:":"b:",r+=n.endAt.position.map(i=>Cs(i)).join(",")),`Target(${r})`}(Ns(t))}; limitType=${t.limitType})`}function Cm(t,e){return e.isFoundDocument()&&function(r,i){const s=i.key.path;return r.collectionGroup!==null?i.key.hasCollectionId(r.collectionGroup)&&r.path.isPrefixOf(s):Z.isDocumentKey(r.path)?r.path.isEqual(s):r.path.isImmediateParentOf(s)}(t,e)&&function(r,i){for(const s of ds(r))if(!s.field.isKeyField()&&i.data.field(s.field)===null)return!1;return!0}(t,e)&&function(r,i){for(const s of r.filters)if(!s.matches(i))return!1;return!0}(t,e)&&function(r,i){return!(r.startAt&&!function(o,a,l){const u=xy(o,a,l);return o.inclusive?u<=0:u<0}(r.startAt,ds(r),i)||r.endAt&&!function(o,a,l){const u=xy(o,a,l);return o.inclusive?u>=0:u>0}(r.endAt,ds(r),i))}(t,e)}function VN(t){return(e,n)=>{let r=!1;for(const i of ds(t)){const s=FN(i,e,n);if(s!==0)return s;r=r||i.field.isKeyField()}return 0}}function FN(t,e,n){const r=t.field.isKeyField()?Z.comparator(e.key,n.key):function(s,o,a){const l=o.data.field(s),u=a.data.field(s);return l!==null&&u!==null?Rs(l,u):ne()}(t.field,e,n);switch(t.dir){case"asc":return r;case"desc":return-1*r;default:return ne()}}/**
 * @license
 * Copyright 2017 Google LLC
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */class zs{constructor(e,n){this.mapKeyFn=e,this.equalsFn=n,this.inner={},this.innerSize=0}get(e){const n=this.mapKeyFn(e),r=this.inner[n];if(r!==void 0){for(const[i,s]of r)if(this.equalsFn(i,e))return s}}has(e){return this.get(e)!==void 0}set(e,n){const r=this.mapKeyFn(e),i=this.inner[r];if(i===void 0)return this.inner[r]=[[e,n]],void this.innerSize++;for(let s=0;s<i.length;s++)if(this.equalsFn(i[s][0],e))return void(i[s]=[e,n]);i.push([e,n]),this.innerSize++}delete(e){const n=this.mapKeyFn(e),r=this.inner[n];if(r===void 0)return!1;for(let i=0;i<r.length;i++)if(this.equalsFn(r[i][0],e))return r.length===1?delete this.inner[n]:r.splice(i,1),this.innerSize--,!0;return!1}forEach(e){Ha(this.inner,(n,r)=>{for(const[i,s]of r)e(i,s)})}isEmpty(){return WE(this.inner)}size(){return this.innerSize}}/**
 * @license
 * Copyright 2017 Google LLC
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */const UN=new bt(Z.comparator);function qu(){return UN}const nT=new bt(Z.comparator);function Al(...t){let e=nT;for(const n of t)e=e.insert(n.key,n);return e}function rT(t){let e=nT;return t.forEach((n,r)=>e=e.insert(n,r.overlayedDocument)),e}function ii(){return Ro()}function iT(){return Ro()}function Ro(){return new zs(t=>t.toString(),(t,e)=>t.isEqual(e))}const $N=new bt(Z.comparator),zN=new It(Z.comparator);function pt(...t){let e=zN;for(const n of t)e=e.add(n);return e}const BN=new It(de);function HN(){return BN}/**
 * @license
 * Copyright 2020 Google LLC
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */function sT(t,e){if(t.useProto3Json){if(isNaN(e))return{doubleValue:"NaN"};if(e===1/0)return{doubleValue:"Infinity"};if(e===-1/0)return{doubleValue:"-Infinity"}}return{doubleValue:Hu(e)?"-0":e}}function oT(t){return{integerValue:""+t}}function WN(t,e){return yN(e)?oT(e):sT(t,e)}/**
 * @license
 * Copyright 2018 Google LLC
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */class zc{constructor(){this._=void 0}}function KN(t,e,n){return t instanceof Qu?function(i,s){const o={fields:{__type__:{stringValue:"server_timestamp"},__local_write_time__:{timestampValue:{seconds:i.seconds,nanos:i.nanoseconds}}}};return s&&Am(s)&&(s=KE(s)),s&&(o.fields.__previous_value__=s),{mapValue:o}}(n,e):t instanceof ha?lT(t,e):t instanceof da?uT(t,e):function(i,s){const o=aT(i,s),a=by(o)+by(i.Te);return Sd(o)&&Sd(i.Te)?oT(a):sT(i.serializer,a)}(t,e)}function GN(t,e,n){return t instanceof ha?lT(t,e):t instanceof da?uT(t,e):n}function aT(t,e){return t instanceof Yu?function(r){return Sd(r)||function(s){return!!s&&"doubleValue"in s}(r)}(e)?e:{integerValue:0}:null}class Qu extends zc{}class ha extends zc{constructor(e){super(),this.elements=e}}function lT(t,e){const n=cT(e);for(const r of t.elements)n.some(i=>xn(i,r))||n.push(r);return{arrayValue:{values:n}}}class da extends zc{constructor(e){super(),this.elements=e}}function uT(t,e){let n=cT(e);for(const r of t.elements)n=n.filter(i=>!xn(i,r));return{arrayValue:{values:n}}}class Yu extends zc{constructor(e,n){super(),this.serializer=e,this.Te=n}}function by(t){return rt(t.integerValue||t.doubleValue)}function cT(t){return xm(t)&&t.arrayValue.values?t.arrayValue.values.slice():[]}function qN(t,e){return t.field.isEqual(e.field)&&function(r,i){return r instanceof ha&&i instanceof ha||r instanceof da&&i instanceof da?xs(r.elements,i.elements,xn):r instanceof Yu&&i instanceof Yu?xn(r.Te,i.Te):r instanceof Qu&&i instanceof Qu}(t.transform,e.transform)}class QN{constructor(e,n){this.version=e,this.transformResults=n}}class Un{constructor(e,n){this.updateTime=e,this.exists=n}static none(){return new Un}static exists(e){return new Un(void 0,e)}static updateTime(e){return new Un(e)}get isNone(){return this.updateTime===void 0&&this.exists===void 0}isEqual(e){return this.exists===e.exists&&(this.updateTime?!!e.updateTime&&this.updateTime.isEqual(e.updateTime):!e.updateTime)}}function eu(t,e){return t.updateTime!==void 0?e.isFoundDocument()&&e.version.isEqual(t.updateTime):t.exists===void 0||t.exists===e.isFoundDocument()}class Bc{}function fT(t,e){if(!t.hasLocalMutations||e&&e.fields.length===0)return null;if(e===null)return t.isNoDocument()?new dT(t.key,Un.none()):new Wa(t.key,t.data,Un.none());{const n=t.data,r=sn.empty();let i=new It(mt.comparator);for(let s of e.fields)if(!i.has(s)){let o=n.field(s);o===null&&s.length>1&&(s=s.popLast(),o=n.field(s)),o===null?r.delete(s):r.set(s,o),i=i.add(s)}return new Ni(t.key,r,new on(i.toArray()),Un.none())}}function YN(t,e,n){t instanceof Wa?function(i,s,o){const a=i.value.clone(),l=Dy(i.fieldTransforms,s,o.transformResults);a.setAll(l),s.convertToFoundDocument(o.version,a).setHasCommittedMutations()}(t,e,n):t instanceof Ni?function(i,s,o){if(!eu(i.precondition,s))return void s.convertToUnknownDocument(o.version);const a=Dy(i.fieldTransforms,s,o.transformResults),l=s.data;l.setAll(hT(i)),l.setAll(a),s.convertToFoundDocument(o.version,l).setHasCommittedMutations()}(t,e,n):function(i,s,o){s.convertToNoDocument(o.version).setHasCommittedMutations()}(0,e,n)}function Co(t,e,n,r){return t instanceof Wa?function(s,o,a,l){if(!eu(s.precondition,o))return a;const u=s.value.clone(),c=My(s.fieldTransforms,l,o);return u.setAll(c),o.convertToFoundDocument(o.version,u).setHasLocalMutations(),null}(t,e,n,r):t instanceof Ni?function(s,o,a,l){if(!eu(s.precondition,o))return a;const u=My(s.fieldTransforms,l,o),c=o.data;return c.setAll(hT(s)),c.setAll(u),o.convertToFoundDocument(o.version,c).setHasLocalMutations(),a===null?null:a.unionWith(s.fieldMask.fields).unionWith(s.fieldTransforms.map(h=>h.field))}(t,e,n,r):function(s,o,a){return eu(s.precondition,o)?(o.convertToNoDocument(o.version).setHasLocalMutations(),null):a}(t,e,n)}function XN(t,e){let n=null;for(const r of t.fieldTransforms){const i=e.data.field(r.field),s=aT(r.transform,i||null);s!=null&&(n===null&&(n=sn.empty()),n.set(r.field,s))}return n||null}function Oy(t,e){return t.type===e.type&&!!t.key.isEqual(e.key)&&!!t.precondition.isEqual(e.precondition)&&!!function(r,i){return r===void 0&&i===void 0||!(!r||!i)&&xs(r,i,(s,o)=>qN(s,o))}(t.fieldTransforms,e.fieldTransforms)&&(t.type===0?t.value.isEqual(e.value):t.type!==1||t.data.isEqual(e.data)&&t.fieldMask.isEqual(e.fieldMask))}class Wa extends Bc{constructor(e,n,r,i=[]){super(),this.key=e,this.value=n,this.precondition=r,this.fieldTransforms=i,this.type=0}getFieldMask(){return null}}class Ni extends Bc{constructor(e,n,r,i,s=[]){super(),this.key=e,this.data=n,this.fieldMask=r,this.precondition=i,this.fieldTransforms=s,this.type=1}getFieldMask(){return this.fieldMask}}function hT(t){const e=new Map;return t.fieldMask.fields.forEach(n=>{if(!n.isEmpty()){const r=t.data.field(n);e.set(n,r)}}),e}function Dy(t,e,n){const r=new Map;Ze(t.length===n.length);for(let i=0;i<n.length;i++){const s=t[i],o=s.transform,a=e.data.field(s.field);r.set(s.field,GN(o,a,n[i]))}return r}function My(t,e,n){const r=new Map;for(const i of t){const s=i.transform,o=n.data.field(i.field);r.set(i.field,KN(s,o,e))}return r}class dT extends Bc{constructor(e,n){super(),this.key=e,this.precondition=n,this.type=2,this.fieldTransforms=[]}getFieldMask(){return null}}class JN extends Bc{constructor(e,n){super(),this.key=e,this.precondition=n,this.type=3,this.fieldTransforms=[]}getFieldMask(){return null}}/**
 * @license
 * Copyright 2017 Google LLC
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */class ZN{constructor(e,n,r,i){this.batchId=e,this.localWriteTime=n,this.baseMutations=r,this.mutations=i}applyToRemoteDocument(e,n){const r=n.mutationResults;for(let i=0;i<this.mutations.length;i++){const s=this.mutations[i];s.key.isEqual(e.key)&&YN(s,e,r[i])}}applyToLocalView(e,n){for(const r of this.baseMutations)r.key.isEqual(e.key)&&(n=Co(r,e,n,this.localWriteTime));for(const r of this.mutations)r.key.isEqual(e.key)&&(n=Co(r,e,n,this.localWriteTime));return n}applyToLocalDocumentSet(e,n){const r=iT();return this.mutations.forEach(i=>{const s=e.get(i.key),o=s.overlayedDocument;let a=this.applyToLocalView(o,s.mutatedFields);a=n.has(i.key)?null:a;const l=fT(o,a);l!==null&&r.set(i.key,l),o.isValidDocument()||o.convertToNoDocument(Se.min())}),r}keys(){return this.mutations.reduce((e,n)=>e.add(n.key),pt())}isEqual(e){return this.batchId===e.batchId&&xs(this.mutations,e.mutations,(n,r)=>Oy(n,r))&&xs(this.baseMutations,e.baseMutations,(n,r)=>Oy(n,r))}}class Nm{constructor(e,n,r,i){this.batch=e,this.commitVersion=n,this.mutationResults=r,this.docVersions=i}static from(e,n,r){Ze(e.mutations.length===r.length);let i=function(){return $N}();const s=e.mutations;for(let o=0;o<s.length;o++)i=i.insert(s[o].key,r[o].version);return new Nm(e,n,r,i)}}/**
 * @license
 * Copyright 2022 Google LLC
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */class eb{constructor(e,n){this.largestBatchId=e,this.mutation=n}getKey(){return this.mutation.key}isEqual(e){return e!==null&&this.mutation===e.mutation}toString(){return`Overlay{
      largestBatchId: ${this.largestBatchId},
      mutation: ${this.mutation.toString()}
    }`}}/**
 * @license
 * Copyright 2017 Google LLC
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */var De,re;function tb(t){switch(t){default:return ne();case R.CANCELLED:case R.UNKNOWN:case R.DEADLINE_EXCEEDED:case R.RESOURCE_EXHAUSTED:case R.INTERNAL:case R.UNAVAILABLE:case R.UNAUTHENTICATED:return!1;case R.INVALID_ARGUMENT:case R.NOT_FOUND:case R.ALREADY_EXISTS:case R.PERMISSION_DENIED:case R.FAILED_PRECONDITION:case R.ABORTED:case R.OUT_OF_RANGE:case R.UNIMPLEMENTED:case R.DATA_LOSS:return!0}}function nb(t){if(t===void 0)return Ii("GRPC error has no .code"),R.UNKNOWN;switch(t){case De.OK:return R.OK;case De.CANCELLED:return R.CANCELLED;case De.UNKNOWN:return R.UNKNOWN;case De.DEADLINE_EXCEEDED:return R.DEADLINE_EXCEEDED;case De.RESOURCE_EXHAUSTED:return R.RESOURCE_EXHAUSTED;case De.INTERNAL:return R.INTERNAL;case De.UNAVAILABLE:return R.UNAVAILABLE;case De.UNAUTHENTICATED:return R.UNAUTHENTICATED;case De.INVALID_ARGUMENT:return R.INVALID_ARGUMENT;case De.NOT_FOUND:return R.NOT_FOUND;case De.ALREADY_EXISTS:return R.ALREADY_EXISTS;case De.PERMISSION_DENIED:return R.PERMISSION_DENIED;case De.FAILED_PRECONDITION:return R.FAILED_PRECONDITION;case De.ABORTED:return R.ABORTED;case De.OUT_OF_RANGE:return R.OUT_OF_RANGE;case De.UNIMPLEMENTED:return R.UNIMPLEMENTED;case De.DATA_LOSS:return R.DATA_LOSS;default:return ne()}}(re=De||(De={}))[re.OK=0]="OK",re[re.CANCELLED=1]="CANCELLED",re[re.UNKNOWN=2]="UNKNOWN",re[re.INVALID_ARGUMENT=3]="INVALID_ARGUMENT",re[re.DEADLINE_EXCEEDED=4]="DEADLINE_EXCEEDED",re[re.NOT_FOUND=5]="NOT_FOUND",re[re.ALREADY_EXISTS=6]="ALREADY_EXISTS",re[re.PERMISSION_DENIED=7]="PERMISSION_DENIED",re[re.UNAUTHENTICATED=16]="UNAUTHENTICATED",re[re.RESOURCE_EXHAUSTED=8]="RESOURCE_EXHAUSTED",re[re.FAILED_PRECONDITION=9]="FAILED_PRECONDITION",re[re.ABORTED=10]="ABORTED",re[re.OUT_OF_RANGE=11]="OUT_OF_RANGE",re[re.UNIMPLEMENTED=12]="UNIMPLEMENTED",re[re.INTERNAL=13]="INTERNAL",re[re.UNAVAILABLE=14]="UNAVAILABLE",re[re.DATA_LOSS=15]="DATA_LOSS";/**
 * @license
 * Copyright 2022 Google LLC
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */new rN([4294967295,4294967295],0);class rb{constructor(e,n){this.databaseId=e,this.useProto3Json=n}}function xd(t,e){return t.useProto3Json?`${new Date(1e3*e.seconds).toISOString().replace(/\.\d*/,"").replace("Z","")}.${("000000000"+e.nanoseconds).slice(-9)}Z`:{seconds:""+e.seconds,nanos:e.nanoseconds}}function ib(t,e){return t.useProto3Json?e.toBase64():e.toUint8Array()}function sb(t,e){return xd(t,e.toTimestamp())}function ps(t){return Ze(!!t),Se.fromTimestamp(function(n){const r=Si(n);return new ze(r.seconds,r.nanos)}(t))}function pT(t,e){return function(r){return new Le(["projects",r.projectId,"databases",r.database])}(t).child("documents").child(e).canonicalString()}function ob(t){const e=Le.fromString(t);return Ze(pb(e)),e}function Pd(t,e){return pT(t.databaseId,e.path)}function ab(t){const e=ob(t);return e.length===4?Le.emptyPath():ub(e)}function lb(t){return new Le(["projects",t.databaseId.projectId,"databases",t.databaseId.database]).canonicalString()}function ub(t){return Ze(t.length>4&&t.get(4)==="documents"),t.popFirst(5)}function Ly(t,e,n){return{name:Pd(t,e),fields:n.value.mapValue.fields}}function cb(t,e){let n;if(e instanceof Wa)n={update:Ly(t,e.key,e.value)};else if(e instanceof dT)n={delete:Pd(t,e.key)};else if(e instanceof Ni)n={update:Ly(t,e.key,e.data),updateMask:db(e.fieldMask)};else{if(!(e instanceof JN))return ne();n={verify:Pd(t,e.key)}}return e.fieldTransforms.length>0&&(n.updateTransforms=e.fieldTransforms.map(r=>function(s,o){const a=o.transform;if(a instanceof Qu)return{fieldPath:o.field.canonicalString(),setToServerValue:"REQUEST_TIME"};if(a instanceof ha)return{fieldPath:o.field.canonicalString(),appendMissingElements:{values:a.elements}};if(a instanceof da)return{fieldPath:o.field.canonicalString(),removeAllFromArray:{values:a.elements}};if(a instanceof Yu)return{fieldPath:o.field.canonicalString(),increment:a.Te};throw ne()}(0,r))),e.precondition.isNone||(n.currentDocument=function(i,s){return s.updateTime!==void 0?{updateTime:sb(i,s.updateTime)}:s.exists!==void 0?{exists:s.exists}:ne()}(t,e.precondition)),n}function fb(t,e){return t&&t.length>0?(Ze(e!==void 0),t.map(n=>function(i,s){let o=i.updateTime?ps(i.updateTime):ps(s);return o.isEqual(Se.min())&&(o=ps(s)),new QN(o,i.transformResults||[])}(n,e))):[]}function hb(t){let e=ab(t.parent);const n=t.structuredQuery,r=n.from?n.from.length:0;let i=null;if(r>0){Ze(r===1);const c=n.from[0];c.allDescendants?i=c.collectionId:e=e.child(c.collectionId)}let s=[];n.where&&(s=function(h){const d=mT(h);return d instanceof Lr&&YE(d)?d.getFilters():[d]}(n.where));let o=[];n.orderBy&&(o=function(h){return h.map(d=>function(_){return new Po(ji(_.field),function(T){switch(T){case"ASCENDING":return"asc";case"DESCENDING":return"desc";default:return}}(_.direction))}(d))}(n.orderBy));let a=null;n.limit&&(a=function(h){let d;return d=typeof h=="object"?h.value:h,km(d)?null:d}(n.limit));let l=null;n.startAt&&(l=function(h){const d=!!h.before,y=h.values||[];return new Gu(y,d)}(n.startAt));let u=null;return n.endAt&&(u=function(h){const d=!h.before,y=h.values||[];return new Gu(y,d)}(n.endAt)),ON(e,i,o,s,a,"F",l,u)}function mT(t){return t.unaryFilter!==void 0?function(n){switch(n.unaryFilter.op){case"IS_NAN":const r=ji(n.unaryFilter.field);return Ye.create(r,"==",{doubleValue:NaN});case"IS_NULL":const i=ji(n.unaryFilter.field);return Ye.create(i,"==",{nullValue:"NULL_VALUE"});case"IS_NOT_NAN":const s=ji(n.unaryFilter.field);return Ye.create(s,"!=",{doubleValue:NaN});case"IS_NOT_NULL":const o=ji(n.unaryFilter.field);return Ye.create(o,"!=",{nullValue:"NULL_VALUE"});default:return ne()}}(t):t.fieldFilter!==void 0?function(n){return Ye.create(ji(n.fieldFilter.field),function(i){switch(i){case"EQUAL":return"==";case"NOT_EQUAL":return"!=";case"GREATER_THAN":return">";case"GREATER_THAN_OR_EQUAL":return">=";case"LESS_THAN":return"<";case"LESS_THAN_OR_EQUAL":return"<=";case"ARRAY_CONTAINS":return"array-contains";case"IN":return"in";case"NOT_IN":return"not-in";case"ARRAY_CONTAINS_ANY":return"array-contains-any";default:return ne()}}(n.fieldFilter.op),n.fieldFilter.value)}(t):t.compositeFilter!==void 0?function(n){return Lr.create(n.compositeFilter.filters.map(r=>mT(r)),function(i){switch(i){case"AND":return"and";case"OR":return"or";default:return ne()}}(n.compositeFilter.op))}(t):ne()}function ji(t){return mt.fromServerFormat(t.fieldPath)}function db(t){const e=[];return t.fields.forEach(n=>e.push(n.canonicalString())),{fieldPaths:e}}function pb(t){return t.length>=4&&t.get(0)==="projects"&&t.get(2)==="databases"}/**
 * @license
 * Copyright 2017 Google LLC
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */class mb{constructor(e){this.ct=e}}function gb(t){const e=hb({parent:t.parent,structuredQuery:t.structuredQuery});return t.limitType==="LAST"?Ad(e,e.limit,"L"):e}/**
 * @license
 * Copyright 2019 Google LLC
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */class vb{constructor(){this.sn=new yb}addToCollectionParentIndex(e,n){return this.sn.add(n),C.resolve()}getCollectionParents(e,n){return C.resolve(this.sn.getEntries(n))}addFieldIndex(e,n){return C.resolve()}deleteFieldIndex(e,n){return C.resolve()}getDocumentsMatchingTarget(e,n){return C.resolve(null)}getIndexType(e,n){return C.resolve(0)}getFieldIndexes(e,n){return C.resolve([])}getNextCollectionGroupToUpdate(e){return C.resolve(null)}getMinOffset(e,n){return C.resolve(Mr.min())}getMinOffsetFromCollectionGroup(e,n){return C.resolve(Mr.min())}updateCollectionGroup(e,n,r){return C.resolve()}updateIndexEntries(e,n){return C.resolve()}}class yb{constructor(){this.index={}}add(e){const n=e.lastSegment(),r=e.popLast(),i=this.index[n]||new It(Le.comparator),s=!i.has(r);return this.index[n]=i.add(r),s}has(e){const n=e.lastSegment(),r=e.popLast(),i=this.index[n];return i&&i.has(r)}getEntries(e){return(this.index[e]||new It(Le.comparator)).toArray()}}/**
 * @license
 * Copyright 2017 Google LLC
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */class bs{constructor(e){this.Mn=e}next(){return this.Mn+=2,this.Mn}static xn(){return new bs(0)}static On(){return new bs(-1)}}/**
 * @license
 * Copyright 2017 Google LLC
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */class _b{constructor(){this.changes=new zs(e=>e.toString(),(e,n)=>e.isEqual(n)),this.changesApplied=!1}addEntry(e){this.assertNotApplied(),this.changes.set(e.key,e)}removeEntry(e,n){this.assertNotApplied(),this.changes.set(e,rn.newInvalidDocument(e).setReadTime(n))}getEntry(e,n){this.assertNotApplied();const r=this.changes.get(n);return r!==void 0?C.resolve(r):this.getFromCache(e,n)}getEntries(e,n){return this.getAllFromCache(e,n)}apply(e){return this.assertNotApplied(),this.changesApplied=!0,this.applyChanges(e)}assertNotApplied(){}}/**
 * @license
 * Copyright 2017 Google LLC
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 *//**
 * @license
 * Copyright 2022 Google LLC
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */class wb{constructor(e,n){this.overlayedDocument=e,this.mutatedFields=n}}/**
 * @license
 * Copyright 2017 Google LLC
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */class Eb{constructor(e,n,r,i){this.remoteDocumentCache=e,this.mutationQueue=n,this.documentOverlayCache=r,this.indexManager=i}getDocument(e,n){let r=null;return this.documentOverlayCache.getOverlay(e,n).next(i=>(r=i,this.remoteDocumentCache.getEntry(e,n))).next(i=>(r!==null&&Co(r.mutation,i,on.empty(),ze.now()),i))}getDocuments(e,n){return this.remoteDocumentCache.getEntries(e,n).next(r=>this.getLocalViewOfDocuments(e,r,pt()).next(()=>r))}getLocalViewOfDocuments(e,n,r=pt()){const i=ii();return this.populateOverlays(e,i,n).next(()=>this.computeViews(e,n,i,r).next(s=>{let o=Al();return s.forEach((a,l)=>{o=o.insert(a,l.overlayedDocument)}),o}))}getOverlayedDocuments(e,n){const r=ii();return this.populateOverlays(e,r,n).next(()=>this.computeViews(e,n,r,pt()))}populateOverlays(e,n,r){const i=[];return r.forEach(s=>{n.has(s)||i.push(s)}),this.documentOverlayCache.getOverlays(e,i).next(s=>{s.forEach((o,a)=>{n.set(o,a)})})}computeViews(e,n,r,i){let s=qu();const o=Ro(),a=function(){return Ro()}();return n.forEach((l,u)=>{const c=r.get(u.key);i.has(u.key)&&(c===void 0||c.mutation instanceof Ni)?s=s.insert(u.key,u):c!==void 0?(o.set(u.key,c.mutation.getFieldMask()),Co(c.mutation,u,c.mutation.getFieldMask(),ze.now())):o.set(u.key,on.empty())}),this.recalculateAndSaveOverlays(e,s).next(l=>(l.forEach((u,c)=>o.set(u,c)),n.forEach((u,c)=>{var h;return a.set(u,new wb(c,(h=o.get(u))!==null&&h!==void 0?h:null))}),a))}recalculateAndSaveOverlays(e,n){const r=Ro();let i=new bt((o,a)=>o-a),s=pt();return this.mutationQueue.getAllMutationBatchesAffectingDocumentKeys(e,n).next(o=>{for(const a of o)a.keys().forEach(l=>{const u=n.get(l);if(u===null)return;let c=r.get(l)||on.empty();c=a.applyToLocalView(u,c),r.set(l,c);const h=(i.get(a.batchId)||pt()).add(l);i=i.insert(a.batchId,h)})}).next(()=>{const o=[],a=i.getReverseIterator();for(;a.hasNext();){const l=a.getNext(),u=l.key,c=l.value,h=iT();c.forEach(d=>{if(!s.has(d)){const y=fT(n.get(d),r.get(d));y!==null&&h.set(d,y),s=s.add(d)}}),o.push(this.documentOverlayCache.saveOverlays(e,u,h))}return C.waitFor(o)}).next(()=>r)}recalculateAndSaveOverlaysForDocumentKeys(e,n){return this.remoteDocumentCache.getEntries(e,n).next(r=>this.recalculateAndSaveOverlays(e,r))}getDocumentsMatchingQuery(e,n,r){return function(s){return Z.isDocumentKey(s.path)&&s.collectionGroup===null&&s.filters.length===0}(n)?this.getDocumentsMatchingDocumentQuery(e,n.path):jN(n)?this.getDocumentsMatchingCollectionGroupQuery(e,n,r):this.getDocumentsMatchingCollectionQuery(e,n,r)}getNextDocuments(e,n,r,i){return this.remoteDocumentCache.getAllFromCollectionGroup(e,n,r,i).next(s=>{const o=i-s.size>0?this.documentOverlayCache.getOverlaysForCollectionGroup(e,n,r.largestBatchId,i-s.size):C.resolve(ii());let a=-1,l=s;return o.next(u=>C.forEach(u,(c,h)=>(a<h.largestBatchId&&(a=h.largestBatchId),s.get(c)?C.resolve():this.remoteDocumentCache.getEntry(e,c).next(d=>{l=l.insert(c,d)}))).next(()=>this.populateOverlays(e,u,s)).next(()=>this.computeViews(e,l,u,pt())).next(c=>({batchId:a,changes:rT(c)})))})}getDocumentsMatchingDocumentQuery(e,n){return this.getDocument(e,new Z(n)).next(r=>{let i=Al();return r.isFoundDocument()&&(i=i.insert(r.key,r)),i})}getDocumentsMatchingCollectionGroupQuery(e,n,r){const i=n.collectionGroup;let s=Al();return this.indexManager.getCollectionParents(e,i).next(o=>C.forEach(o,a=>{const l=function(c,h){return new $c(h,null,c.explicitOrderBy.slice(),c.filters.slice(),c.limit,c.limitType,c.startAt,c.endAt)}(n,a.child(i));return this.getDocumentsMatchingCollectionQuery(e,l,r).next(u=>{u.forEach((c,h)=>{s=s.insert(c,h)})})}).next(()=>s))}getDocumentsMatchingCollectionQuery(e,n,r){let i;return this.documentOverlayCache.getOverlaysForCollection(e,n.path,r.largestBatchId).next(s=>(i=s,this.remoteDocumentCache.getDocumentsMatchingQuery(e,n,r,i))).next(s=>{i.forEach((a,l)=>{const u=l.getKey();s.get(u)===null&&(s=s.insert(u,rn.newInvalidDocument(u)))});let o=Al();return s.forEach((a,l)=>{const u=i.get(a);u!==void 0&&Co(u.mutation,l,on.empty(),ze.now()),Cm(n,l)&&(o=o.insert(a,l))}),o})}}/**
 * @license
 * Copyright 2020 Google LLC
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */class Tb{constructor(e){this.serializer=e,this.ar=new Map,this.ur=new Map}getBundleMetadata(e,n){return C.resolve(this.ar.get(n))}saveBundleMetadata(e,n){return this.ar.set(n.id,function(i){return{id:i.id,version:i.version,createTime:ps(i.createTime)}}(n)),C.resolve()}getNamedQuery(e,n){return C.resolve(this.ur.get(n))}saveNamedQuery(e,n){return this.ur.set(n.name,function(i){return{name:i.name,query:gb(i.bundledQuery),readTime:ps(i.readTime)}}(n)),C.resolve()}}/**
 * @license
 * Copyright 2022 Google LLC
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */class Ib{constructor(){this.overlays=new bt(Z.comparator),this.cr=new Map}getOverlay(e,n){return C.resolve(this.overlays.get(n))}getOverlays(e,n){const r=ii();return C.forEach(n,i=>this.getOverlay(e,i).next(s=>{s!==null&&r.set(i,s)})).next(()=>r)}saveOverlays(e,n,r){return r.forEach((i,s)=>{this.ht(e,n,s)}),C.resolve()}removeOverlaysForBatchId(e,n,r){const i=this.cr.get(r);return i!==void 0&&(i.forEach(s=>this.overlays=this.overlays.remove(s)),this.cr.delete(r)),C.resolve()}getOverlaysForCollection(e,n,r){const i=ii(),s=n.length+1,o=new Z(n.child("")),a=this.overlays.getIteratorFrom(o);for(;a.hasNext();){const l=a.getNext().value,u=l.getKey();if(!n.isPrefixOf(u.path))break;u.path.length===s&&l.largestBatchId>r&&i.set(l.getKey(),l)}return C.resolve(i)}getOverlaysForCollectionGroup(e,n,r,i){let s=new bt((u,c)=>u-c);const o=this.overlays.getIterator();for(;o.hasNext();){const u=o.getNext().value;if(u.getKey().getCollectionGroup()===n&&u.largestBatchId>r){let c=s.get(u.largestBatchId);c===null&&(c=ii(),s=s.insert(u.largestBatchId,c)),c.set(u.getKey(),u)}}const a=ii(),l=s.getIterator();for(;l.hasNext()&&(l.getNext().value.forEach((u,c)=>a.set(u,c)),!(a.size()>=i)););return C.resolve(a)}ht(e,n,r){const i=this.overlays.get(r.key);if(i!==null){const o=this.cr.get(i.largestBatchId).delete(r.key);this.cr.set(i.largestBatchId,o)}this.overlays=this.overlays.insert(r.key,new eb(n,r));let s=this.cr.get(n);s===void 0&&(s=pt(),this.cr.set(n,s)),this.cr.set(n,s.add(r.key))}}/**
 * @license
 * Copyright 2017 Google LLC
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */class bm{constructor(){this.lr=new It(Ge.hr),this.Pr=new It(Ge.Ir)}isEmpty(){return this.lr.isEmpty()}addReference(e,n){const r=new Ge(e,n);this.lr=this.lr.add(r),this.Pr=this.Pr.add(r)}Tr(e,n){e.forEach(r=>this.addReference(r,n))}removeReference(e,n){this.Er(new Ge(e,n))}dr(e,n){e.forEach(r=>this.removeReference(r,n))}Ar(e){const n=new Z(new Le([])),r=new Ge(n,e),i=new Ge(n,e+1),s=[];return this.Pr.forEachInRange([r,i],o=>{this.Er(o),s.push(o.key)}),s}Rr(){this.lr.forEach(e=>this.Er(e))}Er(e){this.lr=this.lr.delete(e),this.Pr=this.Pr.delete(e)}Vr(e){const n=new Z(new Le([])),r=new Ge(n,e),i=new Ge(n,e+1);let s=pt();return this.Pr.forEachInRange([r,i],o=>{s=s.add(o.key)}),s}containsKey(e){const n=new Ge(e,0),r=this.lr.firstAfterOrEqual(n);return r!==null&&e.isEqual(r.key)}}class Ge{constructor(e,n){this.key=e,this.mr=n}static hr(e,n){return Z.comparator(e.key,n.key)||de(e.mr,n.mr)}static Ir(e,n){return de(e.mr,n.mr)||Z.comparator(e.key,n.key)}}/**
 * @license
 * Copyright 2017 Google LLC
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */class Sb{constructor(e,n){this.indexManager=e,this.referenceDelegate=n,this.mutationQueue=[],this.gr=1,this.pr=new It(Ge.hr)}checkEmpty(e){return C.resolve(this.mutationQueue.length===0)}addMutationBatch(e,n,r,i){const s=this.gr;this.gr++,this.mutationQueue.length>0&&this.mutationQueue[this.mutationQueue.length-1];const o=new ZN(s,n,r,i);this.mutationQueue.push(o);for(const a of i)this.pr=this.pr.add(new Ge(a.key,s)),this.indexManager.addToCollectionParentIndex(e,a.key.path.popLast());return C.resolve(o)}lookupMutationBatch(e,n){return C.resolve(this.yr(n))}getNextMutationBatchAfterBatchId(e,n){const r=n+1,i=this.wr(r),s=i<0?0:i;return C.resolve(this.mutationQueue.length>s?this.mutationQueue[s]:null)}getHighestUnacknowledgedBatchId(){return C.resolve(this.mutationQueue.length===0?-1:this.gr-1)}getAllMutationBatches(e){return C.resolve(this.mutationQueue.slice())}getAllMutationBatchesAffectingDocumentKey(e,n){const r=new Ge(n,0),i=new Ge(n,Number.POSITIVE_INFINITY),s=[];return this.pr.forEachInRange([r,i],o=>{const a=this.yr(o.mr);s.push(a)}),C.resolve(s)}getAllMutationBatchesAffectingDocumentKeys(e,n){let r=new It(de);return n.forEach(i=>{const s=new Ge(i,0),o=new Ge(i,Number.POSITIVE_INFINITY);this.pr.forEachInRange([s,o],a=>{r=r.add(a.mr)})}),C.resolve(this.Sr(r))}getAllMutationBatchesAffectingQuery(e,n){const r=n.path,i=r.length+1;let s=r;Z.isDocumentKey(s)||(s=s.child(""));const o=new Ge(new Z(s),0);let a=new It(de);return this.pr.forEachWhile(l=>{const u=l.key.path;return!!r.isPrefixOf(u)&&(u.length===i&&(a=a.add(l.mr)),!0)},o),C.resolve(this.Sr(a))}Sr(e){const n=[];return e.forEach(r=>{const i=this.yr(r);i!==null&&n.push(i)}),n}removeMutationBatch(e,n){Ze(this.br(n.batchId,"removed")===0),this.mutationQueue.shift();let r=this.pr;return C.forEach(n.mutations,i=>{const s=new Ge(i.key,n.batchId);return r=r.delete(s),this.referenceDelegate.markPotentiallyOrphaned(e,i.key)}).next(()=>{this.pr=r})}Cn(e){}containsKey(e,n){const r=new Ge(n,0),i=this.pr.firstAfterOrEqual(r);return C.resolve(n.isEqual(i&&i.key))}performConsistencyCheck(e){return this.mutationQueue.length,C.resolve()}br(e,n){return this.wr(e)}wr(e){return this.mutationQueue.length===0?0:e-this.mutationQueue[0].batchId}yr(e){const n=this.wr(e);return n<0||n>=this.mutationQueue.length?null:this.mutationQueue[n]}}/**
 * @license
 * Copyright 2017 Google LLC
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */class kb{constructor(e){this.Dr=e,this.docs=function(){return new bt(Z.comparator)}(),this.size=0}setIndexManager(e){this.indexManager=e}addEntry(e,n){const r=n.key,i=this.docs.get(r),s=i?i.size:0,o=this.Dr(n);return this.docs=this.docs.insert(r,{document:n.mutableCopy(),size:o}),this.size+=o-s,this.indexManager.addToCollectionParentIndex(e,r.path.popLast())}removeEntry(e){const n=this.docs.get(e);n&&(this.docs=this.docs.remove(e),this.size-=n.size)}getEntry(e,n){const r=this.docs.get(n);return C.resolve(r?r.document.mutableCopy():rn.newInvalidDocument(n))}getEntries(e,n){let r=qu();return n.forEach(i=>{const s=this.docs.get(i);r=r.insert(i,s?s.document.mutableCopy():rn.newInvalidDocument(i))}),C.resolve(r)}getDocumentsMatchingQuery(e,n,r,i){let s=qu();const o=n.path,a=new Z(o.child("")),l=this.docs.getIteratorFrom(a);for(;l.hasNext();){const{key:u,value:{document:c}}=l.getNext();if(!o.isPrefixOf(u.path))break;u.path.length>o.length+1||mN(pN(c),r)<=0||(i.has(c.key)||Cm(n,c))&&(s=s.insert(c.key,c.mutableCopy()))}return C.resolve(s)}getAllFromCollectionGroup(e,n,r,i){ne()}vr(e,n){return C.forEach(this.docs,r=>n(r))}newChangeBuffer(e){return new Ab(this)}getSize(e){return C.resolve(this.size)}}class Ab extends _b{constructor(e){super(),this.sr=e}applyChanges(e){const n=[];return this.changes.forEach((r,i)=>{i.isValidDocument()?n.push(this.sr.addEntry(e,i)):this.sr.removeEntry(r)}),C.waitFor(n)}getFromCache(e,n){return this.sr.getEntry(e,n)}getAllFromCache(e,n){return this.sr.getEntries(e,n)}}/**
 * @license
 * Copyright 2017 Google LLC
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */class xb{constructor(e){this.persistence=e,this.Cr=new zs(n=>Pm(n),Rm),this.lastRemoteSnapshotVersion=Se.min(),this.highestTargetId=0,this.Fr=0,this.Mr=new bm,this.targetCount=0,this.Or=bs.xn()}forEachTarget(e,n){return this.Cr.forEach((r,i)=>n(i)),C.resolve()}getLastRemoteSnapshotVersion(e){return C.resolve(this.lastRemoteSnapshotVersion)}getHighestSequenceNumber(e){return C.resolve(this.Fr)}allocateTargetId(e){return this.highestTargetId=this.Or.next(),C.resolve(this.highestTargetId)}setTargetsMetadata(e,n,r){return r&&(this.lastRemoteSnapshotVersion=r),n>this.Fr&&(this.Fr=n),C.resolve()}Ln(e){this.Cr.set(e.target,e);const n=e.targetId;n>this.highestTargetId&&(this.Or=new bs(n),this.highestTargetId=n),e.sequenceNumber>this.Fr&&(this.Fr=e.sequenceNumber)}addTargetData(e,n){return this.Ln(n),this.targetCount+=1,C.resolve()}updateTargetData(e,n){return this.Ln(n),C.resolve()}removeTargetData(e,n){return this.Cr.delete(n.target),this.Mr.Ar(n.targetId),this.targetCount-=1,C.resolve()}removeTargets(e,n,r){let i=0;const s=[];return this.Cr.forEach((o,a)=>{a.sequenceNumber<=n&&r.get(a.targetId)===null&&(this.Cr.delete(o),s.push(this.removeMatchingKeysForTargetId(e,a.targetId)),i++)}),C.waitFor(s).next(()=>i)}getTargetCount(e){return C.resolve(this.targetCount)}getTargetData(e,n){const r=this.Cr.get(n)||null;return C.resolve(r)}addMatchingKeys(e,n,r){return this.Mr.Tr(n,r),C.resolve()}removeMatchingKeys(e,n,r){this.Mr.dr(n,r);const i=this.persistence.referenceDelegate,s=[];return i&&n.forEach(o=>{s.push(i.markPotentiallyOrphaned(e,o))}),C.waitFor(s)}removeMatchingKeysForTargetId(e,n){return this.Mr.Ar(n),C.resolve()}getMatchingKeysForTargetId(e,n){const r=this.Mr.Vr(n);return C.resolve(r)}containsKey(e,n){return C.resolve(this.Mr.containsKey(n))}}/**
 * @license
 * Copyright 2017 Google LLC
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */class Pb{constructor(e,n){this.Nr={},this.overlays={},this.Br=new HE(0),this.Lr=!1,this.Lr=!0,this.referenceDelegate=e(this),this.kr=new xb(this),this.indexManager=new vb,this.remoteDocumentCache=function(i){return new kb(i)}(r=>this.referenceDelegate.qr(r)),this.serializer=new mb(n),this.Qr=new Tb(this.serializer)}start(){return Promise.resolve()}shutdown(){return this.Lr=!1,Promise.resolve()}get started(){return this.Lr}setDatabaseDeletedListener(){}setNetworkEnabled(){}getIndexManager(e){return this.indexManager}getDocumentOverlayCache(e){let n=this.overlays[e.toKey()];return n||(n=new Ib,this.overlays[e.toKey()]=n),n}getMutationQueue(e,n){let r=this.Nr[e.toKey()];return r||(r=new Sb(n,this.referenceDelegate),this.Nr[e.toKey()]=r),r}getTargetCache(){return this.kr}getRemoteDocumentCache(){return this.remoteDocumentCache}getBundleCache(){return this.Qr}runTransaction(e,n,r){$("MemoryPersistence","Starting transaction:",e);const i=new Rb(this.Br.next());return this.referenceDelegate.Kr(),r(i).next(s=>this.referenceDelegate.$r(i).next(()=>s)).toPromise().then(s=>(i.raiseOnCommittedEvent(),s))}Ur(e,n){return C.or(Object.values(this.Nr).map(r=>()=>r.containsKey(e,n)))}}class Rb extends vN{constructor(e){super(),this.currentSequenceNumber=e}}class Om{constructor(e){this.persistence=e,this.Wr=new bm,this.Gr=null}static zr(e){return new Om(e)}get jr(){if(this.Gr)return this.Gr;throw ne()}addReference(e,n,r){return this.Wr.addReference(r,n),this.jr.delete(r.toString()),C.resolve()}removeReference(e,n,r){return this.Wr.removeReference(r,n),this.jr.add(r.toString()),C.resolve()}markPotentiallyOrphaned(e,n){return this.jr.add(n.toString()),C.resolve()}removeTarget(e,n){this.Wr.Ar(n.targetId).forEach(i=>this.jr.add(i.toString()));const r=this.persistence.getTargetCache();return r.getMatchingKeysForTargetId(e,n.targetId).next(i=>{i.forEach(s=>this.jr.add(s.toString()))}).next(()=>r.removeTargetData(e,n))}Kr(){this.Gr=new Set}$r(e){const n=this.persistence.getRemoteDocumentCache().newChangeBuffer();return C.forEach(this.jr,r=>{const i=Z.fromPath(r);return this.Hr(e,i).next(s=>{s||n.removeEntry(i,Se.min())})}).next(()=>(this.Gr=null,n.apply(e)))}updateLimboDocument(e,n){return this.Hr(e,n).next(r=>{r?this.jr.delete(n.toString()):this.jr.add(n.toString())})}qr(e){return 0}Hr(e,n){return C.or([()=>C.resolve(this.Wr.containsKey(n)),()=>this.persistence.getTargetCache().containsKey(e,n),()=>this.persistence.Ur(e,n)])}}/**
 * @license
 * Copyright 2017 Google LLC
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */class Dm{constructor(e,n,r,i){this.targetId=e,this.fromCache=n,this.Li=r,this.ki=i}static qi(e,n){let r=pt(),i=pt();for(const s of n.docChanges)switch(s.type){case 0:r=r.add(s.doc.key);break;case 1:i=i.add(s.doc.key)}return new Dm(e,n.fromCache,r,i)}}/**
 * @license
 * Copyright 2019 Google LLC
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */class Cb{constructor(){this.Qi=!1}initialize(e,n){this.Ki=e,this.indexManager=n,this.Qi=!0}getDocumentsMatchingQuery(e,n,r,i){return this.$i(e,n).next(s=>s||this.Ui(e,n,i,r)).next(s=>s||this.Wi(e,n))}$i(e,n){if(Cy(n))return C.resolve(null);let r=Ns(n);return this.indexManager.getIndexType(e,r).next(i=>i===0?null:(n.limit!==null&&i===1&&(n=Ad(n,null,"F"),r=Ns(n)),this.indexManager.getDocumentsMatchingTarget(e,r).next(s=>{const o=pt(...s);return this.Ki.getDocuments(e,o).next(a=>this.indexManager.getMinOffset(e,r).next(l=>{const u=this.Gi(n,a);return this.zi(n,u,o,l.readTime)?this.$i(e,Ad(n,null,"F")):this.ji(e,u,n,l)}))})))}Ui(e,n,r,i){return Cy(n)||i.isEqual(Se.min())?this.Wi(e,n):this.Ki.getDocuments(e,r).next(s=>{const o=this.Gi(n,s);return this.zi(n,o,r,i)?this.Wi(e,n):(Iy()<=se.DEBUG&&$("QueryEngine","Re-using previous result from %s to execute query: %s",i.toString(),Ny(n)),this.ji(e,o,n,dN(i,-1)))})}Gi(e,n){let r=new It(VN(e));return n.forEach((i,s)=>{Cm(e,s)&&(r=r.add(s))}),r}zi(e,n,r,i){if(e.limit===null)return!1;if(r.size!==n.size)return!0;const s=e.limitType==="F"?n.last():n.first();return!!s&&(s.hasPendingWrites||s.version.compareTo(i)>0)}Wi(e,n){return Iy()<=se.DEBUG&&$("QueryEngine","Using full collection scan to execute query:",Ny(n)),this.Ki.getDocumentsMatchingQuery(e,n,Mr.min())}ji(e,n,r,i){return this.Ki.getDocumentsMatchingQuery(e,r,i).next(s=>(n.forEach(o=>{s=s.insert(o.key,o)}),s))}}/**
 * @license
 * Copyright 2020 Google LLC
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */class Nb{constructor(e,n,r,i){this.persistence=e,this.Hi=n,this.serializer=i,this.Ji=new bt(de),this.Yi=new zs(s=>Pm(s),Rm),this.Zi=new Map,this.Xi=e.getRemoteDocumentCache(),this.kr=e.getTargetCache(),this.Qr=e.getBundleCache(),this.es(r)}es(e){this.documentOverlayCache=this.persistence.getDocumentOverlayCache(e),this.indexManager=this.persistence.getIndexManager(e),this.mutationQueue=this.persistence.getMutationQueue(e,this.indexManager),this.localDocuments=new Eb(this.Xi,this.mutationQueue,this.documentOverlayCache,this.indexManager),this.Xi.setIndexManager(this.indexManager),this.Hi.initialize(this.localDocuments,this.indexManager)}collectGarbage(e){return this.persistence.runTransaction("Collect garbage","readwrite-primary",n=>e.collect(n,this.Ji))}}function bb(t,e,n,r){return new Nb(t,e,n,r)}async function gT(t,e){const n=we(t);return await n.persistence.runTransaction("Handle user change","readonly",r=>{let i;return n.mutationQueue.getAllMutationBatches(r).next(s=>(i=s,n.es(e),n.mutationQueue.getAllMutationBatches(r))).next(s=>{const o=[],a=[];let l=pt();for(const u of i){o.push(u.batchId);for(const c of u.mutations)l=l.add(c.key)}for(const u of s){a.push(u.batchId);for(const c of u.mutations)l=l.add(c.key)}return n.localDocuments.getDocuments(r,l).next(u=>({ts:u,removedBatchIds:o,addedBatchIds:a}))})})}function Ob(t,e){const n=we(t);return n.persistence.runTransaction("Acknowledge batch","readwrite-primary",r=>{const i=e.batch.keys(),s=n.Xi.newChangeBuffer({trackRemovals:!0});return function(a,l,u,c){const h=u.batch,d=h.keys();let y=C.resolve();return d.forEach(_=>{y=y.next(()=>c.getEntry(l,_)).next(w=>{const T=u.docVersions.get(_);Ze(T!==null),w.version.compareTo(T)<0&&(h.applyToRemoteDocument(w,u),w.isValidDocument()&&(w.setReadTime(u.commitVersion),c.addEntry(w)))})}),y.next(()=>a.mutationQueue.removeMutationBatch(l,h))}(n,r,e,s).next(()=>s.apply(r)).next(()=>n.mutationQueue.performConsistencyCheck(r)).next(()=>n.documentOverlayCache.removeOverlaysForBatchId(r,i,e.batch.batchId)).next(()=>n.localDocuments.recalculateAndSaveOverlaysForDocumentKeys(r,function(a){let l=pt();for(let u=0;u<a.mutationResults.length;++u)a.mutationResults[u].transformResults.length>0&&(l=l.add(a.batch.mutations[u].key));return l}(e))).next(()=>n.localDocuments.getDocuments(r,i))})}function Db(t){const e=we(t);return e.persistence.runTransaction("Get last remote snapshot version","readonly",n=>e.kr.getLastRemoteSnapshotVersion(n))}function Mb(t,e){const n=we(t);return n.persistence.runTransaction("Get next mutation batch","readonly",r=>(e===void 0&&(e=-1),n.mutationQueue.getNextMutationBatchAfterBatchId(r,e)))}class jy{constructor(){this.activeTargetIds=HN()}hs(e){this.activeTargetIds=this.activeTargetIds.add(e)}Ps(e){this.activeTargetIds=this.activeTargetIds.delete(e)}ls(){const e={activeTargetIds:this.activeTargetIds.toArray(),updateTimeMs:Date.now()};return JSON.stringify(e)}}class Lb{constructor(){this.Hs=new jy,this.Js={},this.onlineStateHandler=null,this.sequenceNumberHandler=null}addPendingMutation(e){}updateMutationState(e,n,r){}addLocalQueryTarget(e){return this.Hs.hs(e),this.Js[e]||"not-current"}updateQueryState(e,n,r){this.Js[e]=n}removeLocalQueryTarget(e){this.Hs.Ps(e)}isLocalQueryTarget(e){return this.Hs.activeTargetIds.has(e)}clearQueryState(e){delete this.Js[e]}getAllActiveQueryTargets(){return this.Hs.activeTargetIds}isActiveQueryTarget(e){return this.Hs.activeTargetIds.has(e)}start(){return this.Hs=new jy,Promise.resolve()}handleUserChange(e,n,r){}setOnlineState(e){}shutdown(){}writeSequenceNumber(e){}notifyBundleLoaded(e){}}/**
 * @license
 * Copyright 2019 Google LLC
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */class jb{Ys(e){}shutdown(){}}/**
 * @license
 * Copyright 2019 Google LLC
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */class Vy{constructor(){this.Zs=()=>this.Xs(),this.eo=()=>this.no(),this.ro=[],this.io()}Ys(e){this.ro.push(e)}shutdown(){window.removeEventListener("online",this.Zs),window.removeEventListener("offline",this.eo)}io(){window.addEventListener("online",this.Zs),window.addEventListener("offline",this.eo)}Xs(){$("ConnectivityMonitor","Network connectivity changed: AVAILABLE");for(const e of this.ro)e(0)}no(){$("ConnectivityMonitor","Network connectivity changed: UNAVAILABLE");for(const e of this.ro)e(1)}static v(){return typeof window<"u"&&window.addEventListener!==void 0&&window.removeEventListener!==void 0}}/**
 * @license
 * Copyright 2023 Google LLC
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */let xl=null;function Xf(){return xl===null?xl=function(){return 268435456+Math.round(2147483648*Math.random())}():xl++,"0x"+xl.toString(16)}/**
 * @license
 * Copyright 2020 Google LLC
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */const Vb={BatchGetDocuments:"batchGet",Commit:"commit",RunQuery:"runQuery",RunAggregationQuery:"runAggregationQuery"};/**
 * @license
 * Copyright 2017 Google LLC
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */class Fb{constructor(e){this.so=e.so,this.oo=e.oo}_o(e){this.ao=e}uo(e){this.co=e}onMessage(e){this.lo=e}close(){this.oo()}send(e){this.so(e)}ho(){this.ao()}Po(e){this.co(e)}Io(e){this.lo(e)}}/**
 * @license
 * Copyright 2017 Google LLC
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */const ft="WebChannelConnection";class Ub extends class{constructor(n){this.databaseInfo=n,this.databaseId=n.databaseId;const r=n.ssl?"https":"http",i=encodeURIComponent(this.databaseId.projectId),s=encodeURIComponent(this.databaseId.database);this.To=r+"://"+n.host,this.Eo=`projects/${i}/databases/${s}`,this.Ao=this.databaseId.database==="(default)"?`project_id=${i}`:`project_id=${i}&database_id=${s}`}get Ro(){return!1}Vo(n,r,i,s,o){const a=Xf(),l=this.mo(n,r);$("RestConnection",`Sending RPC '${n}' ${a}:`,l,i);const u={"google-cloud-resource-prefix":this.Eo,"x-goog-request-params":this.Ao};return this.fo(u,s,o),this.po(n,l,u,i).then(c=>($("RestConnection",`Received RPC '${n}' ${a}: `,c),c),c=>{throw Bu("RestConnection",`RPC '${n}' ${a} failed with error: `,c,"url: ",l,"request:",i),c})}yo(n,r,i,s,o,a){return this.Vo(n,r,i,s,o)}fo(n,r,i){n["X-Goog-Api-Client"]=function(){return"gl-js/ fire/"+$s}(),n["Content-Type"]="text/plain",this.databaseInfo.appId&&(n["X-Firebase-GMPID"]=this.databaseInfo.appId),r&&r.headers.forEach((s,o)=>n[o]=s),i&&i.headers.forEach((s,o)=>n[o]=s)}mo(n,r){const i=Vb[n];return`${this.To}/v1/${r}:${i}`}}{constructor(e){super(e),this.forceLongPolling=e.forceLongPolling,this.autoDetectLongPolling=e.autoDetectLongPolling,this.useFetchStreams=e.useFetchStreams,this.longPollingOptions=e.longPollingOptions}po(e,n,r,i){const s=Xf();return new Promise((o,a)=>{const l=new nN;l.setWithCredentials(!0),l.listenOnce(ZC.COMPLETE,()=>{try{switch(l.getLastErrorCode()){case Yf.NO_ERROR:const c=l.getResponseJson();$(ft,`XHR for RPC '${e}' ${s} received:`,JSON.stringify(c)),o(c);break;case Yf.TIMEOUT:$(ft,`RPC '${e}' ${s} timed out`),a(new G(R.DEADLINE_EXCEEDED,"Request time out"));break;case Yf.HTTP_ERROR:const h=l.getStatus();if($(ft,`RPC '${e}' ${s} failed with status:`,h,"response text:",l.getResponseText()),h>0){let d=l.getResponseJson();Array.isArray(d)&&(d=d[0]);const y=d==null?void 0:d.error;if(y&&y.status&&y.message){const _=function(T){const g=T.toLowerCase().replace(/_/g,"-");return Object.values(R).indexOf(g)>=0?g:R.UNKNOWN}(y.status);a(new G(_,y.message))}else a(new G(R.UNKNOWN,"Server responded with status "+l.getStatus()))}else a(new G(R.UNAVAILABLE,"Connection failed."));break;default:ne()}}finally{$(ft,`RPC '${e}' ${s} completed.`)}});const u=JSON.stringify(i);$(ft,`RPC '${e}' ${s} sending request:`,i),l.send(n,"POST",u,r,15)})}wo(e,n,r){const i=Xf(),s=[this.To,"/","google.firestore.v1.Firestore","/",e,"/channel"],o=XC(),a=JC(),l={httpSessionIdParam:"gsessionid",initMessageHeaders:{},messageUrlParams:{database:`projects/${this.databaseId.projectId}/databases/${this.databaseId.database}`},sendRawJson:!0,supportsCrossDomainXhr:!0,internalChannelParams:{forwardChannelRequestTimeoutMs:6e5},forceLongPolling:this.forceLongPolling,detectBufferingProxy:this.autoDetectLongPolling},u=this.longPollingOptions.timeoutSeconds;u!==void 0&&(l.longPollingTimeout=Math.round(1e3*u)),this.useFetchStreams&&(l.xmlHttpFactory=new tN({})),this.fo(l.initMessageHeaders,n,r),l.encodeInitMessageHeaders=!0;const c=s.join("");$(ft,`Creating RPC '${e}' stream ${i}: ${c}`,l);const h=o.createWebChannel(c,l);let d=!1,y=!1;const _=new Fb({so:T=>{y?$(ft,`Not sending because RPC '${e}' stream ${i} is closed:`,T):(d||($(ft,`Opening RPC '${e}' stream ${i} transport.`),h.open(),d=!0),$(ft,`RPC '${e}' stream ${i} sending:`,T),h.send(T))},oo:()=>h.close()}),w=(T,g,p)=>{T.listen(g,v=>{try{p(v)}catch(m){setTimeout(()=>{throw m},0)}})};return w(h,Il.EventType.OPEN,()=>{y||$(ft,`RPC '${e}' stream ${i} transport opened.`)}),w(h,Il.EventType.CLOSE,()=>{y||(y=!0,$(ft,`RPC '${e}' stream ${i} transport closed`),_.Po())}),w(h,Il.EventType.ERROR,T=>{y||(y=!0,Bu(ft,`RPC '${e}' stream ${i} transport errored:`,T),_.Po(new G(R.UNAVAILABLE,"The operation could not be completed")))}),w(h,Il.EventType.MESSAGE,T=>{var g;if(!y){const p=T.data[0];Ze(!!p);const v=p,m=v.error||((g=v[0])===null||g===void 0?void 0:g.error);if(m){$(ft,`RPC '${e}' stream ${i} received error:`,m);const S=m.status;let P=function(B){const U=De[B];if(U!==void 0)return nb(U)}(S),A=m.message;P===void 0&&(P=R.INTERNAL,A="Unknown error status: "+S+" with message "+m.message),y=!0,_.Po(new G(P,A)),h.close()}else $(ft,`RPC '${e}' stream ${i} received:`,p),_.Io(p)}}),w(a,eN.STAT_EVENT,T=>{T.stat===Ey.PROXY?$(ft,`RPC '${e}' stream ${i} detected buffering proxy`):T.stat===Ey.NOPROXY&&$(ft,`RPC '${e}' stream ${i} detected no buffering proxy`)}),setTimeout(()=>{_.ho()},0),_}}function Jf(){return typeof document<"u"?document:null}/**
 * @license
 * Copyright 2020 Google LLC
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */function Hc(t){return new rb(t,!0)}/**
 * @license
 * Copyright 2017 Google LLC
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */class vT{constructor(e,n,r=1e3,i=1.5,s=6e4){this.ii=e,this.timerId=n,this.So=r,this.bo=i,this.Do=s,this.vo=0,this.Co=null,this.Fo=Date.now(),this.reset()}reset(){this.vo=0}Mo(){this.vo=this.Do}xo(e){this.cancel();const n=Math.floor(this.vo+this.Oo()),r=Math.max(0,Date.now()-this.Fo),i=Math.max(0,n-r);i>0&&$("ExponentialBackoff",`Backing off for ${i} ms (base delay: ${this.vo} ms, delay with jitter: ${n} ms, last attempt: ${r} ms ago)`),this.Co=this.ii.enqueueAfterDelay(this.timerId,i,()=>(this.Fo=Date.now(),e())),this.vo*=this.bo,this.vo<this.So&&(this.vo=this.So),this.vo>this.Do&&(this.vo=this.Do)}No(){this.Co!==null&&(this.Co.skipDelay(),this.Co=null)}cancel(){this.Co!==null&&(this.Co.cancel(),this.Co=null)}Oo(){return(Math.random()-.5)*this.vo}}/**
 * @license
 * Copyright 2017 Google LLC
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */class $b{constructor(e,n,r,i,s,o,a,l){this.ii=e,this.Bo=r,this.Lo=i,this.connection=s,this.authCredentialsProvider=o,this.appCheckCredentialsProvider=a,this.listener=l,this.state=0,this.ko=0,this.qo=null,this.Qo=null,this.stream=null,this.Ko=new vT(e,n)}$o(){return this.state===1||this.state===5||this.Uo()}Uo(){return this.state===2||this.state===3}start(){this.state!==4?this.auth():this.Wo()}async stop(){this.$o()&&await this.close(0)}Go(){this.state=0,this.Ko.reset()}zo(){this.Uo()&&this.qo===null&&(this.qo=this.ii.enqueueAfterDelay(this.Bo,6e4,()=>this.jo()))}Ho(e){this.Jo(),this.stream.send(e)}async jo(){if(this.Uo())return this.close(0)}Jo(){this.qo&&(this.qo.cancel(),this.qo=null)}Yo(){this.Qo&&(this.Qo.cancel(),this.Qo=null)}async close(e,n){this.Jo(),this.Yo(),this.Ko.cancel(),this.ko++,e!==4?this.Ko.reset():n&&n.code===R.RESOURCE_EXHAUSTED?(Ii(n.toString()),Ii("Using maximum backoff delay to prevent overloading the backend."),this.Ko.Mo()):n&&n.code===R.UNAUTHENTICATED&&this.state!==3&&(this.authCredentialsProvider.invalidateToken(),this.appCheckCredentialsProvider.invalidateToken()),this.stream!==null&&(this.Zo(),this.stream.close(),this.stream=null),this.state=e,await this.listener.uo(n)}Zo(){}auth(){this.state=1;const e=this.Xo(this.ko),n=this.ko;Promise.all([this.authCredentialsProvider.getToken(),this.appCheckCredentialsProvider.getToken()]).then(([r,i])=>{this.ko===n&&this.e_(r,i)},r=>{e(()=>{const i=new G(R.UNKNOWN,"Fetching auth token failed: "+r.message);return this.t_(i)})})}e_(e,n){const r=this.Xo(this.ko);this.stream=this.n_(e,n),this.stream._o(()=>{r(()=>(this.state=2,this.Qo=this.ii.enqueueAfterDelay(this.Lo,1e4,()=>(this.Uo()&&(this.state=3),Promise.resolve())),this.listener._o()))}),this.stream.uo(i=>{r(()=>this.t_(i))}),this.stream.onMessage(i=>{r(()=>this.onMessage(i))})}Wo(){this.state=5,this.Ko.xo(async()=>{this.state=0,this.start()})}t_(e){return $("PersistentStream",`close with error: ${e}`),this.stream=null,this.close(4,e)}Xo(e){return n=>{this.ii.enqueueAndForget(()=>this.ko===e?n():($("PersistentStream","stream callback skipped by getCloseGuardedDispatcher."),Promise.resolve()))}}}class zb extends $b{constructor(e,n,r,i,s,o){super(e,"write_stream_connection_backoff","write_stream_idle","health_check_timeout",n,r,i,o),this.serializer=s,this.o_=!1}get __(){return this.o_}start(){this.o_=!1,this.lastStreamToken=void 0,super.start()}Zo(){this.o_&&this.a_([])}n_(e,n){return this.connection.wo("Write",e,n)}onMessage(e){if(Ze(!!e.streamToken),this.lastStreamToken=e.streamToken,this.o_){this.Ko.reset();const n=fb(e.writeResults,e.commitTime),r=ps(e.commitTime);return this.listener.u_(r,n)}return Ze(!e.writeResults||e.writeResults.length===0),this.o_=!0,this.listener.c_()}l_(){const e={};e.database=lb(this.serializer),this.Ho(e)}a_(e){const n={streamToken:this.lastStreamToken,writes:e.map(r=>cb(this.serializer,r))};this.Ho(n)}}/**
 * @license
 * Copyright 2017 Google LLC
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */class Bb extends class{}{constructor(e,n,r,i){super(),this.authCredentials=e,this.appCheckCredentials=n,this.connection=r,this.serializer=i,this.h_=!1}P_(){if(this.h_)throw new G(R.FAILED_PRECONDITION,"The client has already been terminated.")}Vo(e,n,r){return this.P_(),Promise.all([this.authCredentials.getToken(),this.appCheckCredentials.getToken()]).then(([i,s])=>this.connection.Vo(e,n,r,i,s)).catch(i=>{throw i.name==="FirebaseError"?(i.code===R.UNAUTHENTICATED&&(this.authCredentials.invalidateToken(),this.appCheckCredentials.invalidateToken()),i):new G(R.UNKNOWN,i.toString())})}yo(e,n,r,i){return this.P_(),Promise.all([this.authCredentials.getToken(),this.appCheckCredentials.getToken()]).then(([s,o])=>this.connection.yo(e,n,r,s,o,i)).catch(s=>{throw s.name==="FirebaseError"?(s.code===R.UNAUTHENTICATED&&(this.authCredentials.invalidateToken(),this.appCheckCredentials.invalidateToken()),s):new G(R.UNKNOWN,s.toString())})}terminate(){this.h_=!0}}class Hb{constructor(e,n){this.asyncQueue=e,this.onlineStateHandler=n,this.state="Unknown",this.T_=0,this.E_=null,this.d_=!0}A_(){this.T_===0&&(this.R_("Unknown"),this.E_=this.asyncQueue.enqueueAfterDelay("online_state_timeout",1e4,()=>(this.E_=null,this.V_("Backend didn't respond within 10 seconds."),this.R_("Offline"),Promise.resolve())))}m_(e){this.state==="Online"?this.R_("Unknown"):(this.T_++,this.T_>=1&&(this.f_(),this.V_(`Connection failed 1 times. Most recent error: ${e.toString()}`),this.R_("Offline")))}set(e){this.f_(),this.T_=0,e==="Online"&&(this.d_=!1),this.R_(e)}R_(e){e!==this.state&&(this.state=e,this.onlineStateHandler(e))}V_(e){const n=`Could not reach Cloud Firestore backend. ${e}
This typically indicates that your device does not have a healthy Internet connection at the moment. The client will operate in offline mode until it is able to successfully connect to the backend.`;this.d_?(Ii(n),this.d_=!1):$("OnlineStateTracker",n)}f_(){this.E_!==null&&(this.E_.cancel(),this.E_=null)}}/**
 * @license
 * Copyright 2017 Google LLC
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */class Wb{constructor(e,n,r,i,s){this.localStore=e,this.datastore=n,this.asyncQueue=r,this.remoteSyncer={},this.g_=[],this.p_=new Map,this.y_=new Set,this.w_=[],this.S_=s,this.S_.Ys(o=>{r.enqueueAndForget(async()=>{Ga(this)&&($("RemoteStore","Restarting streams for network reachability change."),await async function(l){const u=we(l);u.y_.add(4),await Ka(u),u.b_.set("Unknown"),u.y_.delete(4),await Wc(u)}(this))})}),this.b_=new Hb(r,i)}}async function Wc(t){if(Ga(t))for(const e of t.w_)await e(!0)}async function Ka(t){for(const e of t.w_)await e(!1)}function Ga(t){return we(t).y_.size===0}async function yT(t,e,n){if(!Uc(e))throw e;t.y_.add(1),await Ka(t),t.b_.set("Offline"),n||(n=()=>Db(t.localStore)),t.asyncQueue.enqueueRetryable(async()=>{$("RemoteStore","Retrying IndexedDB access"),await n(),t.y_.delete(1),await Wc(t)})}function _T(t,e){return e().catch(n=>yT(t,n,e))}async function Kc(t){const e=we(t),n=jr(e);let r=e.g_.length>0?e.g_[e.g_.length-1].batchId:-1;for(;Kb(e);)try{const i=await Mb(e.localStore,r);if(i===null){e.g_.length===0&&n.zo();break}r=i.batchId,Gb(e,i)}catch(i){await yT(e,i)}wT(e)&&ET(e)}function Kb(t){return Ga(t)&&t.g_.length<10}function Gb(t,e){t.g_.push(e);const n=jr(t);n.Uo()&&n.__&&n.a_(e.mutations)}function wT(t){return Ga(t)&&!jr(t).$o()&&t.g_.length>0}function ET(t){jr(t).start()}async function qb(t){jr(t).l_()}async function Qb(t){const e=jr(t);for(const n of t.g_)e.a_(n.mutations)}async function Yb(t,e,n){const r=t.g_.shift(),i=Nm.from(r,e,n);await _T(t,()=>t.remoteSyncer.applySuccessfulWrite(i)),await Kc(t)}async function Xb(t,e){e&&jr(t).__&&await async function(r,i){if(function(o){return tb(o)&&o!==R.ABORTED}(i.code)){const s=r.g_.shift();jr(r).Go(),await _T(r,()=>r.remoteSyncer.rejectFailedWrite(s.batchId,i)),await Kc(r)}}(t,e),wT(t)&&ET(t)}async function Fy(t,e){const n=we(t);n.asyncQueue.verifyOperationInProgress(),$("RemoteStore","RemoteStore received new credentials");const r=Ga(n);n.y_.add(3),await Ka(n),r&&n.b_.set("Unknown"),await n.remoteSyncer.handleCredentialChange(e),n.y_.delete(3),await Wc(n)}async function Jb(t,e){const n=we(t);e?(n.y_.delete(2),await Wc(n)):e||(n.y_.add(2),await Ka(n),n.b_.set("Unknown"))}function jr(t){return t.C_||(t.C_=function(n,r,i){const s=we(n);return s.P_(),new zb(r,s.connection,s.authCredentials,s.appCheckCredentials,s.serializer,i)}(t.datastore,t.asyncQueue,{_o:qb.bind(null,t),uo:Xb.bind(null,t),c_:Qb.bind(null,t),u_:Yb.bind(null,t)}),t.w_.push(async e=>{e?(t.C_.Go(),await Kc(t)):(await t.C_.stop(),t.g_.length>0&&($("RemoteStore",`Stopping write stream with ${t.g_.length} pending writes`),t.g_=[]))})),t.C_}/**
 * @license
 * Copyright 2017 Google LLC
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */class Mm{constructor(e,n,r,i,s){this.asyncQueue=e,this.timerId=n,this.targetTimeMs=r,this.op=i,this.removalCallback=s,this.deferred=new fi,this.then=this.deferred.promise.then.bind(this.deferred.promise),this.deferred.promise.catch(o=>{})}static createAndSchedule(e,n,r,i,s){const o=Date.now()+r,a=new Mm(e,n,o,i,s);return a.start(r),a}start(e){this.timerHandle=setTimeout(()=>this.handleDelayElapsed(),e)}skipDelay(){return this.handleDelayElapsed()}cancel(e){this.timerHandle!==null&&(this.clearTimeout(),this.deferred.reject(new G(R.CANCELLED,"Operation cancelled"+(e?": "+e:""))))}handleDelayElapsed(){this.asyncQueue.enqueueAndForget(()=>this.timerHandle!==null?(this.clearTimeout(),this.op().then(e=>this.deferred.resolve(e))):Promise.resolve())}clearTimeout(){this.timerHandle!==null&&(this.removalCallback(this),clearTimeout(this.timerHandle),this.timerHandle=null)}}function TT(t,e){if(Ii("AsyncQueue",`${e}: ${t}`),Uc(t))return new G(R.UNAVAILABLE,`${e}: ${t}`);throw t}class Zb{constructor(){this.queries=new zs(e=>tT(e),eT),this.onlineState="Unknown",this.O_=new Set}}function e4(t){t.O_.forEach(e=>{e.next()})}class t4{constructor(e,n,r,i,s,o){this.localStore=e,this.remoteStore=n,this.eventManager=r,this.sharedClientState=i,this.currentUser=s,this.maxConcurrentLimboResolutions=o,this.Ta={},this.Ea=new zs(a=>tT(a),eT),this.da=new Map,this.Aa=new Set,this.Ra=new bt(Z.comparator),this.Va=new Map,this.ma=new bm,this.fa={},this.ga=new Map,this.pa=bs.On(),this.onlineState="Unknown",this.ya=void 0}get isPrimaryClient(){return this.ya===!0}}async function n4(t,e,n){const r=o4(t);try{const i=await function(o,a){const l=we(o),u=ze.now(),c=a.reduce((y,_)=>y.add(_.key),pt());let h,d;return l.persistence.runTransaction("Locally write mutations","readwrite",y=>{let _=qu(),w=pt();return l.Xi.getEntries(y,c).next(T=>{_=T,_.forEach((g,p)=>{p.isValidDocument()||(w=w.add(g))})}).next(()=>l.localDocuments.getOverlayedDocuments(y,_)).next(T=>{h=T;const g=[];for(const p of a){const v=XN(p,h.get(p.key).overlayedDocument);v!=null&&g.push(new Ni(p.key,v,GE(v.value.mapValue),Un.exists(!0)))}return l.mutationQueue.addMutationBatch(y,u,g,a)}).next(T=>{d=T;const g=T.applyToLocalDocumentSet(h,w);return l.documentOverlayCache.saveOverlays(y,T.batchId,g)})}).then(()=>({batchId:d.batchId,changes:rT(h)}))}(r.localStore,e);r.sharedClientState.addPendingMutation(i.batchId),function(o,a,l){let u=o.fa[o.currentUser.toKey()];u||(u=new bt(de)),u=u.insert(a,l),o.fa[o.currentUser.toKey()]=u}(r,i.batchId,n),await Gc(r,i.changes),await Kc(r.remoteStore)}catch(i){const s=TT(i,"Failed to persist write");n.reject(s)}}function Uy(t,e,n){const r=we(t);if(r.isPrimaryClient&&n===0||!r.isPrimaryClient&&n===1){const i=[];r.Ea.forEach((s,o)=>{const a=o.view.N_(e);a.snapshot&&i.push(a.snapshot)}),function(o,a){const l=we(o);l.onlineState=a;let u=!1;l.queries.forEach((c,h)=>{for(const d of h.listeners)d.N_(a)&&(u=!0)}),u&&e4(l)}(r.eventManager,e),i.length&&r.Ta.r_(i),r.onlineState=e,r.isPrimaryClient&&r.sharedClientState.setOnlineState(e)}}async function r4(t,e){const n=we(t),r=e.batch.batchId;try{const i=await Ob(n.localStore,e);ST(n,r,null),IT(n,r),n.sharedClientState.updateMutationState(r,"acknowledged"),await Gc(n,i)}catch(i){await BE(i)}}async function i4(t,e,n){const r=we(t);try{const i=await function(o,a){const l=we(o);return l.persistence.runTransaction("Reject batch","readwrite-primary",u=>{let c;return l.mutationQueue.lookupMutationBatch(u,a).next(h=>(Ze(h!==null),c=h.keys(),l.mutationQueue.removeMutationBatch(u,h))).next(()=>l.mutationQueue.performConsistencyCheck(u)).next(()=>l.documentOverlayCache.removeOverlaysForBatchId(u,c,a)).next(()=>l.localDocuments.recalculateAndSaveOverlaysForDocumentKeys(u,c)).next(()=>l.localDocuments.getDocuments(u,c))})}(r.localStore,e);ST(r,e,n),IT(r,e),r.sharedClientState.updateMutationState(e,"rejected",n),await Gc(r,i)}catch(i){await BE(i)}}function IT(t,e){(t.ga.get(e)||[]).forEach(n=>{n.resolve()}),t.ga.delete(e)}function ST(t,e,n){const r=we(t);let i=r.fa[r.currentUser.toKey()];if(i){const s=i.get(e);s&&(n?s.reject(n):s.resolve(),i=i.remove(e)),r.fa[r.currentUser.toKey()]=i}}async function Gc(t,e,n){const r=we(t),i=[],s=[],o=[];r.Ea.isEmpty()||(r.Ea.forEach((a,l)=>{o.push(r.wa(l,e,n).then(u=>{if((u||n)&&r.isPrimaryClient&&r.sharedClientState.updateQueryState(l.targetId,u!=null&&u.fromCache?"not-current":"current"),u){i.push(u);const c=Dm.qi(l.targetId,u);s.push(c)}}))}),await Promise.all(o),r.Ta.r_(i),await async function(l,u){const c=we(l);try{await c.persistence.runTransaction("notifyLocalViewChanges","readwrite",h=>C.forEach(u,d=>C.forEach(d.Li,y=>c.persistence.referenceDelegate.addReference(h,d.targetId,y)).next(()=>C.forEach(d.ki,y=>c.persistence.referenceDelegate.removeReference(h,d.targetId,y)))))}catch(h){if(!Uc(h))throw h;$("LocalStore","Failed to update sequence numbers: "+h)}for(const h of u){const d=h.targetId;if(!h.fromCache){const y=c.Ji.get(d),_=y.snapshotVersion,w=y.withLastLimboFreeSnapshotVersion(_);c.Ji=c.Ji.insert(d,w)}}}(r.localStore,s))}async function s4(t,e){const n=we(t);if(!n.currentUser.isEqual(e)){$("SyncEngine","User change. New user:",e.toKey());const r=await gT(n.localStore,e);n.currentUser=e,function(s,o){s.ga.forEach(a=>{a.forEach(l=>{l.reject(new G(R.CANCELLED,o))})}),s.ga.clear()}(n,"'waitForPendingWrites' promise is rejected due to a user change."),n.sharedClientState.handleUserChange(e,r.removedBatchIds,r.addedBatchIds),await Gc(n,r.ts)}}function o4(t){const e=we(t);return e.remoteStore.remoteSyncer.applySuccessfulWrite=r4.bind(null,e),e.remoteStore.remoteSyncer.rejectFailedWrite=i4.bind(null,e),e}class $y{constructor(){this.synchronizeTabs=!1}async initialize(e){this.serializer=Hc(e.databaseInfo.databaseId),this.sharedClientState=this.createSharedClientState(e),this.persistence=this.createPersistence(e),await this.persistence.start(),this.localStore=this.createLocalStore(e),this.gcScheduler=this.createGarbageCollectionScheduler(e,this.localStore),this.indexBackfillerScheduler=this.createIndexBackfillerScheduler(e,this.localStore)}createGarbageCollectionScheduler(e,n){return null}createIndexBackfillerScheduler(e,n){return null}createLocalStore(e){return bb(this.persistence,new Cb,e.initialUser,this.serializer)}createPersistence(e){return new Pb(Om.zr,this.serializer)}createSharedClientState(e){return new Lb}async terminate(){this.gcScheduler&&this.gcScheduler.stop(),await this.sharedClientState.shutdown(),await this.persistence.shutdown()}}class a4{async initialize(e,n){this.localStore||(this.localStore=e.localStore,this.sharedClientState=e.sharedClientState,this.datastore=this.createDatastore(n),this.remoteStore=this.createRemoteStore(n),this.eventManager=this.createEventManager(n),this.syncEngine=this.createSyncEngine(n,!e.synchronizeTabs),this.sharedClientState.onlineStateHandler=r=>Uy(this.syncEngine,r,1),this.remoteStore.remoteSyncer.handleCredentialChange=s4.bind(null,this.syncEngine),await Jb(this.remoteStore,this.syncEngine.isPrimaryClient))}createEventManager(e){return function(){return new Zb}()}createDatastore(e){const n=Hc(e.databaseInfo.databaseId),r=function(s){return new Ub(s)}(e.databaseInfo);return function(s,o,a,l){return new Bb(s,o,a,l)}(e.authCredentials,e.appCheckCredentials,r,n)}createRemoteStore(e){return function(r,i,s,o,a){return new Wb(r,i,s,o,a)}(this.localStore,this.datastore,e.asyncQueue,n=>Uy(this.syncEngine,n,0),function(){return Vy.v()?new Vy:new jb}())}createSyncEngine(e,n){return function(i,s,o,a,l,u,c){const h=new t4(i,s,o,a,l,u);return c&&(h.ya=!0),h}(this.localStore,this.remoteStore,this.eventManager,this.sharedClientState,e.initialUser,e.maxConcurrentLimboResolutions,n)}terminate(){return async function(n){const r=we(n);$("RemoteStore","RemoteStore shutting down."),r.y_.add(5),await Ka(r),r.S_.shutdown(),r.b_.set("Unknown")}(this.remoteStore)}}/**
 * @license
 * Copyright 2017 Google LLC
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */class l4{constructor(e,n,r,i){this.authCredentials=e,this.appCheckCredentials=n,this.asyncQueue=r,this.databaseInfo=i,this.user=ht.UNAUTHENTICATED,this.clientId=zE.V(),this.authCredentialListener=()=>Promise.resolve(),this.appCheckCredentialListener=()=>Promise.resolve(),this.authCredentials.start(r,async s=>{$("FirestoreClient","Received user=",s.uid),await this.authCredentialListener(s),this.user=s}),this.appCheckCredentials.start(r,s=>($("FirestoreClient","Received new app check token=",s),this.appCheckCredentialListener(s,this.user)))}async getConfiguration(){return{asyncQueue:this.asyncQueue,databaseInfo:this.databaseInfo,clientId:this.clientId,authCredentials:this.authCredentials,appCheckCredentials:this.appCheckCredentials,initialUser:this.user,maxConcurrentLimboResolutions:100}}setCredentialChangeListener(e){this.authCredentialListener=e}setAppCheckTokenChangeListener(e){this.appCheckCredentialListener=e}verifyNotTerminated(){if(this.asyncQueue.isShuttingDown)throw new G(R.FAILED_PRECONDITION,"The client has already been terminated.")}terminate(){this.asyncQueue.enterRestrictedMode();const e=new fi;return this.asyncQueue.enqueueAndForgetEvenWhileRestricted(async()=>{try{this._onlineComponents&&await this._onlineComponents.terminate(),this._offlineComponents&&await this._offlineComponents.terminate(),this.authCredentials.shutdown(),this.appCheckCredentials.shutdown(),e.resolve()}catch(n){const r=TT(n,"Failed to shutdown persistence");e.reject(r)}}),e.promise}}async function Zf(t,e){t.asyncQueue.verifyOperationInProgress(),$("FirestoreClient","Initializing OfflineComponentProvider");const n=await t.getConfiguration();await e.initialize(n);let r=n.initialUser;t.setCredentialChangeListener(async i=>{r.isEqual(i)||(await gT(e.localStore,i),r=i)}),e.persistence.setDatabaseDeletedListener(()=>t.terminate()),t._offlineComponents=e}async function zy(t,e){t.asyncQueue.verifyOperationInProgress();const n=await c4(t);$("FirestoreClient","Initializing OnlineComponentProvider");const r=await t.getConfiguration();await e.initialize(n,r),t.setCredentialChangeListener(i=>Fy(e.remoteStore,i)),t.setAppCheckTokenChangeListener((i,s)=>Fy(e.remoteStore,s)),t._onlineComponents=e}function u4(t){return t.name==="FirebaseError"?t.code===R.FAILED_PRECONDITION||t.code===R.UNIMPLEMENTED:!(typeof DOMException<"u"&&t instanceof DOMException)||t.code===22||t.code===20||t.code===11}async function c4(t){if(!t._offlineComponents)if(t._uninitializedComponentsProvider){$("FirestoreClient","Using user provided OfflineComponentProvider");try{await Zf(t,t._uninitializedComponentsProvider._offline)}catch(e){const n=e;if(!u4(n))throw n;Bu("Error using user provided cache. Falling back to memory cache: "+n),await Zf(t,new $y)}}else $("FirestoreClient","Using default OfflineComponentProvider"),await Zf(t,new $y);return t._offlineComponents}async function f4(t){return t._onlineComponents||(t._uninitializedComponentsProvider?($("FirestoreClient","Using user provided OnlineComponentProvider"),await zy(t,t._uninitializedComponentsProvider._online)):($("FirestoreClient","Using default OnlineComponentProvider"),await zy(t,new a4))),t._onlineComponents}function h4(t){return f4(t).then(e=>e.syncEngine)}/**
 * @license
 * Copyright 2023 Google LLC
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */function kT(t){const e={};return t.timeoutSeconds!==void 0&&(e.timeoutSeconds=t.timeoutSeconds),e}/**
 * @license
 * Copyright 2020 Google LLC
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */const By=new Map;/**
 * @license
 * Copyright 2017 Google LLC
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */function d4(t,e,n){if(!n)throw new G(R.INVALID_ARGUMENT,`Function ${t}() cannot be called with an empty ${e}.`)}function p4(t,e,n,r){if(e===!0&&r===!0)throw new G(R.INVALID_ARGUMENT,`${t} and ${n} cannot be used together.`)}function Hy(t){if(!Z.isDocumentKey(t))throw new G(R.INVALID_ARGUMENT,`Invalid document reference. Document references must have an even number of segments, but ${t} has ${t.length}.`)}function Lm(t){if(t===void 0)return"undefined";if(t===null)return"null";if(typeof t=="string")return t.length>20&&(t=`${t.substring(0,20)}...`),JSON.stringify(t);if(typeof t=="number"||typeof t=="boolean")return""+t;if(typeof t=="object"){if(t instanceof Array)return"an array";{const e=function(r){return r.constructor?r.constructor.name:null}(t);return e?`a custom ${e} object`:"an object"}}return typeof t=="function"?"a function":ne()}function Rd(t,e){if("_delegate"in t&&(t=t._delegate),!(t instanceof e)){if(e.name===t.constructor.name)throw new G(R.INVALID_ARGUMENT,"Type does not match the expected instance. Did you pass a reference from a different Firestore SDK?");{const n=Lm(t);throw new G(R.INVALID_ARGUMENT,`Expected type '${e.name}', but it was: ${n}`)}}return t}/**
 * @license
 * Copyright 2020 Google LLC
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */class Wy{constructor(e){var n,r;if(e.host===void 0){if(e.ssl!==void 0)throw new G(R.INVALID_ARGUMENT,"Can't provide ssl option if host option is not set");this.host="firestore.googleapis.com",this.ssl=!0}else this.host=e.host,this.ssl=(n=e.ssl)===null||n===void 0||n;if(this.credentials=e.credentials,this.ignoreUndefinedProperties=!!e.ignoreUndefinedProperties,this.localCache=e.localCache,e.cacheSizeBytes===void 0)this.cacheSizeBytes=41943040;else{if(e.cacheSizeBytes!==-1&&e.cacheSizeBytes<1048576)throw new G(R.INVALID_ARGUMENT,"cacheSizeBytes must be at least 1048576");this.cacheSizeBytes=e.cacheSizeBytes}p4("experimentalForceLongPolling",e.experimentalForceLongPolling,"experimentalAutoDetectLongPolling",e.experimentalAutoDetectLongPolling),this.experimentalForceLongPolling=!!e.experimentalForceLongPolling,this.experimentalForceLongPolling?this.experimentalAutoDetectLongPolling=!1:e.experimentalAutoDetectLongPolling===void 0?this.experimentalAutoDetectLongPolling=!0:this.experimentalAutoDetectLongPolling=!!e.experimentalAutoDetectLongPolling,this.experimentalLongPollingOptions=kT((r=e.experimentalLongPollingOptions)!==null&&r!==void 0?r:{}),function(s){if(s.timeoutSeconds!==void 0){if(isNaN(s.timeoutSeconds))throw new G(R.INVALID_ARGUMENT,`invalid long polling timeout: ${s.timeoutSeconds} (must not be NaN)`);if(s.timeoutSeconds<5)throw new G(R.INVALID_ARGUMENT,`invalid long polling timeout: ${s.timeoutSeconds} (minimum allowed value is 5)`);if(s.timeoutSeconds>30)throw new G(R.INVALID_ARGUMENT,`invalid long polling timeout: ${s.timeoutSeconds} (maximum allowed value is 30)`)}}(this.experimentalLongPollingOptions),this.useFetchStreams=!!e.useFetchStreams}isEqual(e){return this.host===e.host&&this.ssl===e.ssl&&this.credentials===e.credentials&&this.cacheSizeBytes===e.cacheSizeBytes&&this.experimentalForceLongPolling===e.experimentalForceLongPolling&&this.experimentalAutoDetectLongPolling===e.experimentalAutoDetectLongPolling&&function(r,i){return r.timeoutSeconds===i.timeoutSeconds}(this.experimentalLongPollingOptions,e.experimentalLongPollingOptions)&&this.ignoreUndefinedProperties===e.ignoreUndefinedProperties&&this.useFetchStreams===e.useFetchStreams}}class jm{constructor(e,n,r,i){this._authCredentials=e,this._appCheckCredentials=n,this._databaseId=r,this._app=i,this.type="firestore-lite",this._persistenceKey="(lite)",this._settings=new Wy({}),this._settingsFrozen=!1}get app(){if(!this._app)throw new G(R.FAILED_PRECONDITION,"Firestore was not initialized using the Firebase SDK. 'app' is not available");return this._app}get _initialized(){return this._settingsFrozen}get _terminated(){return this._terminateTask!==void 0}_setSettings(e){if(this._settingsFrozen)throw new G(R.FAILED_PRECONDITION,"Firestore has already been started and its settings can no longer be changed. You can only modify settings before calling any other methods on a Firestore object.");this._settings=new Wy(e),e.credentials!==void 0&&(this._authCredentials=function(r){if(!r)return new iN;switch(r.type){case"firstParty":return new lN(r.sessionIndex||"0",r.iamToken||null,r.authTokenFactory||null);case"provider":return r.client;default:throw new G(R.INVALID_ARGUMENT,"makeAuthCredentialsProvider failed due to invalid credential type")}}(e.credentials))}_getSettings(){return this._settings}_freezeSettings(){return this._settingsFrozen=!0,this._settings}_delete(){return this._terminateTask||(this._terminateTask=this._terminate()),this._terminateTask}toJSON(){return{app:this._app,databaseId:this._databaseId,settings:this._settings}}_terminate(){return function(n){const r=By.get(n);r&&($("ComponentProvider","Removing Datastore"),By.delete(n),r.terminate())}(this),Promise.resolve()}}function m4(t,e,n,r={}){var i;const s=(t=Rd(t,jm))._getSettings(),o=`${e}:${n}`;if(s.host!=="firestore.googleapis.com"&&s.host!==o&&Bu("Host has been set in both settings() and connectFirestoreEmulator(), emulator host will be used."),t._setSettings(Object.assign(Object.assign({},s),{host:o,ssl:!1})),r.mockUserToken){let a,l;if(typeof r.mockUserToken=="string")a=r.mockUserToken,l=ht.MOCK_USER;else{a=NA(r.mockUserToken,(i=t._app)===null||i===void 0?void 0:i.options.projectId);const u=r.mockUserToken.sub||r.mockUserToken.user_id;if(!u)throw new G(R.INVALID_ARGUMENT,"mockUserToken must contain 'sub' or 'user_id' field!");l=new ht(u)}t._authCredentials=new sN(new $E(a,l))}}/**
 * @license
 * Copyright 2020 Google LLC
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */class Vm{constructor(e,n,r){this.converter=n,this._query=r,this.type="query",this.firestore=e}withConverter(e){return new Vm(this.firestore,e,this._query)}}class $n{constructor(e,n,r){this.converter=n,this._key=r,this.type="document",this.firestore=e}get _path(){return this._key.path}get id(){return this._key.path.lastSegment()}get path(){return this._key.path.canonicalString()}get parent(){return new pa(this.firestore,this.converter,this._key.path.popLast())}withConverter(e){return new $n(this.firestore,e,this._key)}}class pa extends Vm{constructor(e,n,r){super(e,n,DN(r)),this._path=r,this.type="collection"}get id(){return this._query.path.lastSegment()}get path(){return this._query.path.canonicalString()}get parent(){const e=this._path.popLast();return e.isEmpty()?null:new $n(this.firestore,null,new Z(e))}withConverter(e){return new pa(this.firestore,e,this._path)}}function g4(t,e,...n){if(t=kn(t),arguments.length===1&&(e=zE.V()),d4("doc","path",e),t instanceof jm){const r=Le.fromString(e,...n);return Hy(r),new $n(t,null,new Z(r))}{if(!(t instanceof $n||t instanceof pa))throw new G(R.INVALID_ARGUMENT,"Expected first argument to collection() to be a CollectionReference, a DocumentReference or FirebaseFirestore");const r=t._path.child(Le.fromString(e,...n));return Hy(r),new $n(t.firestore,t instanceof pa?t.converter:null,new Z(r))}}/**
 * @license
 * Copyright 2020 Google LLC
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */class v4{constructor(){this.Wa=Promise.resolve(),this.Ga=[],this.za=!1,this.ja=[],this.Ha=null,this.Ja=!1,this.Ya=!1,this.Za=[],this.Ko=new vT(this,"async_queue_retry"),this.Xa=()=>{const n=Jf();n&&$("AsyncQueue","Visibility state changed to "+n.visibilityState),this.Ko.No()};const e=Jf();e&&typeof e.addEventListener=="function"&&e.addEventListener("visibilitychange",this.Xa)}get isShuttingDown(){return this.za}enqueueAndForget(e){this.enqueue(e)}enqueueAndForgetEvenWhileRestricted(e){this.eu(),this.tu(e)}enterRestrictedMode(e){if(!this.za){this.za=!0,this.Ya=e||!1;const n=Jf();n&&typeof n.removeEventListener=="function"&&n.removeEventListener("visibilitychange",this.Xa)}}enqueue(e){if(this.eu(),this.za)return new Promise(()=>{});const n=new fi;return this.tu(()=>this.za&&this.Ya?Promise.resolve():(e().then(n.resolve,n.reject),n.promise)).then(()=>n.promise)}enqueueRetryable(e){this.enqueueAndForget(()=>(this.Ga.push(e),this.nu()))}async nu(){if(this.Ga.length!==0){try{await this.Ga[0](),this.Ga.shift(),this.Ko.reset()}catch(e){if(!Uc(e))throw e;$("AsyncQueue","Operation failed with retryable error: "+e)}this.Ga.length>0&&this.Ko.xo(()=>this.nu())}}tu(e){const n=this.Wa.then(()=>(this.Ja=!0,e().catch(r=>{this.Ha=r,this.Ja=!1;const i=function(o){let a=o.message||"";return o.stack&&(a=o.stack.includes(o.message)?o.stack:o.message+`
`+o.stack),a}(r);throw Ii("INTERNAL UNHANDLED ERROR: ",i),r}).then(r=>(this.Ja=!1,r))));return this.Wa=n,n}enqueueAfterDelay(e,n,r){this.eu(),this.Za.indexOf(e)>-1&&(n=0);const i=Mm.createAndSchedule(this,e,n,r,s=>this.ru(s));return this.ja.push(i),i}eu(){this.Ha&&ne()}verifyOperationInProgress(){}async iu(){let e;do e=this.Wa,await e;while(e!==this.Wa)}su(e){for(const n of this.ja)if(n.timerId===e)return!0;return!1}ou(e){return this.iu().then(()=>{this.ja.sort((n,r)=>n.targetTimeMs-r.targetTimeMs);for(const n of this.ja)if(n.skipDelay(),e!=="all"&&n.timerId===e)break;return this.iu()})}_u(e){this.Za.push(e)}ru(e){const n=this.ja.indexOf(e);this.ja.splice(n,1)}}class AT extends jm{constructor(e,n,r,i){super(e,n,r,i),this.type="firestore",this._queue=function(){return new v4}(),this._persistenceKey=(i==null?void 0:i.name)||"[DEFAULT]"}_terminate(){return this._firestoreClient||xT(this),this._firestoreClient.terminate()}}function y4(t,e){const n=typeof t=="object"?t:nw(),r=typeof t=="string"?t:e||"(default)",i=Gp(n,"firestore").getImmediate({identifier:r});if(!i._initialized){const s=RA("firestore");s&&m4(i,...s)}return i}function _4(t){return t._firestoreClient||xT(t),t._firestoreClient.verifyNotTerminated(),t._firestoreClient}function xT(t){var e,n,r;const i=t._freezeSettings(),s=function(a,l,u,c){return new EN(a,l,u,c.host,c.ssl,c.experimentalForceLongPolling,c.experimentalAutoDetectLongPolling,kT(c.experimentalLongPollingOptions),c.useFetchStreams)}(t._databaseId,((e=t._app)===null||e===void 0?void 0:e.options.appId)||"",t._persistenceKey,i);t._firestoreClient=new l4(t._authCredentials,t._appCheckCredentials,t._queue,s),!((n=i.localCache)===null||n===void 0)&&n._offlineComponentProvider&&(!((r=i.localCache)===null||r===void 0)&&r._onlineComponentProvider)&&(t._firestoreClient._uninitializedComponentsProvider={_offlineKind:i.localCache.kind,_offline:i.localCache._offlineComponentProvider,_online:i.localCache._onlineComponentProvider})}/**
 * @license
 * Copyright 2020 Google LLC
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */class ma{constructor(e){this._byteString=e}static fromBase64String(e){try{return new ma(Qn.fromBase64String(e))}catch(n){throw new G(R.INVALID_ARGUMENT,"Failed to construct data from Base64 string: "+n)}}static fromUint8Array(e){return new ma(Qn.fromUint8Array(e))}toBase64(){return this._byteString.toBase64()}toUint8Array(){return this._byteString.toUint8Array()}toString(){return"Bytes(base64: "+this.toBase64()+")"}isEqual(e){return this._byteString.isEqual(e._byteString)}}/**
 * @license
 * Copyright 2020 Google LLC
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */class PT{constructor(...e){for(let n=0;n<e.length;++n)if(e[n].length===0)throw new G(R.INVALID_ARGUMENT,"Invalid field name at argument $(i + 1). Field names must not be empty.");this._internalPath=new mt(e)}isEqual(e){return this._internalPath.isEqual(e._internalPath)}}/**
 * @license
 * Copyright 2020 Google LLC
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */class RT{constructor(e){this._methodName=e}}/**
 * @license
 * Copyright 2017 Google LLC
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */class CT{constructor(e,n){if(!isFinite(e)||e<-90||e>90)throw new G(R.INVALID_ARGUMENT,"Latitude must be a number between -90 and 90, but was: "+e);if(!isFinite(n)||n<-180||n>180)throw new G(R.INVALID_ARGUMENT,"Longitude must be a number between -180 and 180, but was: "+n);this._lat=e,this._long=n}get latitude(){return this._lat}get longitude(){return this._long}isEqual(e){return this._lat===e._lat&&this._long===e._long}toJSON(){return{latitude:this._lat,longitude:this._long}}_compareTo(e){return de(this._lat,e._lat)||de(this._long,e._long)}}/**
 * @license
 * Copyright 2017 Google LLC
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */const w4=/^__.*__$/;class E4{constructor(e,n,r){this.data=e,this.fieldMask=n,this.fieldTransforms=r}toMutation(e,n){return this.fieldMask!==null?new Ni(e,this.data,this.fieldMask,n,this.fieldTransforms):new Wa(e,this.data,n,this.fieldTransforms)}}function NT(t){switch(t){case 0:case 2:case 1:return!0;case 3:case 4:return!1;default:throw ne()}}class Fm{constructor(e,n,r,i,s,o){this.settings=e,this.databaseId=n,this.serializer=r,this.ignoreUndefinedProperties=i,s===void 0&&this.au(),this.fieldTransforms=s||[],this.fieldMask=o||[]}get path(){return this.settings.path}get uu(){return this.settings.uu}cu(e){return new Fm(Object.assign(Object.assign({},this.settings),e),this.databaseId,this.serializer,this.ignoreUndefinedProperties,this.fieldTransforms,this.fieldMask)}lu(e){var n;const r=(n=this.path)===null||n===void 0?void 0:n.child(e),i=this.cu({path:r,hu:!1});return i.Pu(e),i}Iu(e){var n;const r=(n=this.path)===null||n===void 0?void 0:n.child(e),i=this.cu({path:r,hu:!1});return i.au(),i}Tu(e){return this.cu({path:void 0,hu:!0})}Eu(e){return Xu(e,this.settings.methodName,this.settings.du||!1,this.path,this.settings.Au)}contains(e){return this.fieldMask.find(n=>e.isPrefixOf(n))!==void 0||this.fieldTransforms.find(n=>e.isPrefixOf(n.field))!==void 0}au(){if(this.path)for(let e=0;e<this.path.length;e++)this.Pu(this.path.get(e))}Pu(e){if(e.length===0)throw this.Eu("Document fields must not be empty");if(NT(this.uu)&&w4.test(e))throw this.Eu('Document fields cannot begin and end with "__"')}}class T4{constructor(e,n,r){this.databaseId=e,this.ignoreUndefinedProperties=n,this.serializer=r||Hc(e)}Ru(e,n,r,i=!1){return new Fm({uu:e,methodName:n,Au:r,path:mt.emptyPath(),hu:!1,du:i},this.databaseId,this.serializer,this.ignoreUndefinedProperties)}}function I4(t){const e=t._freezeSettings(),n=Hc(t._databaseId);return new T4(t._databaseId,!!e.ignoreUndefinedProperties,n)}function S4(t,e,n,r,i,s={}){const o=t.Ru(s.merge||s.mergeFields?2:0,e,n,i);MT("Data must be an object, but it was:",o,r);const a=OT(r,o);let l,u;if(s.merge)l=new on(o.fieldMask),u=o.fieldTransforms;else if(s.mergeFields){const c=[];for(const h of s.mergeFields){const d=k4(e,h,n);if(!o.contains(d))throw new G(R.INVALID_ARGUMENT,`Field '${d}' is specified in your field mask but missing from your input data.`);P4(c,d)||c.push(d)}l=new on(c),u=o.fieldTransforms.filter(h=>l.covers(h.field))}else l=null,u=o.fieldTransforms;return new E4(new sn(a),l,u)}function bT(t,e){if(DT(t=kn(t)))return MT("Unsupported field value:",e,t),OT(t,e);if(t instanceof RT)return function(r,i){if(!NT(i.uu))throw i.Eu(`${r._methodName}() can only be used with update() and set()`);if(!i.path)throw i.Eu(`${r._methodName}() is not currently supported inside arrays`);const s=r._toFieldTransform(i);s&&i.fieldTransforms.push(s)}(t,e),null;if(t===void 0&&e.ignoreUndefinedProperties)return null;if(e.path&&e.fieldMask.push(e.path),t instanceof Array){if(e.settings.hu&&e.uu!==4)throw e.Eu("Nested arrays are not supported");return function(r,i){const s=[];let o=0;for(const a of r){let l=bT(a,i.Tu(o));l==null&&(l={nullValue:"NULL_VALUE"}),s.push(l),o++}return{arrayValue:{values:s}}}(t,e)}return function(r,i){if((r=kn(r))===null)return{nullValue:"NULL_VALUE"};if(typeof r=="number")return WN(i.serializer,r);if(typeof r=="boolean")return{booleanValue:r};if(typeof r=="string")return{stringValue:r};if(r instanceof Date){const s=ze.fromDate(r);return{timestampValue:xd(i.serializer,s)}}if(r instanceof ze){const s=new ze(r.seconds,1e3*Math.floor(r.nanoseconds/1e3));return{timestampValue:xd(i.serializer,s)}}if(r instanceof CT)return{geoPointValue:{latitude:r.latitude,longitude:r.longitude}};if(r instanceof ma)return{bytesValue:ib(i.serializer,r._byteString)};if(r instanceof $n){const s=i.databaseId,o=r.firestore._databaseId;if(!o.isEqual(s))throw i.Eu(`Document reference is for database ${o.projectId}/${o.database} but should be for database ${s.projectId}/${s.database}`);return{referenceValue:pT(r.firestore._databaseId||i.databaseId,r._key.path)}}throw i.Eu(`Unsupported field value: ${Lm(r)}`)}(t,e)}function OT(t,e){const n={};return WE(t)?e.path&&e.path.length>0&&e.fieldMask.push(e.path):Ha(t,(r,i)=>{const s=bT(i,e.lu(r));s!=null&&(n[r]=s)}),{mapValue:{fields:n}}}function DT(t){return!(typeof t!="object"||t===null||t instanceof Array||t instanceof Date||t instanceof ze||t instanceof CT||t instanceof ma||t instanceof $n||t instanceof RT)}function MT(t,e,n){if(!DT(n)||!function(i){return typeof i=="object"&&i!==null&&(Object.getPrototypeOf(i)===Object.prototype||Object.getPrototypeOf(i)===null)}(n)){const r=Lm(n);throw r==="an object"?e.Eu(t+" a custom object"):e.Eu(t+" "+r)}}function k4(t,e,n){if((e=kn(e))instanceof PT)return e._internalPath;if(typeof e=="string")return x4(t,e);throw Xu("Field path arguments must be of type string or ",t,!1,void 0,n)}const A4=new RegExp("[~\\*/\\[\\]]");function x4(t,e,n){if(e.search(A4)>=0)throw Xu(`Invalid field path (${e}). Paths must not contain '~', '*', '/', '[', or ']'`,t,!1,void 0,n);try{return new PT(...e.split("."))._internalPath}catch{throw Xu(`Invalid field path (${e}). Paths must not be empty, begin with '.', end with '.', or contain '..'`,t,!1,void 0,n)}}function Xu(t,e,n,r,i){const s=r&&!r.isEmpty(),o=i!==void 0;let a=`Function ${e}() called with invalid data`;n&&(a+=" (via `toFirestore()`)"),a+=". ";let l="";return(s||o)&&(l+=" (found",s&&(l+=` in field ${r}`),o&&(l+=` in document ${i}`),l+=")"),new G(R.INVALID_ARGUMENT,a+t+l)}function P4(t,e){return t.some(n=>n.isEqual(e))}/**
 * @license
 * Copyright 2020 Google LLC
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */function R4(t,e,n){let r;return r=t?n&&(n.merge||n.mergeFields)?t.toFirestore(e,n):t.toFirestore(e):e,r}function C4(t,e,n){t=Rd(t,$n);const r=Rd(t.firestore,AT),i=R4(t.converter,e,n);return N4(r,[S4(I4(r),"setDoc",t._key,i,t.converter!==null,n).toMutation(t._key,Un.none())])}function N4(t,e){return function(r,i){const s=new fi;return r.asyncQueue.enqueueAndForget(async()=>n4(await h4(r),i,s)),s.promise}(_4(t),e)}(function(e,n=!0){(function(i){$s=i})(Vs),ks(new yi("firestore",(r,{instanceIdentifier:i,options:s})=>{const o=r.getProvider("app").getImmediate(),a=new AT(new oN(r.getProvider("auth-internal")),new cN(r.getProvider("app-check-internal")),function(u,c){if(!Object.prototype.hasOwnProperty.apply(u.options,["projectId"]))throw new G(R.INVALID_ARGUMENT,'"projectId" not provided in firebase.initializeApp.');return new Ku(u.options.projectId,c)}(o,i),o);return s=Object.assign({useFetchStreams:n},s),a._setSettings(s),a},"PUBLIC").setMultipleInstances(!0)),br(Ty,"4.1.0",e),br(Ty,"4.1.0","esm2017")})();const b4={apiKey:"AIzaSyAXErqrpT4P1fJ6jNa1a4hJbYigXV2uID4",authDomain:"chatty-app-71ff5.firebaseapp.com",databaseURL:"https://chatty-app.firebaseio.com",projectId:"chatty-app-71ff5",storageBucket:"chatty-app-71ff5.appspot.com",messagingSenderId:"828176173941",appId:"1:828176173941:web:0d7710ae12515af49e613c"},LT=tw(b4),O4=qR(LT),D4=y4(LT),M4=()=>{const[t,e]=N.useState({email:"",name:"",password:"",error:null,loading:!1}),n=B_(),{name:r,email:i,password:s,error:o,loading:a}=t,l=c=>{e({...t,[c.target.name]:c.target.value})},u=async c=>{c.preventDefault(),e({...t,error:null,loading:!0}),(!r||!i||!s)&&e({...t,error:"All fields are required"});try{const h=await DP(O4,i,s);await C4(g4(D4,"users",h.user.uid),{uid:h.user.uid,name:r,email:i,createdAt:ze.fromDate(new Date),isOnline:!0}),e({name:"",email:"",password:"",error:null,loading:!1}),n("/")}catch(h){e({...t,error:h.message,loading:!1})}};return f.jsxs("div",{className:"registration-container",children:[f.jsx("div",{className:"logo",children:"Chatvia logo"}),f.jsxs("div",{className:"register-header ",children:[f.jsx("h1",{children:"Sign up"}),f.jsx("h3",{children:"Get your Chatvia account now"})]}),f.jsxs("form",{id:"registration-form","aria-label":"Registration Form",onSubmit:u,children:[f.jsxs("div",{className:"form-group",children:[f.jsx("label",{htmlFor:"email",children:"Email"}),f.jsx("input",{type:"text",id:"email",name:"email",value:i,onChange:l,"aria-required":"true","aria-label":"Enter your desired email address"})]}),f.jsxs("div",{className:"form-group",children:[f.jsx("label",{htmlFor:"username",children:"Username"}),f.jsx("input",{type:"text",id:"username",name:"name",value:r,onChange:l,"aria-required":"true","aria-label":"Enter your desired username"})]}),f.jsxs("div",{className:"form-group",children:[f.jsx("label",{htmlFor:"password",children:"Password"}),f.jsx("input",{type:"password",id:"password",name:"password",value:s,onChange:l,"aria-required":"true","aria-label":"Enter your desired password"})]}),o?f.jsx("p",{className:"error",children:o}):null,f.jsx("div",{className:"btn-container",children:f.jsx("button",{className:"btn",disabled:a,children:"Sign up"})}),f.jsxs("div",{className:"terms",children:[f.jsx("p",{children:"By registering you agree to the Chatvia"}),f.jsx("p",{children:"Terms of Use"})]})]}),f.jsx("div",{children:"Already have an account? Signin"}),f.jsx("p",{children:"Copyright blah blah"})]})},L4=()=>f.jsxs(f.Fragment,{children:[f.jsxs("header",{children:[f.jsx("div",{className:"chat-via-logo",children:"ChatVia"}),f.jsx("h1",{children:"Sign in"}),f.jsx("h2",{children:"Sign in to continue to ChatVia"})]}),f.jsxs("main",{children:[f.jsx("div",{className:"user",children:"Username"}),f.jsx("div",{className:"password",children:"Password"}),f.jsx("button",{children:"Sign in"})]}),f.jsxs("footer",{children:[f.jsx("h3",{children:"Don't have an account? Sign up now!"}),f.jsx("h4",{children:"Copyright blah blah blah"})]})]}),j4=()=>f.jsx("h2",{children:"Home Page"}),V4=()=>f.jsx("div",{children:f.jsx("h1",{children:"About time..."})}),At="/assets/avatar-2-8d5771fe.jpg",F4=()=>f.jsxs("div",{className:"chats",children:[f.jsxs("div",{className:"user-chat",children:[f.jsx("img",{src:At,alt:"Pic of person in chat list"}),f.jsxs("div",{className:"user-chat-info",children:[f.jsxs("div",{className:"list-head",children:[f.jsx("h4",{children:"Patrick Hendricks"}),f.jsx("p",{className:"time",children:"11:47"})]}),f.jsx("div",{className:"message-p",children:f.jsx("p",{children:"Hello you bum!"})})]})]}),f.jsxs("div",{className:"user-chat",children:[f.jsx("img",{src:At,alt:"Pic of person in chat list"}),f.jsxs("div",{className:"user-chat-info",children:[f.jsxs("div",{className:"list-head",children:[f.jsx("h4",{children:"Patrick Hendricks"}),f.jsx("p",{className:"time",children:"11:47"})]}),f.jsx("div",{className:"message-p",children:f.jsx("p",{children:"Hello you bum!"})})]})]}),f.jsxs("div",{className:"user-chat",children:[f.jsx("img",{src:At,alt:"Pic of person in chat list"}),f.jsxs("div",{className:"user-chat-info",children:[f.jsxs("div",{className:"list-head",children:[f.jsx("h4",{children:"Patrick Hendricks"}),f.jsx("p",{className:"time",children:"11:47"})]}),f.jsx("div",{className:"message-p",children:f.jsx("p",{children:"Hello you bum!"})})]})]}),f.jsxs("div",{className:"user-chat",children:[f.jsx("img",{src:At,alt:"Pic of person in chat list"}),f.jsxs("div",{className:"user-chat-info",children:[f.jsxs("div",{className:"list-head",children:[f.jsx("h4",{children:"Patrick Hendricks"}),f.jsx("p",{className:"time",children:"11:47"})]}),f.jsx("div",{className:"message-p",children:f.jsx("p",{children:"Hello you bum!"})})]})]}),f.jsxs("div",{className:"user-chat",children:[f.jsx("img",{src:At,alt:"Pic of person in chat list"}),f.jsxs("div",{className:"user-chat-info",children:[f.jsxs("div",{className:"list-head",children:[f.jsx("h4",{children:"Patrick Hendricks"}),f.jsx("p",{className:"time",children:"11:47"})]}),f.jsx("div",{className:"message-p",children:f.jsx("p",{children:"Hello you bum!"})})]})]}),f.jsxs("div",{className:"user-chat",children:[f.jsx("img",{src:At,alt:"Pic of person in chat list"}),f.jsxs("div",{className:"user-chat-info",children:[f.jsxs("div",{className:"list-head",children:[f.jsx("h4",{children:"Patrick Hendricks"}),f.jsx("p",{className:"time",children:"11:47"})]}),f.jsx("div",{className:"message-p",children:f.jsx("p",{children:"Hello you bum!"})})]})]}),f.jsxs("div",{className:"user-chat",children:[f.jsx("img",{src:At,alt:"Pic of person in chat list"}),f.jsxs("div",{className:"user-chat-info",children:[f.jsxs("div",{className:"list-head",children:[f.jsx("h4",{children:"Patrick Hendricks"}),f.jsx("p",{className:"time",children:"11:47"})]}),f.jsx("div",{className:"message-p",children:f.jsx("p",{children:"Hello you bum!"})})]})]}),f.jsxs("div",{className:"user-chat",children:[f.jsx("img",{src:At,alt:"Pic of person in chat list"}),f.jsxs("div",{className:"user-chat-info",children:[f.jsxs("div",{className:"list-head",children:[f.jsx("h4",{children:"Patrick Hendricks"}),f.jsx("p",{className:"time",children:"11:47"})]}),f.jsx("div",{className:"message-p",children:f.jsx("p",{children:"Hello you bum!"})})]})]}),f.jsxs("div",{className:"user-chat",children:[f.jsx("img",{src:At,alt:"Pic of person in chat list"}),f.jsxs("div",{className:"user-chat-info",children:[f.jsxs("div",{className:"list-head",children:[f.jsx("h4",{children:"Patrick Hendricks"}),f.jsx("p",{className:"time",children:"11:47"})]}),f.jsx("div",{className:"message-p",children:f.jsx("p",{children:"Hello you bum!"})})]})]}),f.jsxs("div",{className:"user-chat",children:[f.jsx("img",{src:At,alt:"Pic of person in chat list"}),f.jsxs("div",{className:"user-chat-info",children:[f.jsxs("div",{className:"list-head",children:[f.jsx("h4",{children:"Patrick Hendricks"}),f.jsx("p",{className:"time",children:"11:47"})]}),f.jsx("div",{className:"message-p",children:f.jsx("p",{children:"Hello you bum!"})})]})]}),f.jsxs("div",{className:"user-chat",children:[f.jsx("img",{src:At,alt:"Pic of person in chat list"}),f.jsxs("div",{className:"user-chat-info",children:[f.jsxs("div",{className:"list-head",children:[f.jsx("h4",{children:"Patrick Hendricks"}),f.jsx("p",{className:"time",children:"11:47"})]}),f.jsx("div",{className:"message-p",children:f.jsx("p",{children:"Hello you bum!"})})]})]}),f.jsxs("div",{className:"user-chat",children:[f.jsx("img",{src:At,alt:"Pic of person in chat list"}),f.jsxs("div",{className:"user-chat-info",children:[f.jsxs("div",{className:"list-head",children:[f.jsx("h4",{children:"Patrick Hendricks"}),f.jsx("p",{className:"time",children:"11:47"})]}),f.jsx("div",{className:"message-p",children:f.jsx("p",{children:"Hello you bum!"})})]})]}),f.jsxs("div",{className:"user-chat",children:[f.jsx("img",{src:At,alt:"Pic of person in chat list"}),f.jsxs("div",{className:"user-chat-info",children:[f.jsxs("div",{className:"list-head",children:[f.jsx("h4",{children:"Patrick Hendricks"}),f.jsx("p",{className:"time",children:"11:47"})]}),f.jsx("div",{className:"message-p",children:f.jsx("p",{children:"Hello you bum!"})})]})]}),f.jsxs("div",{className:"user-chat",children:[f.jsx("img",{src:At,alt:"Pic of person in chat list"}),f.jsxs("div",{className:"user-chat-info",children:[f.jsxs("div",{className:"list-head",children:[f.jsx("h4",{children:"Patrick Hendricks"}),f.jsx("p",{className:"time",children:"11:47"})]}),f.jsx("div",{className:"message-p",children:f.jsx("p",{children:"Hello you bum!"})})]})]})]});function Ky(t,e){var n=Object.keys(t);if(Object.getOwnPropertySymbols){var r=Object.getOwnPropertySymbols(t);e&&(r=r.filter(function(i){return Object.getOwnPropertyDescriptor(t,i).enumerable})),n.push.apply(n,r)}return n}function M(t){for(var e=1;e<arguments.length;e++){var n=arguments[e]!=null?arguments[e]:{};e%2?Ky(Object(n),!0).forEach(function(r){Be(t,r,n[r])}):Object.getOwnPropertyDescriptors?Object.defineProperties(t,Object.getOwnPropertyDescriptors(n)):Ky(Object(n)).forEach(function(r){Object.defineProperty(t,r,Object.getOwnPropertyDescriptor(n,r))})}return t}function Ju(t){"@babel/helpers - typeof";return Ju=typeof Symbol=="function"&&typeof Symbol.iterator=="symbol"?function(e){return typeof e}:function(e){return e&&typeof Symbol=="function"&&e.constructor===Symbol&&e!==Symbol.prototype?"symbol":typeof e},Ju(t)}function U4(t,e){if(!(t instanceof e))throw new TypeError("Cannot call a class as a function")}function Gy(t,e){for(var n=0;n<e.length;n++){var r=e[n];r.enumerable=r.enumerable||!1,r.configurable=!0,"value"in r&&(r.writable=!0),Object.defineProperty(t,r.key,r)}}function $4(t,e,n){return e&&Gy(t.prototype,e),n&&Gy(t,n),Object.defineProperty(t,"prototype",{writable:!1}),t}function Be(t,e,n){return e in t?Object.defineProperty(t,e,{value:n,enumerable:!0,configurable:!0,writable:!0}):t[e]=n,t}function Um(t,e){return B4(t)||W4(t,e)||jT(t,e)||G4()}function qa(t){return z4(t)||H4(t)||jT(t)||K4()}function z4(t){if(Array.isArray(t))return Cd(t)}function B4(t){if(Array.isArray(t))return t}function H4(t){if(typeof Symbol<"u"&&t[Symbol.iterator]!=null||t["@@iterator"]!=null)return Array.from(t)}function W4(t,e){var n=t==null?null:typeof Symbol<"u"&&t[Symbol.iterator]||t["@@iterator"];if(n!=null){var r=[],i=!0,s=!1,o,a;try{for(n=n.call(t);!(i=(o=n.next()).done)&&(r.push(o.value),!(e&&r.length===e));i=!0);}catch(l){s=!0,a=l}finally{try{!i&&n.return!=null&&n.return()}finally{if(s)throw a}}return r}}function jT(t,e){if(t){if(typeof t=="string")return Cd(t,e);var n=Object.prototype.toString.call(t).slice(8,-1);if(n==="Object"&&t.constructor&&(n=t.constructor.name),n==="Map"||n==="Set")return Array.from(t);if(n==="Arguments"||/^(?:Ui|I)nt(?:8|16|32)(?:Clamped)?Array$/.test(n))return Cd(t,e)}}function Cd(t,e){(e==null||e>t.length)&&(e=t.length);for(var n=0,r=new Array(e);n<e;n++)r[n]=t[n];return r}function K4(){throw new TypeError(`Invalid attempt to spread non-iterable instance.
In order to be iterable, non-array objects must have a [Symbol.iterator]() method.`)}function G4(){throw new TypeError(`Invalid attempt to destructure non-iterable instance.
In order to be iterable, non-array objects must have a [Symbol.iterator]() method.`)}var qy=function(){},$m={},VT={},FT=null,UT={mark:qy,measure:qy};try{typeof window<"u"&&($m=window),typeof document<"u"&&(VT=document),typeof MutationObserver<"u"&&(FT=MutationObserver),typeof performance<"u"&&(UT=performance)}catch{}var q4=$m.navigator||{},Qy=q4.userAgent,Yy=Qy===void 0?"":Qy,Vr=$m,_e=VT,Xy=FT,Pl=UT;Vr.document;var nr=!!_e.documentElement&&!!_e.head&&typeof _e.addEventListener=="function"&&typeof _e.createElement=="function",$T=~Yy.indexOf("MSIE")||~Yy.indexOf("Trident/"),Rl,Cl,Nl,bl,Ol,Yn="___FONT_AWESOME___",Nd=16,zT="fa",BT="svg-inline--fa",ki="data-fa-i2svg",bd="data-fa-pseudo-element",Q4="data-fa-pseudo-element-pending",zm="data-prefix",Bm="data-icon",Jy="fontawesome-i2svg",Y4="async",X4=["HTML","HEAD","STYLE","SCRIPT"],HT=function(){try{return!0}catch{return!1}}(),ge="classic",Re="sharp",Hm=[ge,Re];function Qa(t){return new Proxy(t,{get:function(n,r){return r in n?n[r]:n[ge]}})}var ga=Qa((Rl={},Be(Rl,ge,{fa:"solid",fas:"solid","fa-solid":"solid",far:"regular","fa-regular":"regular",fal:"light","fa-light":"light",fat:"thin","fa-thin":"thin",fad:"duotone","fa-duotone":"duotone",fab:"brands","fa-brands":"brands",fak:"kit","fa-kit":"kit"}),Be(Rl,Re,{fa:"solid",fass:"solid","fa-solid":"solid",fasr:"regular","fa-regular":"regular",fasl:"light","fa-light":"light"}),Rl)),va=Qa((Cl={},Be(Cl,ge,{solid:"fas",regular:"far",light:"fal",thin:"fat",duotone:"fad",brands:"fab",kit:"fak"}),Be(Cl,Re,{solid:"fass",regular:"fasr",light:"fasl"}),Cl)),ya=Qa((Nl={},Be(Nl,ge,{fab:"fa-brands",fad:"fa-duotone",fak:"fa-kit",fal:"fa-light",far:"fa-regular",fas:"fa-solid",fat:"fa-thin"}),Be(Nl,Re,{fass:"fa-solid",fasr:"fa-regular",fasl:"fa-light"}),Nl)),J4=Qa((bl={},Be(bl,ge,{"fa-brands":"fab","fa-duotone":"fad","fa-kit":"fak","fa-light":"fal","fa-regular":"far","fa-solid":"fas","fa-thin":"fat"}),Be(bl,Re,{"fa-solid":"fass","fa-regular":"fasr","fa-light":"fasl"}),bl)),Z4=/fa(s|r|l|t|d|b|k|ss|sr|sl)?[\-\ ]/,WT="fa-layers-text",eO=/Font ?Awesome ?([56 ]*)(Solid|Regular|Light|Thin|Duotone|Brands|Free|Pro|Sharp|Kit)?.*/i,tO=Qa((Ol={},Be(Ol,ge,{900:"fas",400:"far",normal:"far",300:"fal",100:"fat"}),Be(Ol,Re,{900:"fass",400:"fasr",300:"fasl"}),Ol)),KT=[1,2,3,4,5,6,7,8,9,10],nO=KT.concat([11,12,13,14,15,16,17,18,19,20]),rO=["class","data-prefix","data-icon","data-fa-transform","data-fa-mask"],si={GROUP:"duotone-group",SWAP_OPACITY:"swap-opacity",PRIMARY:"primary",SECONDARY:"secondary"},_a=new Set;Object.keys(va[ge]).map(_a.add.bind(_a));Object.keys(va[Re]).map(_a.add.bind(_a));var iO=[].concat(Hm,qa(_a),["2xs","xs","sm","lg","xl","2xl","beat","border","fade","beat-fade","bounce","flip-both","flip-horizontal","flip-vertical","flip","fw","inverse","layers-counter","layers-text","layers","li","pull-left","pull-right","pulse","rotate-180","rotate-270","rotate-90","rotate-by","shake","spin-pulse","spin-reverse","spin","stack-1x","stack-2x","stack","ul",si.GROUP,si.SWAP_OPACITY,si.PRIMARY,si.SECONDARY]).concat(KT.map(function(t){return"".concat(t,"x")})).concat(nO.map(function(t){return"w-".concat(t)})),No=Vr.FontAwesomeConfig||{};function sO(t){var e=_e.querySelector("script["+t+"]");if(e)return e.getAttribute(t)}function oO(t){return t===""?!0:t==="false"?!1:t==="true"?!0:t}if(_e&&typeof _e.querySelector=="function"){var aO=[["data-family-prefix","familyPrefix"],["data-css-prefix","cssPrefix"],["data-family-default","familyDefault"],["data-style-default","styleDefault"],["data-replacement-class","replacementClass"],["data-auto-replace-svg","autoReplaceSvg"],["data-auto-add-css","autoAddCss"],["data-auto-a11y","autoA11y"],["data-search-pseudo-elements","searchPseudoElements"],["data-observe-mutations","observeMutations"],["data-mutate-approach","mutateApproach"],["data-keep-original-source","keepOriginalSource"],["data-measure-performance","measurePerformance"],["data-show-missing-icons","showMissingIcons"]];aO.forEach(function(t){var e=Um(t,2),n=e[0],r=e[1],i=oO(sO(n));i!=null&&(No[r]=i)})}var GT={styleDefault:"solid",familyDefault:"classic",cssPrefix:zT,replacementClass:BT,autoReplaceSvg:!0,autoAddCss:!0,autoA11y:!0,searchPseudoElements:!1,observeMutations:!0,mutateApproach:"async",keepOriginalSource:!0,measurePerformance:!1,showMissingIcons:!0};No.familyPrefix&&(No.cssPrefix=No.familyPrefix);var Os=M(M({},GT),No);Os.autoReplaceSvg||(Os.observeMutations=!1);var V={};Object.keys(GT).forEach(function(t){Object.defineProperty(V,t,{enumerable:!0,set:function(n){Os[t]=n,bo.forEach(function(r){return r(V)})},get:function(){return Os[t]}})});Object.defineProperty(V,"familyPrefix",{enumerable:!0,set:function(e){Os.cssPrefix=e,bo.forEach(function(n){return n(V)})},get:function(){return Os.cssPrefix}});Vr.FontAwesomeConfig=V;var bo=[];function lO(t){return bo.push(t),function(){bo.splice(bo.indexOf(t),1)}}var cr=Nd,wn={size:16,x:0,y:0,rotate:0,flipX:!1,flipY:!1};function uO(t){if(!(!t||!nr)){var e=_e.createElement("style");e.setAttribute("type","text/css"),e.innerHTML=t;for(var n=_e.head.childNodes,r=null,i=n.length-1;i>-1;i--){var s=n[i],o=(s.tagName||"").toUpperCase();["STYLE","LINK"].indexOf(o)>-1&&(r=s)}return _e.head.insertBefore(e,r),t}}var cO="0123456789abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ";function wa(){for(var t=12,e="";t-- >0;)e+=cO[Math.random()*62|0];return e}function Bs(t){for(var e=[],n=(t||[]).length>>>0;n--;)e[n]=t[n];return e}function Wm(t){return t.classList?Bs(t.classList):(t.getAttribute("class")||"").split(" ").filter(function(e){return e})}function qT(t){return"".concat(t).replace(/&/g,"&amp;").replace(/"/g,"&quot;").replace(/'/g,"&#39;").replace(/</g,"&lt;").replace(/>/g,"&gt;")}function fO(t){return Object.keys(t||{}).reduce(function(e,n){return e+"".concat(n,'="').concat(qT(t[n]),'" ')},"").trim()}function qc(t){return Object.keys(t||{}).reduce(function(e,n){return e+"".concat(n,": ").concat(t[n].trim(),";")},"")}function Km(t){return t.size!==wn.size||t.x!==wn.x||t.y!==wn.y||t.rotate!==wn.rotate||t.flipX||t.flipY}function hO(t){var e=t.transform,n=t.containerWidth,r=t.iconWidth,i={transform:"translate(".concat(n/2," 256)")},s="translate(".concat(e.x*32,", ").concat(e.y*32,") "),o="scale(".concat(e.size/16*(e.flipX?-1:1),", ").concat(e.size/16*(e.flipY?-1:1),") "),a="rotate(".concat(e.rotate," 0 0)"),l={transform:"".concat(s," ").concat(o," ").concat(a)},u={transform:"translate(".concat(r/2*-1," -256)")};return{outer:i,inner:l,path:u}}function dO(t){var e=t.transform,n=t.width,r=n===void 0?Nd:n,i=t.height,s=i===void 0?Nd:i,o=t.startCentered,a=o===void 0?!1:o,l="";return a&&$T?l+="translate(".concat(e.x/cr-r/2,"em, ").concat(e.y/cr-s/2,"em) "):a?l+="translate(calc(-50% + ".concat(e.x/cr,"em), calc(-50% + ").concat(e.y/cr,"em)) "):l+="translate(".concat(e.x/cr,"em, ").concat(e.y/cr,"em) "),l+="scale(".concat(e.size/cr*(e.flipX?-1:1),", ").concat(e.size/cr*(e.flipY?-1:1),") "),l+="rotate(".concat(e.rotate,"deg) "),l}var pO=`:root, :host {
  --fa-font-solid: normal 900 1em/1 "Font Awesome 6 Solid";
  --fa-font-regular: normal 400 1em/1 "Font Awesome 6 Regular";
  --fa-font-light: normal 300 1em/1 "Font Awesome 6 Light";
  --fa-font-thin: normal 100 1em/1 "Font Awesome 6 Thin";
  --fa-font-duotone: normal 900 1em/1 "Font Awesome 6 Duotone";
  --fa-font-sharp-solid: normal 900 1em/1 "Font Awesome 6 Sharp";
  --fa-font-sharp-regular: normal 400 1em/1 "Font Awesome 6 Sharp";
  --fa-font-sharp-light: normal 300 1em/1 "Font Awesome 6 Sharp";
  --fa-font-brands: normal 400 1em/1 "Font Awesome 6 Brands";
}

svg:not(:root).svg-inline--fa, svg:not(:host).svg-inline--fa {
  overflow: visible;
  box-sizing: content-box;
}

.svg-inline--fa {
  display: var(--fa-display, inline-block);
  height: 1em;
  overflow: visible;
  vertical-align: -0.125em;
}
.svg-inline--fa.fa-2xs {
  vertical-align: 0.1em;
}
.svg-inline--fa.fa-xs {
  vertical-align: 0em;
}
.svg-inline--fa.fa-sm {
  vertical-align: -0.0714285705em;
}
.svg-inline--fa.fa-lg {
  vertical-align: -0.2em;
}
.svg-inline--fa.fa-xl {
  vertical-align: -0.25em;
}
.svg-inline--fa.fa-2xl {
  vertical-align: -0.3125em;
}
.svg-inline--fa.fa-pull-left {
  margin-right: var(--fa-pull-margin, 0.3em);
  width: auto;
}
.svg-inline--fa.fa-pull-right {
  margin-left: var(--fa-pull-margin, 0.3em);
  width: auto;
}
.svg-inline--fa.fa-li {
  width: var(--fa-li-width, 2em);
  top: 0.25em;
}
.svg-inline--fa.fa-fw {
  width: var(--fa-fw-width, 1.25em);
}

.fa-layers svg.svg-inline--fa {
  bottom: 0;
  left: 0;
  margin: auto;
  position: absolute;
  right: 0;
  top: 0;
}

.fa-layers-counter, .fa-layers-text {
  display: inline-block;
  position: absolute;
  text-align: center;
}

.fa-layers {
  display: inline-block;
  height: 1em;
  position: relative;
  text-align: center;
  vertical-align: -0.125em;
  width: 1em;
}
.fa-layers svg.svg-inline--fa {
  -webkit-transform-origin: center center;
          transform-origin: center center;
}

.fa-layers-text {
  left: 50%;
  top: 50%;
  -webkit-transform: translate(-50%, -50%);
          transform: translate(-50%, -50%);
  -webkit-transform-origin: center center;
          transform-origin: center center;
}

.fa-layers-counter {
  background-color: var(--fa-counter-background-color, #ff253a);
  border-radius: var(--fa-counter-border-radius, 1em);
  box-sizing: border-box;
  color: var(--fa-inverse, #fff);
  line-height: var(--fa-counter-line-height, 1);
  max-width: var(--fa-counter-max-width, 5em);
  min-width: var(--fa-counter-min-width, 1.5em);
  overflow: hidden;
  padding: var(--fa-counter-padding, 0.25em 0.5em);
  right: var(--fa-right, 0);
  text-overflow: ellipsis;
  top: var(--fa-top, 0);
  -webkit-transform: scale(var(--fa-counter-scale, 0.25));
          transform: scale(var(--fa-counter-scale, 0.25));
  -webkit-transform-origin: top right;
          transform-origin: top right;
}

.fa-layers-bottom-right {
  bottom: var(--fa-bottom, 0);
  right: var(--fa-right, 0);
  top: auto;
  -webkit-transform: scale(var(--fa-layers-scale, 0.25));
          transform: scale(var(--fa-layers-scale, 0.25));
  -webkit-transform-origin: bottom right;
          transform-origin: bottom right;
}

.fa-layers-bottom-left {
  bottom: var(--fa-bottom, 0);
  left: var(--fa-left, 0);
  right: auto;
  top: auto;
  -webkit-transform: scale(var(--fa-layers-scale, 0.25));
          transform: scale(var(--fa-layers-scale, 0.25));
  -webkit-transform-origin: bottom left;
          transform-origin: bottom left;
}

.fa-layers-top-right {
  top: var(--fa-top, 0);
  right: var(--fa-right, 0);
  -webkit-transform: scale(var(--fa-layers-scale, 0.25));
          transform: scale(var(--fa-layers-scale, 0.25));
  -webkit-transform-origin: top right;
          transform-origin: top right;
}

.fa-layers-top-left {
  left: var(--fa-left, 0);
  right: auto;
  top: var(--fa-top, 0);
  -webkit-transform: scale(var(--fa-layers-scale, 0.25));
          transform: scale(var(--fa-layers-scale, 0.25));
  -webkit-transform-origin: top left;
          transform-origin: top left;
}

.fa-1x {
  font-size: 1em;
}

.fa-2x {
  font-size: 2em;
}

.fa-3x {
  font-size: 3em;
}

.fa-4x {
  font-size: 4em;
}

.fa-5x {
  font-size: 5em;
}

.fa-6x {
  font-size: 6em;
}

.fa-7x {
  font-size: 7em;
}

.fa-8x {
  font-size: 8em;
}

.fa-9x {
  font-size: 9em;
}

.fa-10x {
  font-size: 10em;
}

.fa-2xs {
  font-size: 0.625em;
  line-height: 0.1em;
  vertical-align: 0.225em;
}

.fa-xs {
  font-size: 0.75em;
  line-height: 0.0833333337em;
  vertical-align: 0.125em;
}

.fa-sm {
  font-size: 0.875em;
  line-height: 0.0714285718em;
  vertical-align: 0.0535714295em;
}

.fa-lg {
  font-size: 1.25em;
  line-height: 0.05em;
  vertical-align: -0.075em;
}

.fa-xl {
  font-size: 1.5em;
  line-height: 0.0416666682em;
  vertical-align: -0.125em;
}

.fa-2xl {
  font-size: 2em;
  line-height: 0.03125em;
  vertical-align: -0.1875em;
}

.fa-fw {
  text-align: center;
  width: 1.25em;
}

.fa-ul {
  list-style-type: none;
  margin-left: var(--fa-li-margin, 2.5em);
  padding-left: 0;
}
.fa-ul > li {
  position: relative;
}

.fa-li {
  left: calc(var(--fa-li-width, 2em) * -1);
  position: absolute;
  text-align: center;
  width: var(--fa-li-width, 2em);
  line-height: inherit;
}

.fa-border {
  border-color: var(--fa-border-color, #eee);
  border-radius: var(--fa-border-radius, 0.1em);
  border-style: var(--fa-border-style, solid);
  border-width: var(--fa-border-width, 0.08em);
  padding: var(--fa-border-padding, 0.2em 0.25em 0.15em);
}

.fa-pull-left {
  float: left;
  margin-right: var(--fa-pull-margin, 0.3em);
}

.fa-pull-right {
  float: right;
  margin-left: var(--fa-pull-margin, 0.3em);
}

.fa-beat {
  -webkit-animation-name: fa-beat;
          animation-name: fa-beat;
  -webkit-animation-delay: var(--fa-animation-delay, 0s);
          animation-delay: var(--fa-animation-delay, 0s);
  -webkit-animation-direction: var(--fa-animation-direction, normal);
          animation-direction: var(--fa-animation-direction, normal);
  -webkit-animation-duration: var(--fa-animation-duration, 1s);
          animation-duration: var(--fa-animation-duration, 1s);
  -webkit-animation-iteration-count: var(--fa-animation-iteration-count, infinite);
          animation-iteration-count: var(--fa-animation-iteration-count, infinite);
  -webkit-animation-timing-function: var(--fa-animation-timing, ease-in-out);
          animation-timing-function: var(--fa-animation-timing, ease-in-out);
}

.fa-bounce {
  -webkit-animation-name: fa-bounce;
          animation-name: fa-bounce;
  -webkit-animation-delay: var(--fa-animation-delay, 0s);
          animation-delay: var(--fa-animation-delay, 0s);
  -webkit-animation-direction: var(--fa-animation-direction, normal);
          animation-direction: var(--fa-animation-direction, normal);
  -webkit-animation-duration: var(--fa-animation-duration, 1s);
          animation-duration: var(--fa-animation-duration, 1s);
  -webkit-animation-iteration-count: var(--fa-animation-iteration-count, infinite);
          animation-iteration-count: var(--fa-animation-iteration-count, infinite);
  -webkit-animation-timing-function: var(--fa-animation-timing, cubic-bezier(0.28, 0.84, 0.42, 1));
          animation-timing-function: var(--fa-animation-timing, cubic-bezier(0.28, 0.84, 0.42, 1));
}

.fa-fade {
  -webkit-animation-name: fa-fade;
          animation-name: fa-fade;
  -webkit-animation-delay: var(--fa-animation-delay, 0s);
          animation-delay: var(--fa-animation-delay, 0s);
  -webkit-animation-direction: var(--fa-animation-direction, normal);
          animation-direction: var(--fa-animation-direction, normal);
  -webkit-animation-duration: var(--fa-animation-duration, 1s);
          animation-duration: var(--fa-animation-duration, 1s);
  -webkit-animation-iteration-count: var(--fa-animation-iteration-count, infinite);
          animation-iteration-count: var(--fa-animation-iteration-count, infinite);
  -webkit-animation-timing-function: var(--fa-animation-timing, cubic-bezier(0.4, 0, 0.6, 1));
          animation-timing-function: var(--fa-animation-timing, cubic-bezier(0.4, 0, 0.6, 1));
}

.fa-beat-fade {
  -webkit-animation-name: fa-beat-fade;
          animation-name: fa-beat-fade;
  -webkit-animation-delay: var(--fa-animation-delay, 0s);
          animation-delay: var(--fa-animation-delay, 0s);
  -webkit-animation-direction: var(--fa-animation-direction, normal);
          animation-direction: var(--fa-animation-direction, normal);
  -webkit-animation-duration: var(--fa-animation-duration, 1s);
          animation-duration: var(--fa-animation-duration, 1s);
  -webkit-animation-iteration-count: var(--fa-animation-iteration-count, infinite);
          animation-iteration-count: var(--fa-animation-iteration-count, infinite);
  -webkit-animation-timing-function: var(--fa-animation-timing, cubic-bezier(0.4, 0, 0.6, 1));
          animation-timing-function: var(--fa-animation-timing, cubic-bezier(0.4, 0, 0.6, 1));
}

.fa-flip {
  -webkit-animation-name: fa-flip;
          animation-name: fa-flip;
  -webkit-animation-delay: var(--fa-animation-delay, 0s);
          animation-delay: var(--fa-animation-delay, 0s);
  -webkit-animation-direction: var(--fa-animation-direction, normal);
          animation-direction: var(--fa-animation-direction, normal);
  -webkit-animation-duration: var(--fa-animation-duration, 1s);
          animation-duration: var(--fa-animation-duration, 1s);
  -webkit-animation-iteration-count: var(--fa-animation-iteration-count, infinite);
          animation-iteration-count: var(--fa-animation-iteration-count, infinite);
  -webkit-animation-timing-function: var(--fa-animation-timing, ease-in-out);
          animation-timing-function: var(--fa-animation-timing, ease-in-out);
}

.fa-shake {
  -webkit-animation-name: fa-shake;
          animation-name: fa-shake;
  -webkit-animation-delay: var(--fa-animation-delay, 0s);
          animation-delay: var(--fa-animation-delay, 0s);
  -webkit-animation-direction: var(--fa-animation-direction, normal);
          animation-direction: var(--fa-animation-direction, normal);
  -webkit-animation-duration: var(--fa-animation-duration, 1s);
          animation-duration: var(--fa-animation-duration, 1s);
  -webkit-animation-iteration-count: var(--fa-animation-iteration-count, infinite);
          animation-iteration-count: var(--fa-animation-iteration-count, infinite);
  -webkit-animation-timing-function: var(--fa-animation-timing, linear);
          animation-timing-function: var(--fa-animation-timing, linear);
}

.fa-spin {
  -webkit-animation-name: fa-spin;
          animation-name: fa-spin;
  -webkit-animation-delay: var(--fa-animation-delay, 0s);
          animation-delay: var(--fa-animation-delay, 0s);
  -webkit-animation-direction: var(--fa-animation-direction, normal);
          animation-direction: var(--fa-animation-direction, normal);
  -webkit-animation-duration: var(--fa-animation-duration, 2s);
          animation-duration: var(--fa-animation-duration, 2s);
  -webkit-animation-iteration-count: var(--fa-animation-iteration-count, infinite);
          animation-iteration-count: var(--fa-animation-iteration-count, infinite);
  -webkit-animation-timing-function: var(--fa-animation-timing, linear);
          animation-timing-function: var(--fa-animation-timing, linear);
}

.fa-spin-reverse {
  --fa-animation-direction: reverse;
}

.fa-pulse,
.fa-spin-pulse {
  -webkit-animation-name: fa-spin;
          animation-name: fa-spin;
  -webkit-animation-direction: var(--fa-animation-direction, normal);
          animation-direction: var(--fa-animation-direction, normal);
  -webkit-animation-duration: var(--fa-animation-duration, 1s);
          animation-duration: var(--fa-animation-duration, 1s);
  -webkit-animation-iteration-count: var(--fa-animation-iteration-count, infinite);
          animation-iteration-count: var(--fa-animation-iteration-count, infinite);
  -webkit-animation-timing-function: var(--fa-animation-timing, steps(8));
          animation-timing-function: var(--fa-animation-timing, steps(8));
}

@media (prefers-reduced-motion: reduce) {
  .fa-beat,
.fa-bounce,
.fa-fade,
.fa-beat-fade,
.fa-flip,
.fa-pulse,
.fa-shake,
.fa-spin,
.fa-spin-pulse {
    -webkit-animation-delay: -1ms;
            animation-delay: -1ms;
    -webkit-animation-duration: 1ms;
            animation-duration: 1ms;
    -webkit-animation-iteration-count: 1;
            animation-iteration-count: 1;
    -webkit-transition-delay: 0s;
            transition-delay: 0s;
    -webkit-transition-duration: 0s;
            transition-duration: 0s;
  }
}
@-webkit-keyframes fa-beat {
  0%, 90% {
    -webkit-transform: scale(1);
            transform: scale(1);
  }
  45% {
    -webkit-transform: scale(var(--fa-beat-scale, 1.25));
            transform: scale(var(--fa-beat-scale, 1.25));
  }
}
@keyframes fa-beat {
  0%, 90% {
    -webkit-transform: scale(1);
            transform: scale(1);
  }
  45% {
    -webkit-transform: scale(var(--fa-beat-scale, 1.25));
            transform: scale(var(--fa-beat-scale, 1.25));
  }
}
@-webkit-keyframes fa-bounce {
  0% {
    -webkit-transform: scale(1, 1) translateY(0);
            transform: scale(1, 1) translateY(0);
  }
  10% {
    -webkit-transform: scale(var(--fa-bounce-start-scale-x, 1.1), var(--fa-bounce-start-scale-y, 0.9)) translateY(0);
            transform: scale(var(--fa-bounce-start-scale-x, 1.1), var(--fa-bounce-start-scale-y, 0.9)) translateY(0);
  }
  30% {
    -webkit-transform: scale(var(--fa-bounce-jump-scale-x, 0.9), var(--fa-bounce-jump-scale-y, 1.1)) translateY(var(--fa-bounce-height, -0.5em));
            transform: scale(var(--fa-bounce-jump-scale-x, 0.9), var(--fa-bounce-jump-scale-y, 1.1)) translateY(var(--fa-bounce-height, -0.5em));
  }
  50% {
    -webkit-transform: scale(var(--fa-bounce-land-scale-x, 1.05), var(--fa-bounce-land-scale-y, 0.95)) translateY(0);
            transform: scale(var(--fa-bounce-land-scale-x, 1.05), var(--fa-bounce-land-scale-y, 0.95)) translateY(0);
  }
  57% {
    -webkit-transform: scale(1, 1) translateY(var(--fa-bounce-rebound, -0.125em));
            transform: scale(1, 1) translateY(var(--fa-bounce-rebound, -0.125em));
  }
  64% {
    -webkit-transform: scale(1, 1) translateY(0);
            transform: scale(1, 1) translateY(0);
  }
  100% {
    -webkit-transform: scale(1, 1) translateY(0);
            transform: scale(1, 1) translateY(0);
  }
}
@keyframes fa-bounce {
  0% {
    -webkit-transform: scale(1, 1) translateY(0);
            transform: scale(1, 1) translateY(0);
  }
  10% {
    -webkit-transform: scale(var(--fa-bounce-start-scale-x, 1.1), var(--fa-bounce-start-scale-y, 0.9)) translateY(0);
            transform: scale(var(--fa-bounce-start-scale-x, 1.1), var(--fa-bounce-start-scale-y, 0.9)) translateY(0);
  }
  30% {
    -webkit-transform: scale(var(--fa-bounce-jump-scale-x, 0.9), var(--fa-bounce-jump-scale-y, 1.1)) translateY(var(--fa-bounce-height, -0.5em));
            transform: scale(var(--fa-bounce-jump-scale-x, 0.9), var(--fa-bounce-jump-scale-y, 1.1)) translateY(var(--fa-bounce-height, -0.5em));
  }
  50% {
    -webkit-transform: scale(var(--fa-bounce-land-scale-x, 1.05), var(--fa-bounce-land-scale-y, 0.95)) translateY(0);
            transform: scale(var(--fa-bounce-land-scale-x, 1.05), var(--fa-bounce-land-scale-y, 0.95)) translateY(0);
  }
  57% {
    -webkit-transform: scale(1, 1) translateY(var(--fa-bounce-rebound, -0.125em));
            transform: scale(1, 1) translateY(var(--fa-bounce-rebound, -0.125em));
  }
  64% {
    -webkit-transform: scale(1, 1) translateY(0);
            transform: scale(1, 1) translateY(0);
  }
  100% {
    -webkit-transform: scale(1, 1) translateY(0);
            transform: scale(1, 1) translateY(0);
  }
}
@-webkit-keyframes fa-fade {
  50% {
    opacity: var(--fa-fade-opacity, 0.4);
  }
}
@keyframes fa-fade {
  50% {
    opacity: var(--fa-fade-opacity, 0.4);
  }
}
@-webkit-keyframes fa-beat-fade {
  0%, 100% {
    opacity: var(--fa-beat-fade-opacity, 0.4);
    -webkit-transform: scale(1);
            transform: scale(1);
  }
  50% {
    opacity: 1;
    -webkit-transform: scale(var(--fa-beat-fade-scale, 1.125));
            transform: scale(var(--fa-beat-fade-scale, 1.125));
  }
}
@keyframes fa-beat-fade {
  0%, 100% {
    opacity: var(--fa-beat-fade-opacity, 0.4);
    -webkit-transform: scale(1);
            transform: scale(1);
  }
  50% {
    opacity: 1;
    -webkit-transform: scale(var(--fa-beat-fade-scale, 1.125));
            transform: scale(var(--fa-beat-fade-scale, 1.125));
  }
}
@-webkit-keyframes fa-flip {
  50% {
    -webkit-transform: rotate3d(var(--fa-flip-x, 0), var(--fa-flip-y, 1), var(--fa-flip-z, 0), var(--fa-flip-angle, -180deg));
            transform: rotate3d(var(--fa-flip-x, 0), var(--fa-flip-y, 1), var(--fa-flip-z, 0), var(--fa-flip-angle, -180deg));
  }
}
@keyframes fa-flip {
  50% {
    -webkit-transform: rotate3d(var(--fa-flip-x, 0), var(--fa-flip-y, 1), var(--fa-flip-z, 0), var(--fa-flip-angle, -180deg));
            transform: rotate3d(var(--fa-flip-x, 0), var(--fa-flip-y, 1), var(--fa-flip-z, 0), var(--fa-flip-angle, -180deg));
  }
}
@-webkit-keyframes fa-shake {
  0% {
    -webkit-transform: rotate(-15deg);
            transform: rotate(-15deg);
  }
  4% {
    -webkit-transform: rotate(15deg);
            transform: rotate(15deg);
  }
  8%, 24% {
    -webkit-transform: rotate(-18deg);
            transform: rotate(-18deg);
  }
  12%, 28% {
    -webkit-transform: rotate(18deg);
            transform: rotate(18deg);
  }
  16% {
    -webkit-transform: rotate(-22deg);
            transform: rotate(-22deg);
  }
  20% {
    -webkit-transform: rotate(22deg);
            transform: rotate(22deg);
  }
  32% {
    -webkit-transform: rotate(-12deg);
            transform: rotate(-12deg);
  }
  36% {
    -webkit-transform: rotate(12deg);
            transform: rotate(12deg);
  }
  40%, 100% {
    -webkit-transform: rotate(0deg);
            transform: rotate(0deg);
  }
}
@keyframes fa-shake {
  0% {
    -webkit-transform: rotate(-15deg);
            transform: rotate(-15deg);
  }
  4% {
    -webkit-transform: rotate(15deg);
            transform: rotate(15deg);
  }
  8%, 24% {
    -webkit-transform: rotate(-18deg);
            transform: rotate(-18deg);
  }
  12%, 28% {
    -webkit-transform: rotate(18deg);
            transform: rotate(18deg);
  }
  16% {
    -webkit-transform: rotate(-22deg);
            transform: rotate(-22deg);
  }
  20% {
    -webkit-transform: rotate(22deg);
            transform: rotate(22deg);
  }
  32% {
    -webkit-transform: rotate(-12deg);
            transform: rotate(-12deg);
  }
  36% {
    -webkit-transform: rotate(12deg);
            transform: rotate(12deg);
  }
  40%, 100% {
    -webkit-transform: rotate(0deg);
            transform: rotate(0deg);
  }
}
@-webkit-keyframes fa-spin {
  0% {
    -webkit-transform: rotate(0deg);
            transform: rotate(0deg);
  }
  100% {
    -webkit-transform: rotate(360deg);
            transform: rotate(360deg);
  }
}
@keyframes fa-spin {
  0% {
    -webkit-transform: rotate(0deg);
            transform: rotate(0deg);
  }
  100% {
    -webkit-transform: rotate(360deg);
            transform: rotate(360deg);
  }
}
.fa-rotate-90 {
  -webkit-transform: rotate(90deg);
          transform: rotate(90deg);
}

.fa-rotate-180 {
  -webkit-transform: rotate(180deg);
          transform: rotate(180deg);
}

.fa-rotate-270 {
  -webkit-transform: rotate(270deg);
          transform: rotate(270deg);
}

.fa-flip-horizontal {
  -webkit-transform: scale(-1, 1);
          transform: scale(-1, 1);
}

.fa-flip-vertical {
  -webkit-transform: scale(1, -1);
          transform: scale(1, -1);
}

.fa-flip-both,
.fa-flip-horizontal.fa-flip-vertical {
  -webkit-transform: scale(-1, -1);
          transform: scale(-1, -1);
}

.fa-rotate-by {
  -webkit-transform: rotate(var(--fa-rotate-angle, none));
          transform: rotate(var(--fa-rotate-angle, none));
}

.fa-stack {
  display: inline-block;
  vertical-align: middle;
  height: 2em;
  position: relative;
  width: 2.5em;
}

.fa-stack-1x,
.fa-stack-2x {
  bottom: 0;
  left: 0;
  margin: auto;
  position: absolute;
  right: 0;
  top: 0;
  z-index: var(--fa-stack-z-index, auto);
}

.svg-inline--fa.fa-stack-1x {
  height: 1em;
  width: 1.25em;
}
.svg-inline--fa.fa-stack-2x {
  height: 2em;
  width: 2.5em;
}

.fa-inverse {
  color: var(--fa-inverse, #fff);
}

.sr-only,
.fa-sr-only {
  position: absolute;
  width: 1px;
  height: 1px;
  padding: 0;
  margin: -1px;
  overflow: hidden;
  clip: rect(0, 0, 0, 0);
  white-space: nowrap;
  border-width: 0;
}

.sr-only-focusable:not(:focus),
.fa-sr-only-focusable:not(:focus) {
  position: absolute;
  width: 1px;
  height: 1px;
  padding: 0;
  margin: -1px;
  overflow: hidden;
  clip: rect(0, 0, 0, 0);
  white-space: nowrap;
  border-width: 0;
}

.svg-inline--fa .fa-primary {
  fill: var(--fa-primary-color, currentColor);
  opacity: var(--fa-primary-opacity, 1);
}

.svg-inline--fa .fa-secondary {
  fill: var(--fa-secondary-color, currentColor);
  opacity: var(--fa-secondary-opacity, 0.4);
}

.svg-inline--fa.fa-swap-opacity .fa-primary {
  opacity: var(--fa-secondary-opacity, 0.4);
}

.svg-inline--fa.fa-swap-opacity .fa-secondary {
  opacity: var(--fa-primary-opacity, 1);
}

.svg-inline--fa mask .fa-primary,
.svg-inline--fa mask .fa-secondary {
  fill: black;
}

.fad.fa-inverse,
.fa-duotone.fa-inverse {
  color: var(--fa-inverse, #fff);
}`;function QT(){var t=zT,e=BT,n=V.cssPrefix,r=V.replacementClass,i=pO;if(n!==t||r!==e){var s=new RegExp("\\.".concat(t,"\\-"),"g"),o=new RegExp("\\--".concat(t,"\\-"),"g"),a=new RegExp("\\.".concat(e),"g");i=i.replace(s,".".concat(n,"-")).replace(o,"--".concat(n,"-")).replace(a,".".concat(r))}return i}var Zy=!1;function eh(){V.autoAddCss&&!Zy&&(uO(QT()),Zy=!0)}var mO={mixout:function(){return{dom:{css:QT,insertCss:eh}}},hooks:function(){return{beforeDOMElementCreation:function(){eh()},beforeI2svg:function(){eh()}}}},Xn=Vr||{};Xn[Yn]||(Xn[Yn]={});Xn[Yn].styles||(Xn[Yn].styles={});Xn[Yn].hooks||(Xn[Yn].hooks={});Xn[Yn].shims||(Xn[Yn].shims=[]);var an=Xn[Yn],YT=[],gO=function t(){_e.removeEventListener("DOMContentLoaded",t),Zu=1,YT.map(function(e){return e()})},Zu=!1;nr&&(Zu=(_e.documentElement.doScroll?/^loaded|^c/:/^loaded|^i|^c/).test(_e.readyState),Zu||_e.addEventListener("DOMContentLoaded",gO));function vO(t){nr&&(Zu?setTimeout(t,0):YT.push(t))}function Ya(t){var e=t.tag,n=t.attributes,r=n===void 0?{}:n,i=t.children,s=i===void 0?[]:i;return typeof t=="string"?qT(t):"<".concat(e," ").concat(fO(r),">").concat(s.map(Ya).join(""),"</").concat(e,">")}function e0(t,e,n){if(t&&t[e]&&t[e][n])return{prefix:e,iconName:n,icon:t[e][n]}}var yO=function(e,n){return function(r,i,s,o){return e.call(n,r,i,s,o)}},th=function(e,n,r,i){var s=Object.keys(e),o=s.length,a=i!==void 0?yO(n,i):n,l,u,c;for(r===void 0?(l=1,c=e[s[0]]):(l=0,c=r);l<o;l++)u=s[l],c=a(c,e[u],u,e);return c};function _O(t){for(var e=[],n=0,r=t.length;n<r;){var i=t.charCodeAt(n++);if(i>=55296&&i<=56319&&n<r){var s=t.charCodeAt(n++);(s&64512)==56320?e.push(((i&1023)<<10)+(s&1023)+65536):(e.push(i),n--)}else e.push(i)}return e}function Od(t){var e=_O(t);return e.length===1?e[0].toString(16):null}function wO(t,e){var n=t.length,r=t.charCodeAt(e),i;return r>=55296&&r<=56319&&n>e+1&&(i=t.charCodeAt(e+1),i>=56320&&i<=57343)?(r-55296)*1024+i-56320+65536:r}function t0(t){return Object.keys(t).reduce(function(e,n){var r=t[n],i=!!r.icon;return i?e[r.iconName]=r.icon:e[n]=r,e},{})}function Dd(t,e){var n=arguments.length>2&&arguments[2]!==void 0?arguments[2]:{},r=n.skipHooks,i=r===void 0?!1:r,s=t0(e);typeof an.hooks.addPack=="function"&&!i?an.hooks.addPack(t,t0(e)):an.styles[t]=M(M({},an.styles[t]||{}),s),t==="fas"&&Dd("fa",e)}var Dl,Ml,Ll,es=an.styles,EO=an.shims,TO=(Dl={},Be(Dl,ge,Object.values(ya[ge])),Be(Dl,Re,Object.values(ya[Re])),Dl),Gm=null,XT={},JT={},ZT={},eI={},tI={},IO=(Ml={},Be(Ml,ge,Object.keys(ga[ge])),Be(Ml,Re,Object.keys(ga[Re])),Ml);function SO(t){return~iO.indexOf(t)}function kO(t,e){var n=e.split("-"),r=n[0],i=n.slice(1).join("-");return r===t&&i!==""&&!SO(i)?i:null}var nI=function(){var e=function(s){return th(es,function(o,a,l){return o[l]=th(a,s,{}),o},{})};XT=e(function(i,s,o){if(s[3]&&(i[s[3]]=o),s[2]){var a=s[2].filter(function(l){return typeof l=="number"});a.forEach(function(l){i[l.toString(16)]=o})}return i}),JT=e(function(i,s,o){if(i[o]=o,s[2]){var a=s[2].filter(function(l){return typeof l=="string"});a.forEach(function(l){i[l]=o})}return i}),tI=e(function(i,s,o){var a=s[2];return i[o]=o,a.forEach(function(l){i[l]=o}),i});var n="far"in es||V.autoFetchSvg,r=th(EO,function(i,s){var o=s[0],a=s[1],l=s[2];return a==="far"&&!n&&(a="fas"),typeof o=="string"&&(i.names[o]={prefix:a,iconName:l}),typeof o=="number"&&(i.unicodes[o.toString(16)]={prefix:a,iconName:l}),i},{names:{},unicodes:{}});ZT=r.names,eI=r.unicodes,Gm=Qc(V.styleDefault,{family:V.familyDefault})};lO(function(t){Gm=Qc(t.styleDefault,{family:V.familyDefault})});nI();function qm(t,e){return(XT[t]||{})[e]}function AO(t,e){return(JT[t]||{})[e]}function oi(t,e){return(tI[t]||{})[e]}function rI(t){return ZT[t]||{prefix:null,iconName:null}}function xO(t){var e=eI[t],n=qm("fas",t);return e||(n?{prefix:"fas",iconName:n}:null)||{prefix:null,iconName:null}}function Fr(){return Gm}var Qm=function(){return{prefix:null,iconName:null,rest:[]}};function Qc(t){var e=arguments.length>1&&arguments[1]!==void 0?arguments[1]:{},n=e.family,r=n===void 0?ge:n,i=ga[r][t],s=va[r][t]||va[r][i],o=t in an.styles?t:null;return s||o||null}var n0=(Ll={},Be(Ll,ge,Object.keys(ya[ge])),Be(Ll,Re,Object.keys(ya[Re])),Ll);function Yc(t){var e,n=arguments.length>1&&arguments[1]!==void 0?arguments[1]:{},r=n.skipLookups,i=r===void 0?!1:r,s=(e={},Be(e,ge,"".concat(V.cssPrefix,"-").concat(ge)),Be(e,Re,"".concat(V.cssPrefix,"-").concat(Re)),e),o=null,a=ge;(t.includes(s[ge])||t.some(function(u){return n0[ge].includes(u)}))&&(a=ge),(t.includes(s[Re])||t.some(function(u){return n0[Re].includes(u)}))&&(a=Re);var l=t.reduce(function(u,c){var h=kO(V.cssPrefix,c);if(es[c]?(c=TO[a].includes(c)?J4[a][c]:c,o=c,u.prefix=c):IO[a].indexOf(c)>-1?(o=c,u.prefix=Qc(c,{family:a})):h?u.iconName=h:c!==V.replacementClass&&c!==s[ge]&&c!==s[Re]&&u.rest.push(c),!i&&u.prefix&&u.iconName){var d=o==="fa"?rI(u.iconName):{},y=oi(u.prefix,u.iconName);d.prefix&&(o=null),u.iconName=d.iconName||y||u.iconName,u.prefix=d.prefix||u.prefix,u.prefix==="far"&&!es.far&&es.fas&&!V.autoFetchSvg&&(u.prefix="fas")}return u},Qm());return(t.includes("fa-brands")||t.includes("fab"))&&(l.prefix="fab"),(t.includes("fa-duotone")||t.includes("fad"))&&(l.prefix="fad"),!l.prefix&&a===Re&&(es.fass||V.autoFetchSvg)&&(l.prefix="fass",l.iconName=oi(l.prefix,l.iconName)||l.iconName),(l.prefix==="fa"||o==="fa")&&(l.prefix=Fr()||"fas"),l}var PO=function(){function t(){U4(this,t),this.definitions={}}return $4(t,[{key:"add",value:function(){for(var n=this,r=arguments.length,i=new Array(r),s=0;s<r;s++)i[s]=arguments[s];var o=i.reduce(this._pullDefinitions,{});Object.keys(o).forEach(function(a){n.definitions[a]=M(M({},n.definitions[a]||{}),o[a]),Dd(a,o[a]);var l=ya[ge][a];l&&Dd(l,o[a]),nI()})}},{key:"reset",value:function(){this.definitions={}}},{key:"_pullDefinitions",value:function(n,r){var i=r.prefix&&r.iconName&&r.icon?{0:r}:r;return Object.keys(i).map(function(s){var o=i[s],a=o.prefix,l=o.iconName,u=o.icon,c=u[2];n[a]||(n[a]={}),c.length>0&&c.forEach(function(h){typeof h=="string"&&(n[a][h]=u)}),n[a][l]=u}),n}}]),t}(),r0=[],ts={},ms={},RO=Object.keys(ms);function CO(t,e){var n=e.mixoutsTo;return r0=t,ts={},Object.keys(ms).forEach(function(r){RO.indexOf(r)===-1&&delete ms[r]}),r0.forEach(function(r){var i=r.mixout?r.mixout():{};if(Object.keys(i).forEach(function(o){typeof i[o]=="function"&&(n[o]=i[o]),Ju(i[o])==="object"&&Object.keys(i[o]).forEach(function(a){n[o]||(n[o]={}),n[o][a]=i[o][a]})}),r.hooks){var s=r.hooks();Object.keys(s).forEach(function(o){ts[o]||(ts[o]=[]),ts[o].push(s[o])})}r.provides&&r.provides(ms)}),n}function Md(t,e){for(var n=arguments.length,r=new Array(n>2?n-2:0),i=2;i<n;i++)r[i-2]=arguments[i];var s=ts[t]||[];return s.forEach(function(o){e=o.apply(null,[e].concat(r))}),e}function Ai(t){for(var e=arguments.length,n=new Array(e>1?e-1:0),r=1;r<e;r++)n[r-1]=arguments[r];var i=ts[t]||[];i.forEach(function(s){s.apply(null,n)})}function Jn(){var t=arguments[0],e=Array.prototype.slice.call(arguments,1);return ms[t]?ms[t].apply(null,e):void 0}function Ld(t){t.prefix==="fa"&&(t.prefix="fas");var e=t.iconName,n=t.prefix||Fr();if(e)return e=oi(n,e)||e,e0(iI.definitions,n,e)||e0(an.styles,n,e)}var iI=new PO,NO=function(){V.autoReplaceSvg=!1,V.observeMutations=!1,Ai("noAuto")},bO={i2svg:function(){var e=arguments.length>0&&arguments[0]!==void 0?arguments[0]:{};return nr?(Ai("beforeI2svg",e),Jn("pseudoElements2svg",e),Jn("i2svg",e)):Promise.reject("Operation requires a DOM of some kind.")},watch:function(){var e=arguments.length>0&&arguments[0]!==void 0?arguments[0]:{},n=e.autoReplaceSvgRoot;V.autoReplaceSvg===!1&&(V.autoReplaceSvg=!0),V.observeMutations=!0,vO(function(){DO({autoReplaceSvgRoot:n}),Ai("watch",e)})}},OO={icon:function(e){if(e===null)return null;if(Ju(e)==="object"&&e.prefix&&e.iconName)return{prefix:e.prefix,iconName:oi(e.prefix,e.iconName)||e.iconName};if(Array.isArray(e)&&e.length===2){var n=e[1].indexOf("fa-")===0?e[1].slice(3):e[1],r=Qc(e[0]);return{prefix:r,iconName:oi(r,n)||n}}if(typeof e=="string"&&(e.indexOf("".concat(V.cssPrefix,"-"))>-1||e.match(Z4))){var i=Yc(e.split(" "),{skipLookups:!0});return{prefix:i.prefix||Fr(),iconName:oi(i.prefix,i.iconName)||i.iconName}}if(typeof e=="string"){var s=Fr();return{prefix:s,iconName:oi(s,e)||e}}}},$t={noAuto:NO,config:V,dom:bO,parse:OO,library:iI,findIconDefinition:Ld,toHtml:Ya},DO=function(){var e=arguments.length>0&&arguments[0]!==void 0?arguments[0]:{},n=e.autoReplaceSvgRoot,r=n===void 0?_e:n;(Object.keys(an.styles).length>0||V.autoFetchSvg)&&nr&&V.autoReplaceSvg&&$t.dom.i2svg({node:r})};function Xc(t,e){return Object.defineProperty(t,"abstract",{get:e}),Object.defineProperty(t,"html",{get:function(){return t.abstract.map(function(r){return Ya(r)})}}),Object.defineProperty(t,"node",{get:function(){if(nr){var r=_e.createElement("div");return r.innerHTML=t.html,r.children}}}),t}function MO(t){var e=t.children,n=t.main,r=t.mask,i=t.attributes,s=t.styles,o=t.transform;if(Km(o)&&n.found&&!r.found){var a=n.width,l=n.height,u={x:a/l/2,y:.5};i.style=qc(M(M({},s),{},{"transform-origin":"".concat(u.x+o.x/16,"em ").concat(u.y+o.y/16,"em")}))}return[{tag:"svg",attributes:i,children:e}]}function LO(t){var e=t.prefix,n=t.iconName,r=t.children,i=t.attributes,s=t.symbol,o=s===!0?"".concat(e,"-").concat(V.cssPrefix,"-").concat(n):s;return[{tag:"svg",attributes:{style:"display: none;"},children:[{tag:"symbol",attributes:M(M({},i),{},{id:o}),children:r}]}]}function Ym(t){var e=t.icons,n=e.main,r=e.mask,i=t.prefix,s=t.iconName,o=t.transform,a=t.symbol,l=t.title,u=t.maskId,c=t.titleId,h=t.extra,d=t.watchable,y=d===void 0?!1:d,_=r.found?r:n,w=_.width,T=_.height,g=i==="fak",p=[V.replacementClass,s?"".concat(V.cssPrefix,"-").concat(s):""].filter(function(B){return h.classes.indexOf(B)===-1}).filter(function(B){return B!==""||!!B}).concat(h.classes).join(" "),v={children:[],attributes:M(M({},h.attributes),{},{"data-prefix":i,"data-icon":s,class:p,role:h.attributes.role||"img",xmlns:"http://www.w3.org/2000/svg",viewBox:"0 0 ".concat(w," ").concat(T)})},m=g&&!~h.classes.indexOf("fa-fw")?{width:"".concat(w/T*16*.0625,"em")}:{};y&&(v.attributes[ki]=""),l&&(v.children.push({tag:"title",attributes:{id:v.attributes["aria-labelledby"]||"title-".concat(c||wa())},children:[l]}),delete v.attributes.title);var S=M(M({},v),{},{prefix:i,iconName:s,main:n,mask:r,maskId:u,transform:o,symbol:a,styles:M(M({},m),h.styles)}),P=r.found&&n.found?Jn("generateAbstractMask",S)||{children:[],attributes:{}}:Jn("generateAbstractIcon",S)||{children:[],attributes:{}},A=P.children,O=P.attributes;return S.children=A,S.attributes=O,a?LO(S):MO(S)}function i0(t){var e=t.content,n=t.width,r=t.height,i=t.transform,s=t.title,o=t.extra,a=t.watchable,l=a===void 0?!1:a,u=M(M(M({},o.attributes),s?{title:s}:{}),{},{class:o.classes.join(" ")});l&&(u[ki]="");var c=M({},o.styles);Km(i)&&(c.transform=dO({transform:i,startCentered:!0,width:n,height:r}),c["-webkit-transform"]=c.transform);var h=qc(c);h.length>0&&(u.style=h);var d=[];return d.push({tag:"span",attributes:u,children:[e]}),s&&d.push({tag:"span",attributes:{class:"sr-only"},children:[s]}),d}function jO(t){var e=t.content,n=t.title,r=t.extra,i=M(M(M({},r.attributes),n?{title:n}:{}),{},{class:r.classes.join(" ")}),s=qc(r.styles);s.length>0&&(i.style=s);var o=[];return o.push({tag:"span",attributes:i,children:[e]}),n&&o.push({tag:"span",attributes:{class:"sr-only"},children:[n]}),o}var nh=an.styles;function jd(t){var e=t[0],n=t[1],r=t.slice(4),i=Um(r,1),s=i[0],o=null;return Array.isArray(s)?o={tag:"g",attributes:{class:"".concat(V.cssPrefix,"-").concat(si.GROUP)},children:[{tag:"path",attributes:{class:"".concat(V.cssPrefix,"-").concat(si.SECONDARY),fill:"currentColor",d:s[0]}},{tag:"path",attributes:{class:"".concat(V.cssPrefix,"-").concat(si.PRIMARY),fill:"currentColor",d:s[1]}}]}:o={tag:"path",attributes:{fill:"currentColor",d:s}},{found:!0,width:e,height:n,icon:o}}var VO={found:!1,width:512,height:512};function FO(t,e){!HT&&!V.showMissingIcons&&t&&console.error('Icon with name "'.concat(t,'" and prefix "').concat(e,'" is missing.'))}function Vd(t,e){var n=e;return e==="fa"&&V.styleDefault!==null&&(e=Fr()),new Promise(function(r,i){if(Jn("missingIconAbstract"),n==="fa"){var s=rI(t)||{};t=s.iconName||t,e=s.prefix||e}if(t&&e&&nh[e]&&nh[e][t]){var o=nh[e][t];return r(jd(o))}FO(t,e),r(M(M({},VO),{},{icon:V.showMissingIcons&&t?Jn("missingIconAbstract")||{}:{}}))})}var s0=function(){},Fd=V.measurePerformance&&Pl&&Pl.mark&&Pl.measure?Pl:{mark:s0,measure:s0},po='FA "6.4.0"',UO=function(e){return Fd.mark("".concat(po," ").concat(e," begins")),function(){return sI(e)}},sI=function(e){Fd.mark("".concat(po," ").concat(e," ends")),Fd.measure("".concat(po," ").concat(e),"".concat(po," ").concat(e," begins"),"".concat(po," ").concat(e," ends"))},Xm={begin:UO,end:sI},tu=function(){};function o0(t){var e=t.getAttribute?t.getAttribute(ki):null;return typeof e=="string"}function $O(t){var e=t.getAttribute?t.getAttribute(zm):null,n=t.getAttribute?t.getAttribute(Bm):null;return e&&n}function zO(t){return t&&t.classList&&t.classList.contains&&t.classList.contains(V.replacementClass)}function BO(){if(V.autoReplaceSvg===!0)return nu.replace;var t=nu[V.autoReplaceSvg];return t||nu.replace}function HO(t){return _e.createElementNS("http://www.w3.org/2000/svg",t)}function WO(t){return _e.createElement(t)}function oI(t){var e=arguments.length>1&&arguments[1]!==void 0?arguments[1]:{},n=e.ceFn,r=n===void 0?t.tag==="svg"?HO:WO:n;if(typeof t=="string")return _e.createTextNode(t);var i=r(t.tag);Object.keys(t.attributes||[]).forEach(function(o){i.setAttribute(o,t.attributes[o])});var s=t.children||[];return s.forEach(function(o){i.appendChild(oI(o,{ceFn:r}))}),i}function KO(t){var e=" ".concat(t.outerHTML," ");return e="".concat(e,"Font Awesome fontawesome.com "),e}var nu={replace:function(e){var n=e[0];if(n.parentNode)if(e[1].forEach(function(i){n.parentNode.insertBefore(oI(i),n)}),n.getAttribute(ki)===null&&V.keepOriginalSource){var r=_e.createComment(KO(n));n.parentNode.replaceChild(r,n)}else n.remove()},nest:function(e){var n=e[0],r=e[1];if(~Wm(n).indexOf(V.replacementClass))return nu.replace(e);var i=new RegExp("".concat(V.cssPrefix,"-.*"));if(delete r[0].attributes.id,r[0].attributes.class){var s=r[0].attributes.class.split(" ").reduce(function(a,l){return l===V.replacementClass||l.match(i)?a.toSvg.push(l):a.toNode.push(l),a},{toNode:[],toSvg:[]});r[0].attributes.class=s.toSvg.join(" "),s.toNode.length===0?n.removeAttribute("class"):n.setAttribute("class",s.toNode.join(" "))}var o=r.map(function(a){return Ya(a)}).join(`
`);n.setAttribute(ki,""),n.innerHTML=o}};function a0(t){t()}function aI(t,e){var n=typeof e=="function"?e:tu;if(t.length===0)n();else{var r=a0;V.mutateApproach===Y4&&(r=Vr.requestAnimationFrame||a0),r(function(){var i=BO(),s=Xm.begin("mutate");t.map(i),s(),n()})}}var Jm=!1;function lI(){Jm=!0}function Ud(){Jm=!1}var ec=null;function l0(t){if(Xy&&V.observeMutations){var e=t.treeCallback,n=e===void 0?tu:e,r=t.nodeCallback,i=r===void 0?tu:r,s=t.pseudoElementsCallback,o=s===void 0?tu:s,a=t.observeMutationsRoot,l=a===void 0?_e:a;ec=new Xy(function(u){if(!Jm){var c=Fr();Bs(u).forEach(function(h){if(h.type==="childList"&&h.addedNodes.length>0&&!o0(h.addedNodes[0])&&(V.searchPseudoElements&&o(h.target),n(h.target)),h.type==="attributes"&&h.target.parentNode&&V.searchPseudoElements&&o(h.target.parentNode),h.type==="attributes"&&o0(h.target)&&~rO.indexOf(h.attributeName))if(h.attributeName==="class"&&$O(h.target)){var d=Yc(Wm(h.target)),y=d.prefix,_=d.iconName;h.target.setAttribute(zm,y||c),_&&h.target.setAttribute(Bm,_)}else zO(h.target)&&i(h.target)})}}),nr&&ec.observe(l,{childList:!0,attributes:!0,characterData:!0,subtree:!0})}}function GO(){ec&&ec.disconnect()}function qO(t){var e=t.getAttribute("style"),n=[];return e&&(n=e.split(";").reduce(function(r,i){var s=i.split(":"),o=s[0],a=s.slice(1);return o&&a.length>0&&(r[o]=a.join(":").trim()),r},{})),n}function QO(t){var e=t.getAttribute("data-prefix"),n=t.getAttribute("data-icon"),r=t.innerText!==void 0?t.innerText.trim():"",i=Yc(Wm(t));return i.prefix||(i.prefix=Fr()),e&&n&&(i.prefix=e,i.iconName=n),i.iconName&&i.prefix||(i.prefix&&r.length>0&&(i.iconName=AO(i.prefix,t.innerText)||qm(i.prefix,Od(t.innerText))),!i.iconName&&V.autoFetchSvg&&t.firstChild&&t.firstChild.nodeType===Node.TEXT_NODE&&(i.iconName=t.firstChild.data)),i}function YO(t){var e=Bs(t.attributes).reduce(function(i,s){return i.name!=="class"&&i.name!=="style"&&(i[s.name]=s.value),i},{}),n=t.getAttribute("title"),r=t.getAttribute("data-fa-title-id");return V.autoA11y&&(n?e["aria-labelledby"]="".concat(V.replacementClass,"-title-").concat(r||wa()):(e["aria-hidden"]="true",e.focusable="false")),e}function XO(){return{iconName:null,title:null,titleId:null,prefix:null,transform:wn,symbol:!1,mask:{iconName:null,prefix:null,rest:[]},maskId:null,extra:{classes:[],styles:{},attributes:{}}}}function u0(t){var e=arguments.length>1&&arguments[1]!==void 0?arguments[1]:{styleParser:!0},n=QO(t),r=n.iconName,i=n.prefix,s=n.rest,o=YO(t),a=Md("parseNodeAttributes",{},t),l=e.styleParser?qO(t):[];return M({iconName:r,title:t.getAttribute("title"),titleId:t.getAttribute("data-fa-title-id"),prefix:i,transform:wn,mask:{iconName:null,prefix:null,rest:[]},maskId:null,symbol:!1,extra:{classes:s,styles:l,attributes:o}},a)}var JO=an.styles;function uI(t){var e=V.autoReplaceSvg==="nest"?u0(t,{styleParser:!1}):u0(t);return~e.extra.classes.indexOf(WT)?Jn("generateLayersText",t,e):Jn("generateSvgReplacementMutation",t,e)}var Ur=new Set;Hm.map(function(t){Ur.add("fa-".concat(t))});Object.keys(ga[ge]).map(Ur.add.bind(Ur));Object.keys(ga[Re]).map(Ur.add.bind(Ur));Ur=qa(Ur);function c0(t){var e=arguments.length>1&&arguments[1]!==void 0?arguments[1]:null;if(!nr)return Promise.resolve();var n=_e.documentElement.classList,r=function(h){return n.add("".concat(Jy,"-").concat(h))},i=function(h){return n.remove("".concat(Jy,"-").concat(h))},s=V.autoFetchSvg?Ur:Hm.map(function(c){return"fa-".concat(c)}).concat(Object.keys(JO));s.includes("fa")||s.push("fa");var o=[".".concat(WT,":not([").concat(ki,"])")].concat(s.map(function(c){return".".concat(c,":not([").concat(ki,"])")})).join(", ");if(o.length===0)return Promise.resolve();var a=[];try{a=Bs(t.querySelectorAll(o))}catch{}if(a.length>0)r("pending"),i("complete");else return Promise.resolve();var l=Xm.begin("onTree"),u=a.reduce(function(c,h){try{var d=uI(h);d&&c.push(d)}catch(y){HT||y.name==="MissingIcon"&&console.error(y)}return c},[]);return new Promise(function(c,h){Promise.all(u).then(function(d){aI(d,function(){r("active"),r("complete"),i("pending"),typeof e=="function"&&e(),l(),c()})}).catch(function(d){l(),h(d)})})}function ZO(t){var e=arguments.length>1&&arguments[1]!==void 0?arguments[1]:null;uI(t).then(function(n){n&&aI([n],e)})}function eD(t){return function(e){var n=arguments.length>1&&arguments[1]!==void 0?arguments[1]:{},r=(e||{}).icon?e:Ld(e||{}),i=n.mask;return i&&(i=(i||{}).icon?i:Ld(i||{})),t(r,M(M({},n),{},{mask:i}))}}var tD=function(e){var n=arguments.length>1&&arguments[1]!==void 0?arguments[1]:{},r=n.transform,i=r===void 0?wn:r,s=n.symbol,o=s===void 0?!1:s,a=n.mask,l=a===void 0?null:a,u=n.maskId,c=u===void 0?null:u,h=n.title,d=h===void 0?null:h,y=n.titleId,_=y===void 0?null:y,w=n.classes,T=w===void 0?[]:w,g=n.attributes,p=g===void 0?{}:g,v=n.styles,m=v===void 0?{}:v;if(e){var S=e.prefix,P=e.iconName,A=e.icon;return Xc(M({type:"icon"},e),function(){return Ai("beforeDOMElementCreation",{iconDefinition:e,params:n}),V.autoA11y&&(d?p["aria-labelledby"]="".concat(V.replacementClass,"-title-").concat(_||wa()):(p["aria-hidden"]="true",p.focusable="false")),Ym({icons:{main:jd(A),mask:l?jd(l.icon):{found:!1,width:null,height:null,icon:{}}},prefix:S,iconName:P,transform:M(M({},wn),i),symbol:o,title:d,maskId:c,titleId:_,extra:{attributes:p,styles:m,classes:T}})})}},nD={mixout:function(){return{icon:eD(tD)}},hooks:function(){return{mutationObserverCallbacks:function(n){return n.treeCallback=c0,n.nodeCallback=ZO,n}}},provides:function(e){e.i2svg=function(n){var r=n.node,i=r===void 0?_e:r,s=n.callback,o=s===void 0?function(){}:s;return c0(i,o)},e.generateSvgReplacementMutation=function(n,r){var i=r.iconName,s=r.title,o=r.titleId,a=r.prefix,l=r.transform,u=r.symbol,c=r.mask,h=r.maskId,d=r.extra;return new Promise(function(y,_){Promise.all([Vd(i,a),c.iconName?Vd(c.iconName,c.prefix):Promise.resolve({found:!1,width:512,height:512,icon:{}})]).then(function(w){var T=Um(w,2),g=T[0],p=T[1];y([n,Ym({icons:{main:g,mask:p},prefix:a,iconName:i,transform:l,symbol:u,maskId:h,title:s,titleId:o,extra:d,watchable:!0})])}).catch(_)})},e.generateAbstractIcon=function(n){var r=n.children,i=n.attributes,s=n.main,o=n.transform,a=n.styles,l=qc(a);l.length>0&&(i.style=l);var u;return Km(o)&&(u=Jn("generateAbstractTransformGrouping",{main:s,transform:o,containerWidth:s.width,iconWidth:s.width})),r.push(u||s.icon),{children:r,attributes:i}}}},rD={mixout:function(){return{layer:function(n){var r=arguments.length>1&&arguments[1]!==void 0?arguments[1]:{},i=r.classes,s=i===void 0?[]:i;return Xc({type:"layer"},function(){Ai("beforeDOMElementCreation",{assembler:n,params:r});var o=[];return n(function(a){Array.isArray(a)?a.map(function(l){o=o.concat(l.abstract)}):o=o.concat(a.abstract)}),[{tag:"span",attributes:{class:["".concat(V.cssPrefix,"-layers")].concat(qa(s)).join(" ")},children:o}]})}}}},iD={mixout:function(){return{counter:function(n){var r=arguments.length>1&&arguments[1]!==void 0?arguments[1]:{},i=r.title,s=i===void 0?null:i,o=r.classes,a=o===void 0?[]:o,l=r.attributes,u=l===void 0?{}:l,c=r.styles,h=c===void 0?{}:c;return Xc({type:"counter",content:n},function(){return Ai("beforeDOMElementCreation",{content:n,params:r}),jO({content:n.toString(),title:s,extra:{attributes:u,styles:h,classes:["".concat(V.cssPrefix,"-layers-counter")].concat(qa(a))}})})}}}},sD={mixout:function(){return{text:function(n){var r=arguments.length>1&&arguments[1]!==void 0?arguments[1]:{},i=r.transform,s=i===void 0?wn:i,o=r.title,a=o===void 0?null:o,l=r.classes,u=l===void 0?[]:l,c=r.attributes,h=c===void 0?{}:c,d=r.styles,y=d===void 0?{}:d;return Xc({type:"text",content:n},function(){return Ai("beforeDOMElementCreation",{content:n,params:r}),i0({content:n,transform:M(M({},wn),s),title:a,extra:{attributes:h,styles:y,classes:["".concat(V.cssPrefix,"-layers-text")].concat(qa(u))}})})}}},provides:function(e){e.generateLayersText=function(n,r){var i=r.title,s=r.transform,o=r.extra,a=null,l=null;if($T){var u=parseInt(getComputedStyle(n).fontSize,10),c=n.getBoundingClientRect();a=c.width/u,l=c.height/u}return V.autoA11y&&!i&&(o.attributes["aria-hidden"]="true"),Promise.resolve([n,i0({content:n.innerHTML,width:a,height:l,transform:s,title:i,extra:o,watchable:!0})])}}},oD=new RegExp('"',"ug"),f0=[1105920,1112319];function aD(t){var e=t.replace(oD,""),n=wO(e,0),r=n>=f0[0]&&n<=f0[1],i=e.length===2?e[0]===e[1]:!1;return{value:Od(i?e[0]:e),isSecondary:r||i}}function h0(t,e){var n="".concat(Q4).concat(e.replace(":","-"));return new Promise(function(r,i){if(t.getAttribute(n)!==null)return r();var s=Bs(t.children),o=s.filter(function(A){return A.getAttribute(bd)===e})[0],a=Vr.getComputedStyle(t,e),l=a.getPropertyValue("font-family").match(eO),u=a.getPropertyValue("font-weight"),c=a.getPropertyValue("content");if(o&&!l)return t.removeChild(o),r();if(l&&c!=="none"&&c!==""){var h=a.getPropertyValue("content"),d=~["Sharp"].indexOf(l[2])?Re:ge,y=~["Solid","Regular","Light","Thin","Duotone","Brands","Kit"].indexOf(l[2])?va[d][l[2].toLowerCase()]:tO[d][u],_=aD(h),w=_.value,T=_.isSecondary,g=l[0].startsWith("FontAwesome"),p=qm(y,w),v=p;if(g){var m=xO(w);m.iconName&&m.prefix&&(p=m.iconName,y=m.prefix)}if(p&&!T&&(!o||o.getAttribute(zm)!==y||o.getAttribute(Bm)!==v)){t.setAttribute(n,v),o&&t.removeChild(o);var S=XO(),P=S.extra;P.attributes[bd]=e,Vd(p,y).then(function(A){var O=Ym(M(M({},S),{},{icons:{main:A,mask:Qm()},prefix:y,iconName:v,extra:P,watchable:!0})),B=_e.createElement("svg");e==="::before"?t.insertBefore(B,t.firstChild):t.appendChild(B),B.outerHTML=O.map(function(U){return Ya(U)}).join(`
`),t.removeAttribute(n),r()}).catch(i)}else r()}else r()})}function lD(t){return Promise.all([h0(t,"::before"),h0(t,"::after")])}function uD(t){return t.parentNode!==document.head&&!~X4.indexOf(t.tagName.toUpperCase())&&!t.getAttribute(bd)&&(!t.parentNode||t.parentNode.tagName!=="svg")}function d0(t){if(nr)return new Promise(function(e,n){var r=Bs(t.querySelectorAll("*")).filter(uD).map(lD),i=Xm.begin("searchPseudoElements");lI(),Promise.all(r).then(function(){i(),Ud(),e()}).catch(function(){i(),Ud(),n()})})}var cD={hooks:function(){return{mutationObserverCallbacks:function(n){return n.pseudoElementsCallback=d0,n}}},provides:function(e){e.pseudoElements2svg=function(n){var r=n.node,i=r===void 0?_e:r;V.searchPseudoElements&&d0(i)}}},p0=!1,fD={mixout:function(){return{dom:{unwatch:function(){lI(),p0=!0}}}},hooks:function(){return{bootstrap:function(){l0(Md("mutationObserverCallbacks",{}))},noAuto:function(){GO()},watch:function(n){var r=n.observeMutationsRoot;p0?Ud():l0(Md("mutationObserverCallbacks",{observeMutationsRoot:r}))}}}},m0=function(e){var n={size:16,x:0,y:0,flipX:!1,flipY:!1,rotate:0};return e.toLowerCase().split(" ").reduce(function(r,i){var s=i.toLowerCase().split("-"),o=s[0],a=s.slice(1).join("-");if(o&&a==="h")return r.flipX=!0,r;if(o&&a==="v")return r.flipY=!0,r;if(a=parseFloat(a),isNaN(a))return r;switch(o){case"grow":r.size=r.size+a;break;case"shrink":r.size=r.size-a;break;case"left":r.x=r.x-a;break;case"right":r.x=r.x+a;break;case"up":r.y=r.y-a;break;case"down":r.y=r.y+a;break;case"rotate":r.rotate=r.rotate+a;break}return r},n)},hD={mixout:function(){return{parse:{transform:function(n){return m0(n)}}}},hooks:function(){return{parseNodeAttributes:function(n,r){var i=r.getAttribute("data-fa-transform");return i&&(n.transform=m0(i)),n}}},provides:function(e){e.generateAbstractTransformGrouping=function(n){var r=n.main,i=n.transform,s=n.containerWidth,o=n.iconWidth,a={transform:"translate(".concat(s/2," 256)")},l="translate(".concat(i.x*32,", ").concat(i.y*32,") "),u="scale(".concat(i.size/16*(i.flipX?-1:1),", ").concat(i.size/16*(i.flipY?-1:1),") "),c="rotate(".concat(i.rotate," 0 0)"),h={transform:"".concat(l," ").concat(u," ").concat(c)},d={transform:"translate(".concat(o/2*-1," -256)")},y={outer:a,inner:h,path:d};return{tag:"g",attributes:M({},y.outer),children:[{tag:"g",attributes:M({},y.inner),children:[{tag:r.icon.tag,children:r.icon.children,attributes:M(M({},r.icon.attributes),y.path)}]}]}}}},rh={x:0,y:0,width:"100%",height:"100%"};function g0(t){var e=arguments.length>1&&arguments[1]!==void 0?arguments[1]:!0;return t.attributes&&(t.attributes.fill||e)&&(t.attributes.fill="black"),t}function dD(t){return t.tag==="g"?t.children:[t]}var pD={hooks:function(){return{parseNodeAttributes:function(n,r){var i=r.getAttribute("data-fa-mask"),s=i?Yc(i.split(" ").map(function(o){return o.trim()})):Qm();return s.prefix||(s.prefix=Fr()),n.mask=s,n.maskId=r.getAttribute("data-fa-mask-id"),n}}},provides:function(e){e.generateAbstractMask=function(n){var r=n.children,i=n.attributes,s=n.main,o=n.mask,a=n.maskId,l=n.transform,u=s.width,c=s.icon,h=o.width,d=o.icon,y=hO({transform:l,containerWidth:h,iconWidth:u}),_={tag:"rect",attributes:M(M({},rh),{},{fill:"white"})},w=c.children?{children:c.children.map(g0)}:{},T={tag:"g",attributes:M({},y.inner),children:[g0(M({tag:c.tag,attributes:M(M({},c.attributes),y.path)},w))]},g={tag:"g",attributes:M({},y.outer),children:[T]},p="mask-".concat(a||wa()),v="clip-".concat(a||wa()),m={tag:"mask",attributes:M(M({},rh),{},{id:p,maskUnits:"userSpaceOnUse",maskContentUnits:"userSpaceOnUse"}),children:[_,g]},S={tag:"defs",children:[{tag:"clipPath",attributes:{id:v},children:dD(d)},m]};return r.push(S,{tag:"rect",attributes:M({fill:"currentColor","clip-path":"url(#".concat(v,")"),mask:"url(#".concat(p,")")},rh)}),{children:r,attributes:i}}}},mD={provides:function(e){var n=!1;Vr.matchMedia&&(n=Vr.matchMedia("(prefers-reduced-motion: reduce)").matches),e.missingIconAbstract=function(){var r=[],i={fill:"currentColor"},s={attributeType:"XML",repeatCount:"indefinite",dur:"2s"};r.push({tag:"path",attributes:M(M({},i),{},{d:"M156.5,447.7l-12.6,29.5c-18.7-9.5-35.9-21.2-51.5-34.9l22.7-22.7C127.6,430.5,141.5,440,156.5,447.7z M40.6,272H8.5 c1.4,21.2,5.4,41.7,11.7,61.1L50,321.2C45.1,305.5,41.8,289,40.6,272z M40.6,240c1.4-18.8,5.2-37,11.1-54.1l-29.5-12.6 C14.7,194.3,10,216.7,8.5,240H40.6z M64.3,156.5c7.8-14.9,17.2-28.8,28.1-41.5L69.7,92.3c-13.7,15.6-25.5,32.8-34.9,51.5 L64.3,156.5z M397,419.6c-13.9,12-29.4,22.3-46.1,30.4l11.9,29.8c20.7-9.9,39.8-22.6,56.9-37.6L397,419.6z M115,92.4 c13.9-12,29.4-22.3,46.1-30.4l-11.9-29.8c-20.7,9.9-39.8,22.6-56.8,37.6L115,92.4z M447.7,355.5c-7.8,14.9-17.2,28.8-28.1,41.5 l22.7,22.7c13.7-15.6,25.5-32.9,34.9-51.5L447.7,355.5z M471.4,272c-1.4,18.8-5.2,37-11.1,54.1l29.5,12.6 c7.5-21.1,12.2-43.5,13.6-66.8H471.4z M321.2,462c-15.7,5-32.2,8.2-49.2,9.4v32.1c21.2-1.4,41.7-5.4,61.1-11.7L321.2,462z M240,471.4c-18.8-1.4-37-5.2-54.1-11.1l-12.6,29.5c21.1,7.5,43.5,12.2,66.8,13.6V471.4z M462,190.8c5,15.7,8.2,32.2,9.4,49.2h32.1 c-1.4-21.2-5.4-41.7-11.7-61.1L462,190.8z M92.4,397c-12-13.9-22.3-29.4-30.4-46.1l-29.8,11.9c9.9,20.7,22.6,39.8,37.6,56.9 L92.4,397z M272,40.6c18.8,1.4,36.9,5.2,54.1,11.1l12.6-29.5C317.7,14.7,295.3,10,272,8.5V40.6z M190.8,50 c15.7-5,32.2-8.2,49.2-9.4V8.5c-21.2,1.4-41.7,5.4-61.1,11.7L190.8,50z M442.3,92.3L419.6,115c12,13.9,22.3,29.4,30.5,46.1 l29.8-11.9C470,128.5,457.3,109.4,442.3,92.3z M397,92.4l22.7-22.7c-15.6-13.7-32.8-25.5-51.5-34.9l-12.6,29.5 C370.4,72.1,384.4,81.5,397,92.4z"})});var o=M(M({},s),{},{attributeName:"opacity"}),a={tag:"circle",attributes:M(M({},i),{},{cx:"256",cy:"364",r:"28"}),children:[]};return n||a.children.push({tag:"animate",attributes:M(M({},s),{},{attributeName:"r",values:"28;14;28;28;14;28;"})},{tag:"animate",attributes:M(M({},o),{},{values:"1;0;1;1;0;1;"})}),r.push(a),r.push({tag:"path",attributes:M(M({},i),{},{opacity:"1",d:"M263.7,312h-16c-6.6,0-12-5.4-12-12c0-71,77.4-63.9,77.4-107.8c0-20-17.8-40.2-57.4-40.2c-29.1,0-44.3,9.6-59.2,28.7 c-3.9,5-11.1,6-16.2,2.4l-13.1-9.2c-5.6-3.9-6.9-11.8-2.6-17.2c21.2-27.2,46.4-44.7,91.2-44.7c52.3,0,97.4,29.8,97.4,80.2 c0,67.6-77.4,63.5-77.4,107.8C275.7,306.6,270.3,312,263.7,312z"}),children:n?[]:[{tag:"animate",attributes:M(M({},o),{},{values:"1;0;0;0;0;1;"})}]}),n||r.push({tag:"path",attributes:M(M({},i),{},{opacity:"0",d:"M232.5,134.5l7,168c0.3,6.4,5.6,11.5,12,11.5h9c6.4,0,11.7-5.1,12-11.5l7-168c0.3-6.8-5.2-12.5-12-12.5h-23 C237.7,122,232.2,127.7,232.5,134.5z"}),children:[{tag:"animate",attributes:M(M({},o),{},{values:"0;0;1;1;0;0;"})}]}),{tag:"g",attributes:{class:"missing"},children:r}}}},gD={hooks:function(){return{parseNodeAttributes:function(n,r){var i=r.getAttribute("data-fa-symbol"),s=i===null?!1:i===""?!0:i;return n.symbol=s,n}}}},vD=[mO,nD,rD,iD,sD,cD,fD,hD,pD,mD,gD];CO(vD,{mixoutsTo:$t});$t.noAuto;$t.config;$t.library;$t.dom;var $d=$t.parse;$t.findIconDefinition;$t.toHtml;var yD=$t.icon;$t.layer;$t.text;$t.counter;var cI={exports:{}},_D="SECRET_DO_NOT_PASS_THIS_OR_YOU_WILL_BE_FIRED",wD=_D,ED=wD;function fI(){}function hI(){}hI.resetWarningCache=fI;var TD=function(){function t(r,i,s,o,a,l){if(l!==ED){var u=new Error("Calling PropTypes validators directly is not supported by the `prop-types` package. Use PropTypes.checkPropTypes() to call them. Read more at http://fb.me/use-check-prop-types");throw u.name="Invariant Violation",u}}t.isRequired=t;function e(){return t}var n={array:t,bigint:t,bool:t,func:t,number:t,object:t,string:t,symbol:t,any:t,arrayOf:e,element:t,elementType:t,instanceOf:e,node:t,objectOf:e,oneOf:e,oneOfType:e,shape:e,exact:e,checkPropTypes:hI,resetWarningCache:fI};return n.PropTypes=n,n};cI.exports=TD();var ID=cI.exports;const ee=_0(ID);function v0(t,e){var n=Object.keys(t);if(Object.getOwnPropertySymbols){var r=Object.getOwnPropertySymbols(t);e&&(r=r.filter(function(i){return Object.getOwnPropertyDescriptor(t,i).enumerable})),n.push.apply(n,r)}return n}function Er(t){for(var e=1;e<arguments.length;e++){var n=arguments[e]!=null?arguments[e]:{};e%2?v0(Object(n),!0).forEach(function(r){ns(t,r,n[r])}):Object.getOwnPropertyDescriptors?Object.defineProperties(t,Object.getOwnPropertyDescriptors(n)):v0(Object(n)).forEach(function(r){Object.defineProperty(t,r,Object.getOwnPropertyDescriptor(n,r))})}return t}function tc(t){"@babel/helpers - typeof";return tc=typeof Symbol=="function"&&typeof Symbol.iterator=="symbol"?function(e){return typeof e}:function(e){return e&&typeof Symbol=="function"&&e.constructor===Symbol&&e!==Symbol.prototype?"symbol":typeof e},tc(t)}function ns(t,e,n){return e in t?Object.defineProperty(t,e,{value:n,enumerable:!0,configurable:!0,writable:!0}):t[e]=n,t}function SD(t,e){if(t==null)return{};var n={},r=Object.keys(t),i,s;for(s=0;s<r.length;s++)i=r[s],!(e.indexOf(i)>=0)&&(n[i]=t[i]);return n}function kD(t,e){if(t==null)return{};var n=SD(t,e),r,i;if(Object.getOwnPropertySymbols){var s=Object.getOwnPropertySymbols(t);for(i=0;i<s.length;i++)r=s[i],!(e.indexOf(r)>=0)&&Object.prototype.propertyIsEnumerable.call(t,r)&&(n[r]=t[r])}return n}function zd(t){return AD(t)||xD(t)||PD(t)||RD()}function AD(t){if(Array.isArray(t))return Bd(t)}function xD(t){if(typeof Symbol<"u"&&t[Symbol.iterator]!=null||t["@@iterator"]!=null)return Array.from(t)}function PD(t,e){if(t){if(typeof t=="string")return Bd(t,e);var n=Object.prototype.toString.call(t).slice(8,-1);if(n==="Object"&&t.constructor&&(n=t.constructor.name),n==="Map"||n==="Set")return Array.from(t);if(n==="Arguments"||/^(?:Ui|I)nt(?:8|16|32)(?:Clamped)?Array$/.test(n))return Bd(t,e)}}function Bd(t,e){(e==null||e>t.length)&&(e=t.length);for(var n=0,r=new Array(e);n<e;n++)r[n]=t[n];return r}function RD(){throw new TypeError(`Invalid attempt to spread non-iterable instance.
In order to be iterable, non-array objects must have a [Symbol.iterator]() method.`)}function CD(t){var e,n=t.beat,r=t.fade,i=t.beatFade,s=t.bounce,o=t.shake,a=t.flash,l=t.spin,u=t.spinPulse,c=t.spinReverse,h=t.pulse,d=t.fixedWidth,y=t.inverse,_=t.border,w=t.listItem,T=t.flip,g=t.size,p=t.rotation,v=t.pull,m=(e={"fa-beat":n,"fa-fade":r,"fa-beat-fade":i,"fa-bounce":s,"fa-shake":o,"fa-flash":a,"fa-spin":l,"fa-spin-reverse":c,"fa-spin-pulse":u,"fa-pulse":h,"fa-fw":d,"fa-inverse":y,"fa-border":_,"fa-li":w,"fa-flip":T===!0,"fa-flip-horizontal":T==="horizontal"||T==="both","fa-flip-vertical":T==="vertical"||T==="both"},ns(e,"fa-".concat(g),typeof g<"u"&&g!==null),ns(e,"fa-rotate-".concat(p),typeof p<"u"&&p!==null&&p!==0),ns(e,"fa-pull-".concat(v),typeof v<"u"&&v!==null),ns(e,"fa-swap-opacity",t.swapOpacity),e);return Object.keys(m).map(function(S){return m[S]?S:null}).filter(function(S){return S})}function ND(t){return t=t-0,t===t}function dI(t){return ND(t)?t:(t=t.replace(/[\-_\s]+(.)?/g,function(e,n){return n?n.toUpperCase():""}),t.substr(0,1).toLowerCase()+t.substr(1))}var bD=["style"];function OD(t){return t.charAt(0).toUpperCase()+t.slice(1)}function DD(t){return t.split(";").map(function(e){return e.trim()}).filter(function(e){return e}).reduce(function(e,n){var r=n.indexOf(":"),i=dI(n.slice(0,r)),s=n.slice(r+1).trim();return i.startsWith("webkit")?e[OD(i)]=s:e[i]=s,e},{})}function pI(t,e){var n=arguments.length>2&&arguments[2]!==void 0?arguments[2]:{};if(typeof e=="string")return e;var r=(e.children||[]).map(function(l){return pI(t,l)}),i=Object.keys(e.attributes||{}).reduce(function(l,u){var c=e.attributes[u];switch(u){case"class":l.attrs.className=c,delete e.attributes.class;break;case"style":l.attrs.style=DD(c);break;default:u.indexOf("aria-")===0||u.indexOf("data-")===0?l.attrs[u.toLowerCase()]=c:l.attrs[dI(u)]=c}return l},{attrs:{}}),s=n.style,o=s===void 0?{}:s,a=kD(n,bD);return i.attrs.style=Er(Er({},i.attrs.style),o),t.apply(void 0,[e.tag,Er(Er({},i.attrs),a)].concat(zd(r)))}var mI=!1;try{mI=!0}catch{}function MD(){if(!mI&&console&&typeof console.error=="function"){var t;(t=console).error.apply(t,arguments)}}function y0(t){if(t&&tc(t)==="object"&&t.prefix&&t.iconName&&t.icon)return t;if($d.icon)return $d.icon(t);if(t===null)return null;if(t&&tc(t)==="object"&&t.prefix&&t.iconName)return t;if(Array.isArray(t)&&t.length===2)return{prefix:t[0],iconName:t[1]};if(typeof t=="string")return{prefix:"fas",iconName:t}}function ih(t,e){return Array.isArray(e)&&e.length>0||!Array.isArray(e)&&e?ns({},t,e):{}}var Qe=rc.forwardRef(function(t,e){var n=t.icon,r=t.mask,i=t.symbol,s=t.className,o=t.title,a=t.titleId,l=t.maskId,u=y0(n),c=ih("classes",[].concat(zd(CD(t)),zd(s.split(" ")))),h=ih("transform",typeof t.transform=="string"?$d.transform(t.transform):t.transform),d=ih("mask",y0(r)),y=yD(u,Er(Er(Er(Er({},c),h),d),{},{symbol:i,title:o,titleId:a,maskId:l}));if(!y)return MD("Could not find icon",u),null;var _=y.abstract,w={ref:e};return Object.keys(t).forEach(function(T){Qe.defaultProps.hasOwnProperty(T)||(w[T]=t[T])}),LD(_[0],w)});Qe.displayName="FontAwesomeIcon";Qe.propTypes={beat:ee.bool,border:ee.bool,beatFade:ee.bool,bounce:ee.bool,className:ee.string,fade:ee.bool,flash:ee.bool,mask:ee.oneOfType([ee.object,ee.array,ee.string]),maskId:ee.string,fixedWidth:ee.bool,inverse:ee.bool,flip:ee.oneOf([!0,!1,"horizontal","vertical","both"]),icon:ee.oneOfType([ee.object,ee.array,ee.string]),listItem:ee.bool,pull:ee.oneOf(["right","left"]),pulse:ee.bool,rotation:ee.oneOf([0,90,180,270]),shake:ee.bool,size:ee.oneOf(["2xs","xs","sm","lg","xl","2xl","1x","2x","3x","4x","5x","6x","7x","8x","9x","10x"]),spin:ee.bool,spinPulse:ee.bool,spinReverse:ee.bool,symbol:ee.oneOfType([ee.bool,ee.string]),title:ee.string,titleId:ee.string,transform:ee.oneOfType([ee.string,ee.object]),swapOpacity:ee.bool};Qe.defaultProps={border:!1,className:"",mask:null,maskId:null,fixedWidth:!1,inverse:!1,flip:!1,icon:null,listItem:!1,pull:null,pulse:!1,rotation:null,size:null,spin:!1,spinPulse:!1,spinReverse:!1,beat:!1,fade:!1,beatFade:!1,bounce:!1,shake:!1,symbol:!1,title:"",titleId:null,transform:null,swapOpacity:!1};var LD=pI.bind(null,rc.createElement),jD={prefix:"fas",iconName:"address-book",icon:[512,512,[62138,"contact-book"],"f2b9","M96 0C60.7 0 32 28.7 32 64V448c0 35.3 28.7 64 64 64H384c35.3 0 64-28.7 64-64V64c0-35.3-28.7-64-64-64H96zM208 288h64c44.2 0 80 35.8 80 80c0 8.8-7.2 16-16 16H144c-8.8 0-16-7.2-16-16c0-44.2 35.8-80 80-80zm-32-96a64 64 0 1 1 128 0 64 64 0 1 1 -128 0zM512 80c0-8.8-7.2-16-16-16s-16 7.2-16 16v64c0 8.8 7.2 16 16 16s16-7.2 16-16V80zM496 192c-8.8 0-16 7.2-16 16v64c0 8.8 7.2 16 16 16s16-7.2 16-16V208c0-8.8-7.2-16-16-16zm16 144c0-8.8-7.2-16-16-16s-16 7.2-16 16v64c0 8.8 7.2 16 16 16s16-7.2 16-16V336z"]},gI={prefix:"fas",iconName:"user",icon:[448,512,[128100,62144],"f007","M224 256A128 128 0 1 0 224 0a128 128 0 1 0 0 256zm-45.7 48C79.8 304 0 383.8 0 482.3C0 498.7 13.3 512 29.7 512H418.3c16.4 0 29.7-13.3 29.7-29.7C448 383.8 368.2 304 269.7 304H178.3z"]},VD={prefix:"fas",iconName:"user-group",icon:[640,512,[128101,"user-friends"],"f500","M96 128a128 128 0 1 1 256 0A128 128 0 1 1 96 128zM0 482.3C0 383.8 79.8 304 178.3 304h91.4C368.2 304 448 383.8 448 482.3c0 16.4-13.3 29.7-29.7 29.7H29.7C13.3 512 0 498.7 0 482.3zM609.3 512H471.4c5.4-9.4 8.6-20.3 8.6-32v-8c0-60.7-27.1-115.2-69.8-151.8c2.4-.1 4.7-.2 7.1-.2h61.4C567.8 320 640 392.2 640 481.3c0 17-13.8 30.7-30.7 30.7zM432 256c-31 0-59-12.6-79.3-32.9C372.4 196.5 384 163.6 384 128c0-26.8-6.6-52.1-18.3-74.3C384.3 40.1 407.2 32 432 32c61.9 0 112 50.1 112 112s-50.1 112-112 112z"]},FD={prefix:"fas",iconName:"file-image",icon:[384,512,[128443],"f1c5","M64 0C28.7 0 0 28.7 0 64V448c0 35.3 28.7 64 64 64H320c35.3 0 64-28.7 64-64V160H256c-17.7 0-32-14.3-32-32V0H64zM256 0V128H384L256 0zM64 256a32 32 0 1 1 64 0 32 32 0 1 1 -64 0zm152 32c5.3 0 10.2 2.6 13.2 6.9l88 128c3.4 4.9 3.7 11.3 1 16.5s-8.2 8.6-14.2 8.6H216 176 128 80c-5.8 0-11.1-3.1-13.9-8.1s-2.8-11.2 .2-16.1l48-80c2.9-4.8 8.1-7.8 13.7-7.8s10.8 2.9 13.7 7.8l12.8 21.4 48.3-70.2c3-4.3 7.9-6.9 13.2-6.9z"]},UD={prefix:"fas",iconName:"comment-dots",icon:[512,512,[128172,62075,"commenting"],"f4ad","M256 448c141.4 0 256-93.1 256-208S397.4 32 256 32S0 125.1 0 240c0 45.1 17.7 86.8 47.7 120.9c-1.9 24.5-11.4 46.3-21.4 62.9c-5.5 9.2-11.1 16.6-15.2 21.6c-2.1 2.5-3.7 4.4-4.9 5.7c-.6 .6-1 1.1-1.3 1.4l-.3 .3 0 0 0 0 0 0 0 0c-4.6 4.6-5.9 11.4-3.4 17.4c2.5 6 8.3 9.9 14.8 9.9c28.7 0 57.6-8.9 81.6-19.3c22.9-10 42.4-21.9 54.3-30.6c31.8 11.5 67 17.9 104.1 17.9zM128 208a32 32 0 1 1 0 64 32 32 0 1 1 0-64zm128 0a32 32 0 1 1 0 64 32 32 0 1 1 0-64zm96 32a32 32 0 1 1 64 0 32 32 0 1 1 -64 0z"]},$D={prefix:"fas",iconName:"phone",icon:[512,512,[128222,128379],"f095","M164.9 24.6c-7.7-18.6-28-28.5-47.4-23.2l-88 24C12.1 30.2 0 46 0 64C0 311.4 200.6 512 448 512c18 0 33.8-12.1 38.6-29.5l24-88c5.3-19.4-4.6-39.7-23.2-47.4l-96-40c-16.3-6.8-35.2-2.1-46.3 11.6L304.7 368C234.3 334.7 177.3 277.7 144 207.3L193.3 167c13.7-11.2 18.4-30 11.6-46.3l-40-96z"]},zD={prefix:"fas",iconName:"paperclip",icon:[448,512,[128206],"f0c6","M364.2 83.8c-24.4-24.4-64-24.4-88.4 0l-184 184c-42.1 42.1-42.1 110.3 0 152.4s110.3 42.1 152.4 0l152-152c10.9-10.9 28.7-10.9 39.6 0s10.9 28.7 0 39.6l-152 152c-64 64-167.6 64-231.6 0s-64-167.6 0-231.6l184-184c46.3-46.3 121.3-46.3 167.6 0s46.3 121.3 0 167.6l-176 176c-28.6 28.6-75 28.6-103.6 0s-28.6-75 0-103.6l144-144c10.9-10.9 28.7-10.9 39.6 0s10.9 28.7 0 39.6l-144 144c-6.7 6.7-6.7 17.7 0 24.4s17.7 6.7 24.4 0l176-176c24.4-24.4 24.4-64 0-88.4z"]},BD={prefix:"fas",iconName:"gear",icon:[512,512,[9881,"cog"],"f013","M495.9 166.6c3.2 8.7 .5 18.4-6.4 24.6l-43.3 39.4c1.1 8.3 1.7 16.8 1.7 25.4s-.6 17.1-1.7 25.4l43.3 39.4c6.9 6.2 9.6 15.9 6.4 24.6c-4.4 11.9-9.7 23.3-15.8 34.3l-4.7 8.1c-6.6 11-14 21.4-22.1 31.2c-5.9 7.2-15.7 9.6-24.5 6.8l-55.7-17.7c-13.4 10.3-28.2 18.9-44 25.4l-12.5 57.1c-2 9.1-9 16.3-18.2 17.8c-13.8 2.3-28 3.5-42.5 3.5s-28.7-1.2-42.5-3.5c-9.2-1.5-16.2-8.7-18.2-17.8l-12.5-57.1c-15.8-6.5-30.6-15.1-44-25.4L83.1 425.9c-8.8 2.8-18.6 .3-24.5-6.8c-8.1-9.8-15.5-20.2-22.1-31.2l-4.7-8.1c-6.1-11-11.4-22.4-15.8-34.3c-3.2-8.7-.5-18.4 6.4-24.6l43.3-39.4C64.6 273.1 64 264.6 64 256s.6-17.1 1.7-25.4L22.4 191.2c-6.9-6.2-9.6-15.9-6.4-24.6c4.4-11.9 9.7-23.3 15.8-34.3l4.7-8.1c6.6-11 14-21.4 22.1-31.2c5.9-7.2 15.7-9.6 24.5-6.8l55.7 17.7c13.4-10.3 28.2-18.9 44-25.4l12.5-57.1c2-9.1 9-16.3 18.2-17.8C227.3 1.2 241.5 0 256 0s28.7 1.2 42.5 3.5c9.2 1.5 16.2 8.7 18.2 17.8l12.5 57.1c15.8 6.5 30.6 15.1 44 25.4l55.7-17.7c8.8-2.8 18.6-.3 24.5 6.8c8.1 9.8 15.5 20.2 22.1 31.2l4.7 8.1c6.1 11 11.4 22.4 15.8 34.3zM256 336a80 80 0 1 0 0-160 80 80 0 1 0 0 160z"]},HD={prefix:"fas",iconName:"ellipsis",icon:[448,512,["ellipsis-h"],"f141","M8 256a56 56 0 1 1 112 0A56 56 0 1 1 8 256zm160 0a56 56 0 1 1 112 0 56 56 0 1 1 -112 0zm216-56a56 56 0 1 1 0 112 56 56 0 1 1 0-112z"]},vI={prefix:"fas",iconName:"magnifying-glass",icon:[512,512,[128269,"search"],"f002","M416 208c0 45.9-14.9 88.3-40 122.7L502.6 457.4c12.5 12.5 12.5 32.8 0 45.3s-32.8 12.5-45.3 0L330.7 376c-34.4 25.2-76.8 40-122.7 40C93.1 416 0 322.9 0 208S93.1 0 208 0S416 93.1 416 208zM208 352a144 144 0 1 0 0-288 144 144 0 1 0 0 288z"]},WD={prefix:"fas",iconName:"circle-half-stroke",icon:[512,512,[9680,"adjust"],"f042","M448 256c0-106-86-192-192-192V448c106 0 192-86 192-192zM0 256a256 256 0 1 1 512 0A256 256 0 1 1 0 256z"]},KD={prefix:"fas",iconName:"video",icon:[576,512,["video-camera"],"f03d","M0 128C0 92.7 28.7 64 64 64H320c35.3 0 64 28.7 64 64V384c0 35.3-28.7 64-64 64H64c-35.3 0-64-28.7-64-64V128zM559.1 99.8c10.4 5.6 16.9 16.4 16.9 28.2V384c0 11.8-6.5 22.6-16.9 28.2s-23 5-32.9-1.6l-96-64L416 337.1V320 192 174.9l14.2-9.5 96-64c9.8-6.5 22.4-7.2 32.9-1.6z"]};const GD=()=>f.jsxs("div",{className:"search",children:[f.jsx("div",{className:"header",children:f.jsx("h2",{children:"Chats"})}),f.jsxs("div",{className:"search-form",children:[f.jsx("input",{type:"text",placeholder:"Search messages or users"}),f.jsx("div",{className:"search-icon",children:f.jsx(Qe,{icon:vI})})]})]}),qD=()=>f.jsxs("div",{className:"chat-layout",children:[f.jsx(GD,{}),f.jsx(F4,{})]}),QD=()=>f.jsx("div",{className:"group-container",children:f.jsx("h2",{children:"Group chats"})}),YD=()=>f.jsx("div",{children:f.jsx("h2",{children:"Contacts"})}),XD=()=>f.jsx("div",{children:f.jsx("h2",{children:"DarkMode yall"})}),yI="/assets/avatar-1-7bb6ff2f.jpg",_I=()=>f.jsxs("div",{className:"user-available",children:[f.jsx("img",{src:yI,alt:"Profile pic of current user"}),f.jsx("p",{children:"user name"}),f.jsx("p",{children:"user availability"})]}),JD=()=>f.jsxs("div",{className:"profile-component",children:[f.jsx("div",{className:"user",children:f.jsx("h2",{children:"My Profile"})}),f.jsx(_I,{})]}),ZD="/assets/avatar-4-b0db35be.jpg",eM=()=>f.jsxs("div",{className:"chat-box",children:[f.jsx("div",{className:"message my-message",children:f.jsxs("p",{children:["Wassssssssssup",f.jsx("br",{}),f.jsx("span",{children:"Just Now"})]})}),f.jsx("div",{className:"message frnd-message",children:f.jsxs("p",{children:["yoyo, what's up?",f.jsxs("span",{children:[f.jsx("br",{}),"Just Now"]})]})}),f.jsx("div",{className:"message my-message",children:f.jsxs("p",{children:["Not much, you?",f.jsxs("span",{children:[f.jsx("br",{}),"Just Now"]})]})}),f.jsx("div",{className:"message frnd-message",children:f.jsxs("p",{children:["nuttin",f.jsxs("span",{children:[f.jsx("br",{}),"Just Now"]})]})}),f.jsx("div",{className:"message my-message",children:f.jsxs("p",{children:["Lorem ipsum dolor sit amet consectetur adipisicing elit. Officiis adipisci ipsam a saepe quod perspiciatis?",f.jsxs("span",{children:[f.jsx("br",{}),"Just Now"]})]})}),f.jsx("div",{className:"message frnd-message",children:f.jsxs("p",{children:["sure",f.jsxs("span",{children:[f.jsx("br",{}),"Just Now"]})]})}),f.jsx("div",{className:"message my-message",children:f.jsxs("p",{children:["Meet in 16?",f.jsxs("span",{children:[f.jsx("br",{}),"Just Now"]})]})}),f.jsx("div",{className:"message frnd-message",children:f.jsxs("p",{children:["Lorem ipsum dolor sit amet consectetur, adipisicing elit. Laborum dolorum corrupti fugit rem?",f.jsxs("span",{children:[f.jsx("br",{}),"Just Now"]})]})}),f.jsx("div",{className:"message my-message",children:f.jsxs("p",{children:["sounds good ma man",f.jsxs("span",{children:[f.jsx("br",{}),"Just Now"]})]})}),f.jsx("div",{className:"message frnd-message",children:f.jsxs("p",{children:["perfect",f.jsxs("span",{children:[f.jsx("br",{}),"Just Now"]})]})}),f.jsx("div",{className:"message my-message",children:f.jsxs("p",{children:["See u later",f.jsxs("span",{children:[f.jsx("br",{}),"Just Now"]})]})}),f.jsx("div",{className:"message frnd-message",children:f.jsxs("p",{children:["counting on it",f.jsxs("span",{children:[f.jsx("br",{}),"Just Now"]})]})}),f.jsx("div",{className:"message my-message",children:f.jsxs("p",{children:["See u later",f.jsxs("span",{children:[f.jsx("br",{}),"Just Now"]})]})}),f.jsx("div",{className:"message frnd-message",children:f.jsxs("p",{children:["counting on it",f.jsxs("span",{children:[f.jsx("br",{}),"Just Now"]})]})}),f.jsx("div",{className:"message my-message",children:f.jsxs("p",{children:["See u later",f.jsxs("span",{children:[f.jsx("br",{}),"Just Now"]})]})}),f.jsx("div",{className:"message frnd-message",children:f.jsxs("p",{children:["counting on it",f.jsxs("span",{children:[f.jsx("br",{}),"Just Now"]})]})}),f.jsx("div",{className:"message my-message",children:f.jsxs("p",{children:["See u later",f.jsxs("span",{children:[f.jsx("br",{}),"Just Now"]})]})}),f.jsx("div",{className:"message frnd-message",children:f.jsxs("p",{children:["counting on it",f.jsxs("span",{children:[f.jsx("br",{}),"Just Now"]})]})})]}),tM=()=>f.jsx("div",{className:"messages",children:f.jsx(eM,{})}),nM=()=>f.jsxs("div",{className:"userInput",children:[f.jsx("input",{type:"text",placeholder:"Enter Message..."}),f.jsxs("div",{className:"send",children:[f.jsx(Qe,{icon:zD}),f.jsx("input",{type:"file",style:{display:"none"}}),f.jsx("label",{htmlFor:"file",children:f.jsx(Qe,{icon:FD})}),f.jsx("button",{children:"Send"})]})]}),rM=()=>f.jsxs("div",{className:"chat",children:[f.jsxs("header",{className:"chat-info",children:[f.jsxs("div",{className:"contact-content",children:[f.jsx("div",{className:"contact-img",children:f.jsx("img",{src:ZD,alt:"Pic of user you are chatting with"})}),f.jsxs("div",{className:"contact-info",children:[f.jsx("h4",{children:"Doris Brown"}),f.jsx("span",{children:"online"})]})]}),f.jsxs("ul",{className:"chat-icons",children:[f.jsx("li",{children:f.jsx(Qe,{icon:vI})}),f.jsx("li",{children:f.jsx(Qe,{icon:$D})}),f.jsx("li",{children:f.jsx(Qe,{icon:KD})}),f.jsx("li",{children:f.jsx(Qe,{icon:gI})}),f.jsx("li",{children:f.jsx(Qe,{icon:HD})})]})]}),f.jsx(tM,{}),f.jsx(nM,{})]}),iM=()=>f.jsx("div",{className:"root-layout",children:f.jsxs("div",{className:"container",children:[f.jsxs("div",{className:"menu-container",children:[f.jsxs("nav",{className:"nav-container",children:[f.jsx("div",{children:f.jsx("h1",{children:f.jsx(Bp,{to:"/",children:"CV"})})}),f.jsxs("ul",{className:"navLink-container",children:[f.jsx("li",{children:f.jsx(tn,{to:"profile",children:f.jsx(Qe,{icon:gI,alt:"User"})})}),f.jsx("li",{children:f.jsx(tn,{to:"chats",children:f.jsx(Qe,{icon:UD,alt:"Chats"})})}),f.jsx("li",{children:f.jsx(tn,{to:"groupchats",children:f.jsx(Qe,{icon:VD,alt:"Group Chats"})})}),f.jsx("li",{children:f.jsx(tn,{to:"contacts",children:f.jsx(Qe,{icon:jD,alt:"Contact List"})})}),f.jsx("li",{children:f.jsx(tn,{to:"settings",children:f.jsx(Qe,{icon:BD,alt:"Settings"})})})]}),f.jsxs("ul",{className:"nav-bottom",children:[f.jsx("li",{children:f.jsx(tn,{to:"darkmode",children:f.jsx(Qe,{icon:WD,alt:"Dark Mode button"})})}),f.jsx("li",{className:"current-user",children:f.jsx(tn,{to:"about",children:f.jsx("img",{src:yI,alt:""})})})]})]}),f.jsx("main",{className:"page-components",children:f.jsx(W_,{})})]}),f.jsx("section",{className:"main-chat",children:f.jsx(rM,{})})]})}),sM=()=>f.jsxs("div",{className:"settings-layout",children:[f.jsx("h2",{children:"Settings"}),f.jsx(_I,{}),f.jsxs("nav",{children:[f.jsx(tn,{to:"personalInfo",children:"Personal Info"}),f.jsx(tn,{to:"privacy",children:"Privacy"}),f.jsx(tn,{to:"security",children:"Security"}),f.jsx(tn,{to:"help",children:"Help"})]}),f.jsx(W_,{})]}),oM=()=>f.jsxs("div",{className:"help",children:[f.jsx("h3",{children:"Frequently Asked Questions"}),f.jsx("p",{children:"a bunch of random questions and answers..."})]});function aM(){return f.jsxs("div",{className:"error-page",children:[f.jsx("h2",{children:"Page not found!"}),f.jsx("p",{children:"Lorem ipsum dolor sit amet consectetur, adipisicing elit. Deleniti minus deserunt natus ullam facilis. Quo recusandae possimus maiores enim aliquam, repellat suscipit, blanditiis repellendus consequatur sint doloremque itaque explicabo libero."}),f.jsxs("p",{children:["Go to the ",f.jsx(Bp,{to:"/",children:"Homepage"}),"."]})]})}const lM=gA(rd(f.jsxs(Ke,{children:[f.jsx(Ke,{path:"register",element:f.jsx(M4,{})}),f.jsx(Ke,{path:"login",element:f.jsx(L4,{})}),f.jsxs(Ke,{path:"/",element:f.jsx(iM,{}),children:[f.jsx(Ke,{index:!0,element:f.jsx(j4,{})}),f.jsx(Ke,{path:"profile",element:f.jsx(JD,{})}),f.jsx(Ke,{path:"chats",element:f.jsx(qD,{})}),f.jsx(Ke,{path:"groupchats",element:f.jsx(QD,{})}),f.jsx(Ke,{path:"contacts",element:f.jsx(YD,{})}),f.jsxs(Ke,{path:"settings",element:f.jsx(sM,{}),children:[f.jsx(Ke,{path:"personalInfo"}),f.jsx(Ke,{path:"privacy"}),f.jsx(Ke,{path:"security"}),f.jsx(Ke,{path:"help",element:f.jsx(oM,{})})]}),f.jsx(Ke,{path:"darkmode",element:f.jsx(XD,{})}),f.jsx(Ke,{path:"about",element:f.jsx(V4,{})}),f.jsx(Ke,{path:"*",element:f.jsx(aM,{})})]})]})));function uM(){return f.jsx(lA,{router:lM})}sh.createRoot(document.getElementById("root")).render(f.jsx(rc.StrictMode,{children:f.jsx(uM,{})}));