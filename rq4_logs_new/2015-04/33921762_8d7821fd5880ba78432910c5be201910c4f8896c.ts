/*
 * This file is derived from oauth project ( https://code.google.com/p/oauth/ )
 * and is modified for oauth-typescript project.
 *
 * Copyright 2008 Netflix, Inc.
 * Copyright 2015 NOBUOKA Yu.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

/// <reference path=".\sha1.ts" />

/* Here's some JavaScript software for implementing OAuth.

   This isn't as useful as you might hope.  OAuth is based around
   allowing tools and websites to talk to each other.  However,
   JavaScript running in web browsers is hampered by security
   restrictions that prevent code running on one website from
   accessing data stored or served on another.

   Before you start hacking, make sure you understand the limitations
   posed by cross-domain XMLHttpRequest.

   On the bright side, some platforms use JavaScript as their
   language, but enable the programmer to access other web sites.
   Examples include Google Gadgets, and Microsoft Vista Sidebar.
   For those platforms, this library should come in handy.
*/

// The HMAC-SHA1 signature method calls b64_hmac_sha1, defined by
// http://pajhome.org.uk/crypt/md5/sha1.js

/* An OAuth message is represented as an object like this:
   {method: "GET", action: "http://server.com/path", parameters: ...}

   The parameters may be either a map {name: value, name2: value2}
   or an Array of name-value pairs [[name, value], [name2, value2]].
   The latter representation is more powerful: it supports parameters
   in a specific sequence, or several parameters with the same name;
   for example [["a", 1], ["b", 2], ["a", 3]].

   Parameter names and values are NOT percent-encoded in an object.
   They must be encoded before transmission and decoded after reception.
   For example, this message object:
   {method: "GET", action: "http://server/path", parameters: {p: "x y"}}
   ... can be transmitted as an HTTP request that begins:
   GET /path?p=x%20y HTTP/1.0
   (This isn't a valid OAuth request, since it lacks a signature etc.)
   Note that the object "x y" is transmitted as x%20y.  To encode
   parameters, you can call OAuth.addToURL, OAuth.formEncode or
   OAuth.getAuthorization.

   This message object model harmonizes with the browser object model for
   input elements of an form, whose value property isn't percent encoded.
   The browser encodes each value before transmitting it. For example,
   see consumer.setInputs in example/consumer.js.
 */

/* This script needs to know what time it is. By default, it uses the local
   clock (new Date), which is apt to be inaccurate in browsers. To do
   better, you can load this script from a URL whose query string contains
   an oauth_timestamp parameter, whose value is a current Unix timestamp.
   For example, when generating the enclosing document using PHP:

   <script src="oauth.js?oauth_timestamp=<?=time()?>" ...

   Another option is to call OAuth.correctTimestamp with a Unix timestamp.
 */

module OAuth {

    export type ParameterList =[string, string][];
    export type ParameterMap = { [key: string]: string; };
    export type ParameterListOrMap = ParameterList|ParameterMap;
    
    export interface Message {
        action: string;
        method: string;
        parameters: ParameterListOrMap;
    }

    /* SignatureMethod expects an accessor object to be like this:
       {tokenSecret: "lakjsdflkj...", consumerSecret: "QOUEWRI..", accessorSecret: "xcmvzc..."}
       The accessorSecret property is optional.
    */
    export interface Accessor {
        consumerKey: string;
        consumerSecret: string;
        accessorSecret?: string;
        token: string;
        tokenSecret: string;
    }

    export function percentEncode(s: string[]): string;
    export function percentEncode(s: string): string;
    export function percentEncode(s: any): string {
        if (s == null) {
            return "";
        }
        if (s instanceof Array) {
            var e = "";
            for (var i = 0; i < s.length; ++s) {
                if (e != "") e += '&';
                e += OAuth.percentEncode(s[i]);
            }
            return e;
        }
        s = encodeURIComponent(s);
        // Now replace the values which encodeURIComponent doesn't do
        // encodeURIComponent ignores: - _ . ! ~ * ' ( )
        // OAuth dictates the only ones you can ignore are: - _ . ~
        // Source: http://developer.mozilla.org/en/docs/Core_JavaScript_1.5_Reference:Global_Functions:encodeURIComponent
        s = s.replace(/\!/g, "%21");
        s = s.replace(/\*/g, "%2A");
        s = s.replace(/\'/g, "%27");
        s = s.replace(/\(/g, "%28");
        s = s.replace(/\)/g, "%29");
        return s;
    }

    export function decodePercent(s: string): string {
        if (s != null) {
            // Handle application/x-www-form-urlencoded, which is defined by
            // http://www.w3.org/TR/html4/interact/forms.html#h-17.13.4.1
            s = s.replace(/\+/g, " ");
        }
        return decodeURIComponent(s);
    }

    /** Convert the given parameters to an Array of name-value pairs. */
    export function getParameterList(parameters: ParameterListOrMap): ParameterList;
    export function getParameterList(parameters: string): ParameterList;
    export function getParameterList(parameters: any): ParameterList {
        if (parameters == null) {
            return [];
        }
        if (typeof parameters != "object") {
            return OAuth.decodeForm(parameters + "");
        }
        if (parameters instanceof Array) {
            return parameters;
        }
        var list = <[string, string][]>[];
        for (var p in parameters) {
            list.push([p, parameters[p]]);
        }
        return list;
    }

    /** Convert the given parameters to a map from name to value. */
    export function getParameterMap(parameters: ParameterListOrMap): ParameterMap;
    export function getParameterMap(parameters: string): ParameterMap;
    export function getParameterMap(parameters: any): ParameterMap {
        if (parameters == null) {
            return {};
        }
        if (typeof parameters != "object") {
            return OAuth.getParameterMap(OAuth.decodeForm(parameters + ""));
        }
        if (parameters instanceof Array) {
            var map = <{ [key: string]: string; }>{};
            for (var p = 0; p < parameters.length; ++p) {
                var key = parameters[p][0];
                if (map[key] === undefined) { // first value wins
                    map[key] = parameters[p][1];
                }
            }
            return map;
        }
        return parameters;
    }

    export function getParameter(parameters: ParameterListOrMap, name: string): string;
    export function getParameter(parameters: string, name: string): string;
    export function getParameter(parameters: any, name: string): string {
        if (parameters instanceof Array) {
            for (var p = 0; p < parameters.length; ++p) {
                if (parameters[p][0] == name) {
                    return parameters[p][1]; // first value wins
                }
            }
        } else {
            return OAuth.getParameterMap(parameters)[name];
        }
        return null;
    }

    export function formEncode(parameters: ParameterListOrMap): string {
        var form = "";
        var list = OAuth.getParameterList(parameters);
        for (var p = 0; p < list.length; ++p) {
            var value = list[p][1];
            if (value == null) value = "";
            if (form != "") form += '&';
            form += OAuth.percentEncode(list[p][0])
            + '=' + OAuth.percentEncode(value);
        }
        return form;
    }

    export function decodeForm(form: string): ParameterList {
        var list = <[string, string][]>[];
        var nvps = form.split('&');
        for (var n = 0; n < nvps.length; ++n) {
            var nvp = nvps[n];
            if (nvp == "") {
                continue;
            }
            var equals = nvp.indexOf('=');
            var name: string;
            var value: string;
            if (equals < 0) {
                name = OAuth.decodePercent(nvp);
                value = null;
            } else {
                name = OAuth.decodePercent(nvp.substring(0, equals));
                value = OAuth.decodePercent(nvp.substring(equals + 1));
            }
            list.push([name, value]);
        }
        return list;
    }

    export function setParameter(message: Message, name: string, value: string): void {
        var pp = message.parameters;
        if (pp instanceof Array) {
            var parameters = <[string, string][]>pp;
            for (var p = 0; p < parameters.length; ++p) {
                if (parameters[p][0] == name) {
                    if (value === undefined) {
                        parameters.splice(p, 1);
                    } else {
                        parameters[p][1] = value;
                        value = undefined;
                    }
                }
            }
            if (value !== undefined) {
                parameters.push([name, value]);
            }
        } else {
            var parameters2 = <{ [key: string]: string; }>pp;
            parameters2 = OAuth.getParameterMap(parameters2);
            parameters2[name] = value;
            message.parameters = parameters2;
        }
    }

    export function setParameters(message: Message, parameters: ParameterListOrMap): void;
    export function setParameters(message: Message, parameters: string): void;
    export function setParameters(message: Message, parameters: any): void {
        var list = OAuth.getParameterList(parameters);
        for (var i = 0; i < list.length; ++i) {
            OAuth.setParameter(message, list[i][0], list[i][1]);
        }
    }
    
    /** Fill in parameters to help construct a request message.
        This function doesn't fill in every parameter.
        The accessor object should be like:
        {consumerKey:'foo', consumerSecret:'bar', accessorSecret:'nurn', token:'krelm', tokenSecret:'blah'}
        The accessorSecret property is optional.
     */
    export function completeRequest(message: Message, accessor: Accessor) {
        if (message.method == null) {
            message.method = "GET";
        }
        var map = OAuth.getParameterMap(message.parameters);
        if (map["oauth_consumer_key"] == null) {
            OAuth.setParameter(message, "oauth_consumer_key", accessor.consumerKey || "");
        }
        if (map["oauth_token"] == null && accessor.token != null) {
            OAuth.setParameter(message, "oauth_token", accessor.token);
        }
        if (map["oauth_version"] == null) {
            OAuth.setParameter(message, "oauth_version", "1.0");
        }
        if (map["oauth_timestamp"] == null) {
            OAuth.setParameter(message, "oauth_timestamp", OAuth.timestamp() + "");
        }
        if (map["oauth_nonce"] == null) {
            OAuth.setParameter(message, "oauth_nonce", OAuth.nonce(6));
        }
        OAuth.SignatureMethod.sign(message, accessor);
    }

    export function setTimestampAndNonce(message: Message) {
        OAuth.setParameter(message, "oauth_timestamp", OAuth.timestamp() + "");
        OAuth.setParameter(message, "oauth_nonce", OAuth.nonce(6));
    }

    export function addToURL(url: string, parameters: ParameterListOrMap): string {
        var newURL = url;
        if (parameters != null) {
            var toAdd = OAuth.formEncode(parameters);
            if (toAdd.length > 0) {
                var q = url.indexOf('?');
                if (q < 0) newURL += '?';
                else newURL += '&';
                newURL += toAdd;
            }
        }
        return newURL;
    }

    /** Construct the value of the Authorization header for an HTTP request. */
    export function getAuthorizationHeader(realm: string, parameters: ParameterListOrMap): string;
    export function getAuthorizationHeader(realm: string, parameters: string): string;
    export function getAuthorizationHeader(realm: string, parameters: any): string {
        var header = 'OAuth realm="' + OAuth.percentEncode(realm) + '"';
        var list = OAuth.getParameterList(parameters);
        for (var p = 0; p < list.length; ++p) {
            var parameter = list[p];
            var name = parameter[0];
            if (name.indexOf("oauth_") == 0) {
                header += ',' + OAuth.percentEncode(name) + '="' + OAuth.percentEncode(parameter[1]) + '"';
            }
        }
        return header;
    }
    
    /** Correct the time using a parameter from the URL from which the last script was loaded. */
    export function correctTimestampFromSrc(parameterName?: string): void {
        parameterName = parameterName || "oauth_timestamp";
        var scripts = document.getElementsByTagName('script');
        if (scripts == null || !scripts.length) return;
        var src = scripts[scripts.length - 1].src;
        if (!src) return;
        var q = src.indexOf("?");
        if (q < 0) return;
        var parameters = OAuth.getParameterMap(OAuth.decodeForm(src.substring(q + 1)));
        var t = parameters[parameterName];
        if (t == null) return;
        OAuth.correctTimestamp(+t);
    }

    /** Generate timestamps starting with the given value. */
    export function correctTimestamp(timestamp: number): void {
        OAuth.timeCorrectionMsec = (timestamp * 1000) - (new Date()).getTime();
    }
    
    /** The difference between the correct time and my clock. */
    export var timeCorrectionMsec = 0;

    export function timestamp() {
        var t = (new Date()).getTime() + OAuth.timeCorrectionMsec;
        return Math.floor(t / 1000);
    }

    var NONCE_CHARS = "0123456789ABCDEFGHIJKLMNOPQRSTUVWXTZabcdefghiklmnopqrstuvwxyz";
    export function nonce(length: number) {
        var chars = NONCE_CHARS;
        var result = "";
        for (var i = 0; i < length; ++i) {
            var rnum = Math.floor(Math.random() * chars.length);
            result += chars.substring(rnum, rnum + 1);
        }
        return result;
    }

    export interface Uri {
        source: string;
        protocol: string;
        authority: string;
        userInfo: string;
        user: string;
        password: string;
        host: string;
        port: string;
        relative: string;
        path: string;
        directory: string;
        file: string;
        query: string;
        anchor: string;
    }

    export class SignatureMethod {
        getSignature(baseString: string): string {
            throw new Error("Abstract method");
        }

        key: string;

        /** Add a signature to the message. */
        sign(message: Message) {
            var baseString = OAuth.SignatureMethod.getBaseString(message);
            var signature = this.getSignature(baseString);
            OAuth.setParameter(message, "oauth_signature", signature);
            return signature; // just in case someone's interested
        }

        /** Set the key string for signing. */
        initialize(name: string, accessor: Accessor) {
            var consumerSecret: string;
            if (accessor.accessorSecret != null
                && name.length > 9
                && name.substring(name.length - 9) == "-Accessor") {
                consumerSecret = accessor.accessorSecret;
            } else {
                consumerSecret = accessor.consumerSecret;
            }
            this.key = OAuth.percentEncode(consumerSecret)
            + "&" + OAuth.percentEncode(accessor.tokenSecret);
        }

        static sign(message: Message, accessor: Accessor) {
            var name = OAuth.getParameterMap(message.parameters)["oauth_signature_method"];
            if (name == null || name == "") {
                name = "HMAC-SHA1";
                OAuth.setParameter(message, "oauth_signature_method", name);
            }
            OAuth.SignatureMethod.newMethod(name, accessor).sign(message);
        }

        /** Instantiate a SignatureMethod for the given method name. */
        static newMethod(name: string, accessor: Accessor) {
            var impl = OAuth.SignatureMethod.REGISTERED[name];
            if (impl != null) {
                var method = new impl();
                method.initialize(name, accessor);
                return method;
            }
            var err = new Error("signature_method_rejected");
            var acceptable = "";
            for (var r in OAuth.SignatureMethod.REGISTERED) {
                if (acceptable != "") acceptable += '&';
                acceptable += OAuth.percentEncode(r);
            }
            // TODO: プロパティの宣言
            (<any>err).oauth_acceptable_signature_methods = acceptable;
            throw err;
        }

        /** A map from signature method name to constructor. */
        static REGISTERED: { [name: string]: { new (): SignatureMethod }; } = {};

        /** Subsequently, the given constructor will be used for the named methods.
            The constructor will be called with no parameters.
            The resulting object should usually implement getSignature(baseString).
            You can easily define such a constructor by calling makeSubclass, below.
         */
        static registerMethodClass(names: string[], classConstructor: { new(): SignatureMethod }) {
            for (var n = 0; n < names.length; ++n) {
                OAuth.SignatureMethod.REGISTERED[names[n]] = classConstructor;
            }
        }

        /** Create a subclass of OAuth.SignatureMethod, with the given getSignature function. */
        static makeSubclass(getSignatureFunction: (baseString: string) => string): { new(): SignatureMethod } {
            var superClass = OAuth.SignatureMethod;
            var subClass = function () {
                superClass.call(this);
            };
            subClass.prototype = new superClass();
            // Delete instance variables from prototype:
            // delete subclass.prototype... There aren't any.
            subClass.prototype.getSignature = getSignatureFunction;
            subClass.prototype.constructor = subClass;
            return <any>subClass;
        }

        static getBaseString(message: Message) {
            var URL = message.action;
            var q = URL.indexOf('?');
            var parameters: [string, string][]|{ [key: string]: string; };
            if (q < 0) {
                parameters = message.parameters;
            } else {
                // Combine the URL query string with the other parameters:
                var pp = OAuth.decodeForm(URL.substring(q + 1));
                var toAdd = OAuth.getParameterList(message.parameters);
                for (var a = 0; a < toAdd.length; ++a) {
                    pp.push(toAdd[a]);
                }
                parameters = pp;
            }
            return OAuth.percentEncode(message.method.toUpperCase())
                + '&' + OAuth.percentEncode(OAuth.SignatureMethod.normalizeUrl(URL))
                + '&' + OAuth.percentEncode(OAuth.SignatureMethod.normalizeParameters(parameters));
        }

        static normalizeUrl(url: string) {
            var uri = OAuth.SignatureMethod.parseUri(url);
            var scheme = uri.protocol.toLowerCase();
            var authority = uri.authority.toLowerCase();
            var dropPort = (scheme == "http" && +uri.port == 80)
                || (scheme == "https" && +uri.port == 443);
            if (dropPort) {
                // find the last : in the authority
                var index = authority.lastIndexOf(":");
                if (index >= 0) {
                    authority = authority.substring(0, index);
                }
            }
            var path = uri.path;
            if (!path) {
                path = "/"; // conforms to RFC 2616 section 3.2.2
            }
            // we know that there is no query and no fragment here.
            return scheme + "://" + authority + path;
        }

        static parseUri(str: string): Uri {
            /* This function was adapted from parseUri 1.2.1
               http://stevenlevithan.com/demo/parseuri/js/assets/parseuri.js
             */
            var o = {
                key: ["source", "protocol", "authority", "userInfo", "user", "password", "host", "port", "relative", "path", "directory", "file", "query", "anchor"],
                parser: { strict: /^(?:([^:\/?#]+):)?(?:\/\/((?:(([^:@\/]*):?([^:@\/]*))?@)?([^:\/?#]*)(?::(\d*))?))?((((?:[^?#\/]*\/)*)([^?#]*))(?:\?([^#]*))?(?:#(.*))?)/ }
            };
            var m = o.parser.strict.exec(str);
            var uri = <{ [key: string]: string; }>{};
            var i = 14;
            while (i--) uri[o.key[i]] = m[i] || "";
            return <Uri><any>uri;
        }

        static normalizeParameters(parameters: [string, string][]|{ [key: string]: string; }): string {
            if (parameters == null) {
                return "";
            }
            var list = OAuth.getParameterList(parameters);
            var sortable = <[string, [string, string]][]>[];
            for (var p = 0; p < list.length; ++p) {
                var nvp = list[p];
                if (nvp[0] != "oauth_signature") {
                    sortable.push([OAuth.percentEncode(nvp[0])
                        + " " // because it comes before any character that can appear in a percentEncoded string.
                        + OAuth.percentEncode(nvp[1])
                        , nvp]);
                }
            }
            sortable.sort(function (a, b) {
                if (a[0] < b[0]) return -1;
                if (a[0] > b[0]) return 1;
                return 0;
            });
            var sorted = <[string, string][]>[];
            for (var s = 0; s < sortable.length; ++s) {
                sorted.push(sortable[s][1]);
            }
            return OAuth.formEncode(sorted);
        }
    }
}

OAuth.SignatureMethod.registerMethodClass(["PLAINTEXT", "PLAINTEXT-Accessor"],
    OAuth.SignatureMethod.makeSubclass(
        function getSignature(baseString) {
            return this.key;
        }
        ));

OAuth.SignatureMethod.registerMethodClass(["HMAC-SHA1", "HMAC-SHA1-Accessor"],
    OAuth.SignatureMethod.makeSubclass(
        function getSignature(baseString) {
            b64pad = '=';
            var signature = b64_hmac_sha1(this.key, baseString);
            return signature;
        }
        ));

try {
    OAuth.correctTimestampFromSrc();
} catch (e) {
}