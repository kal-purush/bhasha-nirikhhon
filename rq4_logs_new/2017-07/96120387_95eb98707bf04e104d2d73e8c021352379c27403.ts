import express = require('express');
import http = require('http');
import fs = require('fs');
import path = require('path');
import url = require('url');
import { warn } from './logger';
import es = require("event-stream");
import send = require('send');

let INJECTED_CODE = '';

export function appendInjetion(script: string) {
    INJECTED_CODE = [
        INJECTED_CODE,
        '<script src="',
        script,
        '"></script>'
    ].join('');
}
export function serveStatic(wwwroot: string, fsroot: string) {
    fsroot = path.resolve(fsroot);
    var isFile = false;
    try {
        isFile = fs.statSync(fsroot).isFile();
    } catch (e) {
        if (e.code !== "ENOENT") throw e;
    }
    return function (req: express.Request, res: express.Response, next: express.NextFunction) {
        if (req.method !== "GET" && req.method !== "HEAD") return next();
        let pathname = url.parse(req.originalUrl).pathname;
        debugger
        if (pathname.substr(0, wwwroot.length) !== wwwroot) return next();
        pathname = pathname.substr(wwwroot.length);
        let reqpath = isFile ? "" : pathname;
        var hasNoOrigin = !req.headers.origin;
        var injectCandidates = [new RegExp("</body>", "i"), new RegExp("</svg>"), new RegExp("</head>", "i")];
        var injectTag: string = null;

        function directory() {
            debugger
            file(pathname + '/index.html');
            // res.statusCode = 301;
            // res.setHeader('Location', pathname + '/');
            // res.end('Redirecting to ' + escape(pathname) + '/');
        }

        function file(filepath: string /*, stat*/) {
            debugger
            var x = path.extname(filepath).toLocaleLowerCase(), match,
                possibleExtensions = ["", ".html", ".htm", ".xhtml", ".php", ".svg"];
            if (hasNoOrigin && (possibleExtensions.indexOf(x) > -1)) {
                var contents = fs.readFileSync(filepath, "utf8");
                for (var i = 0; i < injectCandidates.length; ++i) {
                    match = injectCandidates[i].exec(contents);
                    if (match) {
                        injectTag = match[0];
                        break;
                    }
                }
                if (injectTag === null) {
                    warn("Failed to inject refresh script!",
                        "Couldn't find any of the tags ", injectCandidates, "from", filepath);
                }
            }
        }

        function error(err: any) {
            if (err.status === 404) return next();
            next(err);
        }

        function inject(stream: NodeJS.ReadWriteStream) {
            if (injectTag) {
                var len = INJECTED_CODE.length + Number.parseInt(res.getHeader('Content-Length') as any);
                res.setHeader('Content-Length', len.toString());
                var originalPipe = stream.pipe;
                stream.pipe = function (resp: NodeJS.WritableStream) {
                    originalPipe.call(stream, es.replace(new RegExp(injectTag, "i"), INJECTED_CODE + injectTag)).pipe(resp);
                };
            }
        }

        send(req, reqpath, { root: fsroot })
            .on('error', error)
            .on('directory', directory)
            .on('file', file)
            .on('stream', inject)
            .pipe(res);
    };
}

function escape(html: string) {
    return String(html)
        .replace(/&(?!\w+;)/g, '&amp;')
        .replace(/</g, '&lt;')
        .replace(/>/g, '&gt;')
        .replace(/"/g, '&quot;');
}