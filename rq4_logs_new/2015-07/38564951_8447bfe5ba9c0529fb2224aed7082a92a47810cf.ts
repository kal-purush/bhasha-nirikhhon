
import { combinePaths, contains, fileExtensionIs, map } from './core';

export interface System {
    args: string[];
    newLine: string;
    useCaseSensitiveFileNames: boolean;
    write(s: string): void;
    readFile(path: string, encoding?: string): string;
    writeFile(path: string, data: string, writeByteOrderMark?: boolean): void;
    watchFile?(path: string, callback: (path: string) => void): FileWatcher;
    resolvePath(path: string): string;
    fileExists(path: string): boolean;
    directoryExists(path: string): boolean;
    createDirectory(path: string): void;
    getExecutingFilePath(): string;
    getCurrentDirectory(): string;
    readDirectory(path: string, extension?: string, exclude?: string[]): string[];
    getMemoryUsage?(): number;
    exit(exitCode?: number): void;
}

export interface FileWatcher {
    close(): void;
}

declare var require: any;
declare var module: any;
declare var process: any;
declare var global: any;
declare var __filename: string;

declare class Enumerator {
    public atEnd(): boolean;
    public moveNext(): boolean;
    public item(): any;
    constructor(o: any);
}

export var sys: System = (function () {
    function getNodeSystem(): System {
        var _fs = require("fs");
        var _path = require("path");
        var _os = require('os');

        var platform: string = _os.platform();
        // win32\win64 are case insensitive platforms, MacOS (darwin) by default is also case insensitive
        var useCaseSensitiveFileNames = platform !== "win32" && platform !== "win64" && platform !== "darwin";

        function readFile(fileName: string, encoding?: string): string {
            if (!_fs.existsSync(fileName)) {
                return undefined;
            }
            var buffer = _fs.readFileSync(fileName);
            var len = buffer.length;
            if (len >= 2 && buffer[0] === 0xFE && buffer[1] === 0xFF) {
                // Big endian UTF-16 byte order mark detected. Since big endian is not supported by node.js,
                // flip all byte pairs and treat as little endian.
                len &= ~1;
                for (var i = 0; i < len; i += 2) {
                    var temp = buffer[i];
                    buffer[i] = buffer[i + 1];
                    buffer[i + 1] = temp;
                }
                return buffer.toString("utf16le", 2);
            }
            if (len >= 2 && buffer[0] === 0xFF && buffer[1] === 0xFE) {
                // Little endian UTF-16 byte order mark detected
                return buffer.toString("utf16le", 2);
            }
            if (len >= 3 && buffer[0] === 0xEF && buffer[1] === 0xBB && buffer[2] === 0xBF) {
                // UTF-8 byte order mark detected
                return buffer.toString("utf8", 3);
            }
            // Default is UTF-8 with no byte order mark
            return buffer.toString("utf8");
        }

        function writeFile(fileName: string, data: string, writeByteOrderMark?: boolean): void {
            // If a BOM is required, emit one
            if (writeByteOrderMark) {
                data = '\uFEFF' + data;
            }

            _fs.writeFileSync(fileName, data, "utf8");
        }

        function getCanonicalPath(path: string): string {
            return useCaseSensitiveFileNames ? path.toLowerCase() : path;
        }

        function readDirectory(path: string, extension?: string, exclude?: string[]): string[] {
            var result: string[] = [];
            exclude = map(exclude, s => getCanonicalPath(combinePaths(path, s)));
            visitDirectory(path);
            return result;
            function visitDirectory(path: string) {
                var files = _fs.readdirSync(path || ".").sort();
                var directories: string[] = [];
                for (let current of files) {
                    var name = combinePaths(path, current);
                    if (!contains(exclude, getCanonicalPath(name))) {
                        var stat = _fs.statSync(name);
                        if (stat.isFile()) {
                            if (!extension || fileExtensionIs(name, extension)) {
                                result.push(name);
                            }
                        }
                        else if (stat.isDirectory()) {
                            directories.push(name);
                        }
                    }
                }
                for (let current of directories) {
                    visitDirectory(current);
                }
            }
        }

        return {
            args: process.argv.slice(2),
            newLine: _os.EOL,
            useCaseSensitiveFileNames: useCaseSensitiveFileNames,
            write(s: string): void {
                // 1 is a standard descriptor for stdout
                _fs.writeSync(1, s);
            },
            readFile,
            writeFile,
            watchFile: (fileName, callback) => {
                // watchFile polls a file every 250ms, picking up file notifications.
                _fs.watchFile(fileName, { persistent: true, interval: 250 }, fileChanged);

                return {
                    close() { _fs.unwatchFile(fileName, fileChanged); }
                };

                function fileChanged(curr: any, prev: any) {
                    if (+curr.mtime <= +prev.mtime) {
                        return;
                    }

                    callback(fileName);
                };
            },
            resolvePath: function (path: string): string {
                return _path.resolve(path);
            },
            fileExists(path: string): boolean {
                return _fs.existsSync(path);
            },
            directoryExists(path: string) {
                return _fs.existsSync(path) && _fs.statSync(path).isDirectory();
            },
            createDirectory(directoryName: string) {
                if (!this.directoryExists(directoryName)) {
                    _fs.mkdirSync(directoryName);
                }
            },
            getExecutingFilePath() {
                return __filename;
            },
            getCurrentDirectory() {
                return process.cwd();
            },
            readDirectory,
            getMemoryUsage() {
                if (global.gc) {
                    global.gc();
                }
                return process.memoryUsage().heapUsed;
            },

           /**
            * Exit process with an optional exit code.
            */
            exit(exitCode: number): void {
                process.exit(exitCode);
            }
        };
    }
    return getNodeSystem();
})();