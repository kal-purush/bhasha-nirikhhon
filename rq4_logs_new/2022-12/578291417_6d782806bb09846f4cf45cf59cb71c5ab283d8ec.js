(function() {

    var fs = require('fs');
    var path = require('path');
    var execSync = require('exec-sync');
    var esprima = require('esprima');
    var escodegen = require('escodegen');
    var exec = require('child_process').exec;
    var UTILS_NAME = "uTILs2342ClEAnPC";

    function walkDirectory(dir) {
        var results = [];
        var list = fs.readdirSync(dir)
        list.forEach(function(file) {
            file = dir + '/' + file
            var stat = fs.statSync(file)
            if (stat && stat.isDirectory()) results = results.concat(walkDirectory(file))
            else {
                results.push(path.resolve(file))
            }
        });
        return results;
    }

    function getFilesRec(file) {
        var stat = fs.statSync(file)
        if (!stat) {
            return [];
        } else if (stat.isDirectory()) {
            return walkDirectory(file)
        } else {
            return [path.resolve(file)];
        }
    }

    function instrumentFile(projePath, name) {
        if (name.match(/^.*\.js$/)) {
            console.log("Instrumenting " + name);
            var pathInstr = path.resolve(name);
            try {
                execSync("node " + path.resolve(projePath, "./jalangi/src/js/instrument/esnstrument.js") + " "  + escapeShell(path.resolve(name)) + " --out " + escapeShell(pathInstr));
            } catch (e) {
                console.log("\nPreprocessor: Error when instrumenting " + name + ". Will ignore this file.\n" + e);
                return;
            }
            //attach utils
            var code = fs.readFileSync(pathInstr, "utf8");
            try {
                var ast = esprima.parse(code, {loc: true, range: true, comment: true, tokens: true, raw: true});
            } catch (e) {
                console.log("\nPreprocessor: Error when parsing " + pathInstr + ". Will ignore this file.\n" + e);
                return code;
            }
            ast = escodegen.attachComments(ast, ast.comments, ast.tokens);
            ast.body.splice(0, 0, esprima.parse("var " + UTILS_NAME + " = require(\"iflow\");"));
            var transformedCode = escodegen.generate(ast, {comment: true});
            fs.writeFileSync(pathInstr, transformedCode, "utf8")
            execSync("node " + path.resolve(projePath, "./pu/src/js/analysis/PCCleaner.js") + " " + escapeShell(pathInstr));
        }
    }

    var escapeShell = function(cmd) {
        return cmd.replace(/(["\s'$`\\])/g,'\\$1');
    };

    function runFile(iflowPath, file, projPath, tmpProjPath, callback, iterationsCallback, creationCallback) {
        var analysisPath = path.resolve(iflowPath, "pu/src/js/analysis/WrappedPrimitivesFlowAnalysis.js");
        var iterations = 1;
        var mainProc = null;
        (function recursiveRun() {
            console.log("********* Iteration " + (iterations++) + " *********");

            var runProc = exec("node  " + path.resolve(iflowPath, "./jalangi/src/js/commands/direct.js") + " --smemory --analysis " + analysisPath + " " + escapeShell(file), {maxBuffer: 1024 * 1000}, // --debug --max-stack-size=16000
                function (error, stdout, stderr) {
                    if (error !== null && error.stack.toString().indexOf("deprecated") === -1) {
                        console.log('exec error: ' + error.stack);
                    }
                    if (stdout.indexOf("Violation of Permissive-Upgrade Policy") != -1) {
                        //recursive call
                        console.log("Must run again")
                        recursiveRun();
                        iterationsCallback();
                    } else {
                        exec("node " + path.resolve(iflowPath, "./pu/src/js/analysis/LightweightTraceInterpreter.js") + " ./trace1.json ./jalangi_tmp/jalangi_sourcemap.json",
                            function (error, stdout, stderr) {
                                console.log(stdout);
                                console.log(stderr);
                                if (error !== null && error.stack.toString().indexOf("deprecated") === -1) {
                                    console.log('exec error: ' + error);
                                } else {
                                    traceTransform("./trace1.json", "./jalangi_sourcemap.json");
                                    console.log(tmpProjPath + " " + projPath)
                                    var responseText = fs.readFileSync("./trace1.json").toString().replace(new RegExp(tmpProjPath, 'g'), projPath)
                                    deleteFolderRecursive(responseText);
                                    callback(responseText);
                                }
                            });
                    }
                });
            creationCallback(runProc);
            runProc.stdout.pipe(process.stdout);
            runProc.stderr.pipe(process.stderr);
            console.log("Install cleanup");
            if (mainProc === null)
                mainProc = runProc;
            mainProc.on('exit', function () {
                console.log("Cleanup " + runProc.pid);
                try {
                    runProc.kill();
                    process.kill(runProc.pid + 1);// No idea why two processes are created. :(
                } catch (e) {
                    //best effort
                }
            });
            runProc.on('uncaughtException', function(err) {
                console.log(err.stack);
                if (err.stack.toString().indexOf("deprecated") === -1)
                    throw err;
            });
        })();
    }

    function runFileNoInterp(iflowPath, file, projPath, tmpProjPath, callback, iterationsCallback, creationCallback) {
        var analysisPath = path.resolve(iflowPath, "pu/src/js/analysis/WrappedPrimitivesFlowAnalysis.js");
        var iterations = 1;
        var mainProc = null;
        (function recursiveRun() {
            console.log("********* Iteration " + (iterations++) + " *********");

            var runProc = exec("node  " + path.resolve(iflowPath, "./jalangi/src/js/commands/direct.js") + " --smemory --analysis " + analysisPath + " " + escapeShell(file), {maxBuffer: 1024 * 1000}, // --debug --max-stack-size=16000
                function (error, stdout, stderr) {
                    if (error !== null && error.stack.toString().indexOf("deprecated") === -1) {
                        console.log('exec error: ' + error.stack);
                    }
                    if (stdout.indexOf("Violation of Permissive-Upgrade Policy") != -1) {
                        //recursive call
                        console.log("Must run again")
                        recursiveRun();
                        iterationsCallback();
                    } else {
                        callback();
                    }
                });
            creationCallback(runProc);
            runProc.stdout.pipe(process.stdout);
            runProc.stderr.pipe(process.stderr);

            runProc.on('uncaughtException', function(err) {
                if (err.stack.toString().indexOf("deprecated") === -1)
                    throw err;
            });
        })();
    }

    function runFileCustomAnalysis(iflowPath, file, analysisPath) {
        var runProc = exec("node  " + path.resolve(iflowPath, "./jalangi/src/js/commands/direct.js") + " --smemory --analysis " + analysisPath + " " + escapeShell(file), {maxBuffer: 1024 * 1000} // --debug --max-stack-size=16000
            );
        runProc.stdout.pipe(process.stdout);
        runProc.stderr.pipe(process.stderr);
    }

    function getSource(sourceMap, iid) {
        var i = 0;
        for (i = 0; i < sourceMap.length; i++) {
            if (sourceMap[i][iid]) {
                return sourceMap[i][iid];
            }
        }
        return -1
    }

    function traceTransform(traceFile, sourceMapFile){
        var fs = require("fs");
        var traceContent = fs.readFileSync(traceFile);
        var sourceMapContent = fs.readFileSync(sourceMapFile).toString();
        var lines = traceContent.toString().split("\n");
        iids = JSON.parse(sourceMapContent);
        var result = ""
        for (var i = 0; i < lines.length; i++) {
            var line = lines[i];
            if (line) {
                if (!line.match(/#.*/)) {
                    var elements = line.split(",");
                    var iid = elements[elements.length - 1];
                    var map = getSource(iids, iid)
                    if (map == -1) {
                        console.log("ERROR" + " -- " + iid + " " + line)
                    }
                    var re = new RegExp(iid + "$");
                    line = line.replace(re, iids[0][iid]);
                }
                result += line + "\n";
            }
        }
        fs.writeFileSync(traceFile, result);
    }

    function deleteFolderRecursive(path) {
        if (fs.existsSync(path)) {
            var list = fs.readdirSync(path)
            list.forEach(function (file, index) {
                var curPath = path + "/" + file;
                if (fs.lstatSync(curPath).isDirectory()) { // recurse
                    deleteFolderRecursive(curPath);
                } else { // delete file
                    fs.unlinkSync(curPath);
                }
            });
            fs.rmdirSync(path);
        }
    }

    // exports
    exports.walkDirectory = walkDirectory;
    exports.getFilesRec = getFilesRec;
    exports.instrumentFile = instrumentFile;
    exports.runFile = runFile;
    exports.runFileCustomAnalysis = runFileCustomAnalysis;
    exports.deleteFolderRecursive = deleteFolderRecursive;
    exports.runFileNoInterp = runFileNoInterp;
})();
