import * as ts from "typescript";
import * as fs from "fs";
import * as path from "path";

interface Weak {
    <T>(o: T, callback?: () => void): T;
    isDead(o: any): boolean;
}

declare function require(s: "weak"): Weak;
declare function require(s: string): any;

interface Test {
    bump(): void;
    names: string[];
    snapshots: ts.Map<ts.IScriptSnapshot>;
    ls: ts.LanguageService;
}

if (typeof global === "undefined" || !global.gc) {
    throw new Error("use --expose_gc key");
}

const weak = require("weak");

let data = createLanguageService(path.join(process.cwd(), "test/tsconfig.json"));
testOperation(data, (data) => data.ls.getSemanticDiagnostics(data.names[0]));
testOperation(data, data => {
    data.ls.getCompletionsAtPosition(data.names[0], 0);
})

function stringToPath(s: string) {
    s = path.normalize(s);
    return path.resolve(s);
}


function testOperation(test: Test, op: (test: Test) => void) {
    op(test);
    let ref = weak(test.ls.getProgram());
    let tcref = weak(test.ls.getProgram().getTypeChecker());
    test.bump();
    op(test);
    global.gc();
    if (!weak.isDead(ref)) {
        console.log("unexpected: old program is not dead");
    }
    if (!weak.isDead(tcref)) {
        console.log("unexpected: old typechecker is not dead");
    }
}
function createLanguageService(configPath: string) {
    const config = ts.readConfigFile(configPath, file => ts.sys.readFile(file));
    const commandLine = ts.parseJsonConfigFileContent(config.config, ts.sys, path.dirname(configPath));
    const program = ts.createProgram(commandLine.fileNames, commandLine.options);
    const snapshots: ts.Map<ts.IScriptSnapshot> = {};
    for (const f of program.getSourceFiles()) {
        snapshots[stringToPath(f.fileName)] = ts.ScriptSnapshot.fromString(f.text);
    }
    const names: string[] = [];
    for (const f in snapshots) {
        names.push(f);
    }
    let version = 0;
    const host: ts.LanguageServiceHost = {
        directoryExists(name) {
            return ts.sys.directoryExists(name);
        },
        getCancellationToken() {
            return undefined;
        },
        getDefaultLibFileName() {
            return "lib.d.ts";
        },
        getCurrentDirectory() {
            return process.cwd();
        },
        getScriptFileNames() {
            return names;
        },
        getScriptSnapshot(f) {
            return snapshots[stringToPath(f)];
        },
        getCompilationSettings() {
            return commandLine.options
        },
        getScriptVersion(f) {
            return stringToPath(f) === names[0] ? version.toString() : "0";
        }
    };
    const ls = ts.createLanguageService(host);
    return { ls, bump: () => {  version++; }, names, snapshots };
}