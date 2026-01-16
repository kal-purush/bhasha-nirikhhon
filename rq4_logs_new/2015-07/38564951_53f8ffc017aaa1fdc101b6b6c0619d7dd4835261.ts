
/// <reference path='../../typings/node/node.d.ts'/>

import {createTextWriter} from '../core';
import * as fs from 'fs';

export const enum ModuleKind {
    Amd,
    CommonJs,
}

interface ComponentImport {
    className: string;
    path: string;

    /**
     * The current route of page.
     */
    route: string;
}

interface EmitTextWriter {
    write(s: string): void;
    writeLine(): void;
    increaseIndent(): void;
    decreaseIndent(): void;
    getText(): string;
    rawWrite(s: string): void;
    writeLiteral(s: string): void;
    getTextPos(): number;
    getLine(): number;
    getColumn(): number;
    getIndent(): number;
}

interface EmitClientComposerOptions {
    moduleKind: ModuleKind;
    output: string;
}

export interface EmitPageInfo {
    route: string;
    layout: ComponentImport;
    contents: EmitContentInfo[];
}

export interface EmitContentInfo extends ComponentImport {

    /**
     * Region of the layout this content belongs to.
     */
    region: string;
}

export function emitClientComposer(imports: ComponentImport[], pageInfos: EmitPageInfo, opt: EmitClientComposerOptions, writer: EmitTextWriter) {
    let {write, writeLine, increaseIndent} = writer;

    writeClientComposer();
    fs.writeFileSync(opt.output, writer.getText(), { encoding: 'utf-8' });
    return;

    function writeClientComposer(): void {
        if (opt.moduleKind === ModuleKind.Amd) {
            writeStartOfAmd(imports);
        }
        else {
            writeCommonJsImportList(imports);
        }
    }

    function writeQuote(): void {
        write('\'');
    }

    /**
     * Writes `define([...], function(...) {`.
     */
    function writeStartOfAmd(imports: ComponentImport[]): void {
        write('define([');
        for (let i in imports) {
            writeQuote();
            write(imports[i].path);
            writeQuote();
            if(i !== imports.length - 1) {
                write(',');
            }
        }
        write('], function(');
        for (let i in imports) {
            write(imports[i].value);
            if(i !== imports.length - 1) {
                write(',');
            }
        }
        write(') {');
    }

    function writeCommonJsImportList(imports: ComponentImport[]): void {
        for (let i of imports) {
            write(`var ${i.value} = require('${i.path}');`);
            writeLine();
        }
    }

    function writeVariableList(vars: string[]): void {
        for (let i in vars) {
            write(vars[i]);

            if(i !== vars.length - 1) {
                write(',');
            }
        }
    }
}