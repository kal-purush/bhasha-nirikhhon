import 'reflect-metadata';


import * as fs from 'fs';
import { EOL } from 'os';
import * as path from 'path';

import * as mkdirp from 'mkdirp';
import * as sanitize from 'sanitize-filename';
import * as ts from 'typescript';

import Example from './example';
import ExampleMetadata from './example-metadata'
import FilePath from './file-path';


export default class ExampleContext {
  public readonly console: Console;

  public readonly mdFile: string;
  public readonly metadata: ExampleMetadata

  private _markerIndex = 0;
  public get markerIndex() { return this._markerIndex; }

  public constructor(
      public readonly filePath: FilePath,
      public readonly obj: Object,
      public readonly fn: Function | undefined,
      public readonly fileSourceText: string,
      public readonly fileSource: ts.SourceFile,
      public readonly clsSource?: ts.ClassDeclaration,
      public readonly fnSource?: ts.MethodDeclaration,
      public readonly indent = 2,
      public readonly width = 80,
      console?: Console) {
    const clsBaseFileName = sanitize(obj.constructor.name);

    this.mdFile = path.join(filePath.mdRootDir, clsBaseFileName + '.md');

    mkdirp.sync(filePath.mdRootDir);

    if (!fn) {
      fs.closeSync(fs.openSync(this.mdFile, 'w'));
    }

    this.console = console || global.console;

    const clsMetadata = Example.getMetadata(obj.constructor);
    const fnMetadata = fn ? Example.getMetadata(obj, fn.name) : undefined;

    this.metadata = {
      pkg: fnMetadata && fnMetadata.pkg || clsMetadata.pkg,
      module: fnMetadata && fnMetadata.module || clsMetadata.module,
      type: fnMetadata && fnMetadata.type || clsMetadata.type,
      member: fnMetadata && fnMetadata.member || clsMetadata.member,
    }
  }

  public static indentLines(
      depth: number, message: string, indent = 2, width = 80, marker?: string): string {

    const markerLength = (marker) ? marker.length : 0;
    const indentationLength = indent * depth;
    const columnWidth = width - indentationLength - markerLength;
    const indentation = ' '.repeat(indentationLength);
    const lines = message.split(/\r?\n/);

    let indentedLines: string[] = [];

    for (const line of lines) {
      let marked = !marker ? true : false;
      let lineFragment = line;

      while (lineFragment.length > columnWidth)
      {
        let lastSpaceIndex = lineFragment.lastIndexOf(' ', columnWidth - 1);

        if (lastSpaceIndex === -1) {
          let indentedLine = indentation + lineFragment.substring(0, columnWidth);

          if (!marked) {
            indentedLine += ' '.repeat(columnWidth - indentedLine.length - markerLength) + marker;
            marked = true;
          }

          indentedLines.push(indentedLine);

          lineFragment = lineFragment.substr(columnWidth);
        }
        else {
          let indentedLine = indentation + lineFragment.substring(0, lastSpaceIndex);
          if (!marked){
            indentedLine += ' '.repeat(columnWidth - indentedLine.length - markerLength) + marker;
            marked = true;
          }

          indentedLines.push(indentedLine);

          if (lastSpaceIndex === lineFragment.length) {
            lineFragment = '';
          }
          else {
            lineFragment = lineFragment.substr(lastSpaceIndex + 1);
          }
        }
      }

      let indentedLine = indentation + lineFragment;

      if (!marked) {
        indentedLine += ' '.repeat(columnWidth - line.length - markerLength) + marker;
        marked = true;
      }

      indentedLines.push(indentedLine);
    }

    return indentedLines.join(EOL);
  }

  public log(message: string = '') {
    this.console.log(message);
    fs.appendFileSync(this.mdFile, message + EOL);
  }

  public logHeader() {
    const metadata = this.metadata;

    const name = this.fn ? this.fn.name : this.obj.constructor.name;
    const description = metadata.member
      ? `Examples for package ${metadata.pkg ? `\`${metadata.pkg}\` ` : ''}submodule ` +
        `\`${metadata.module}\` type ${metadata.type ? `\`${metadata.type}\` `: ''}member ` +
        `\`${metadata.member}\``
      : metadata.type
      ? `Examples for package ${metadata.pkg ? `\`${metadata.pkg}\` ` : ''}submodule ` +
        `\`${metadata.module}\` type \`${metadata.type}\``
      : metadata.module
      ? `Examples for package ${metadata.pkg ? `\`${metadata.pkg}\` ` : ''}submodule ` +
        `\`${metadata.module}\``
      : metadata.pkg
      ? `Examples for package \`${metadata.pkg}\``
      : `Examples in \`${name}\``;

    const hdr = (this.fn) ? '## ' : '# ';

    const message = `${hdr}${name}${EOL}${description}${EOL}`;

    this.log(message)
  }

  public logIndented(depth: number, message: string, marker?: string | boolean): void {

    const logMessage = Class.indentLines(depth, message, this.indent, this.width, this.getMarker(marker));

    this.log(logMessage);
  }

  public logException(depth: number, fn: () => void, marker?: string | boolean): void {
    try {
      fn();
    }
    catch (err) {
      this.logIndented(depth, err.toString(), this.getMarker(marker));
    }
  }

  private getMarker(marker?: string | boolean): string | undefined {
    if (!marker) {
      return undefined;
    }

    if (typeof marker === 'string') {
      return marker;
    }

    const markerNumber = this._markerIndex + 1;
    this._markerIndex = this._markerIndex + 1;

    if (markerNumber <= 20) {
      return String.fromCharCode(0x2460 + markerNumber);
    }

    const markerNumerals = markerNumber.toString();

    return [...markerNumerals].map(char => String.fromCharCode(0x2460 + parseInt(char))).join();
  }
}
const Class = ExampleContext; // tslint:disable-line:variable-name