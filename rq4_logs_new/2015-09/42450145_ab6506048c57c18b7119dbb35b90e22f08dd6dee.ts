import * as fs from 'fs';
import compactDom = require("compact-dom");

let SortedArray: any = require("sorted-array");

interface ParsedFile {
  path: string;
  tagNames: string[];
  classNames: string[];
}

let createTypescriptDefinitionsManager = (options: compactDom.Options) => {
  
  let parsedFiles: {[index:string]: ParsedFile} = {};
  
  let parse = (path: string, contents: string) => {
    let result: ParsedFile = {
      path: path,
      tagNames: [],
      classNames: []
    }
    contents.split("\n").forEach((line: string) => {
      let regexp = compactDom.createRegExp(options);
      let matches: string[];
      while ((matches = regexp.exec(line)) !== null) {
        var split = matches[1].split(".");
        result.tagNames.push(split[1]);
        for (let i=2;i<split.length;i++) {
          result.classNames.push(split[i]);
        }
      }
    });
    return result;
  }
  
  return {
    addFile: (path: string, contents: string): void => {
      var parsedFile = parse(path, contents);
      parsedFiles[path] = parsedFile;
    },
    removeFile: (path: string): void => {
      delete parsedFiles[path];
    },
    updateFile: (path: string, contents: string): void => {
      var parsedFile = parse(path, contents);
      parsedFiles[path] = parsedFile;
    },
    createOutput: (): string => {
      let tagNames = new SortedArray([]);
      let classNames = new SortedArray([]);
      Object.keys(parsedFiles).forEach((key:string) => {
        parsedFiles[key].tagNames.forEach((tagName: string) => {
          tagNames.insert(tagName);
        });
        parsedFiles[key].classNames.forEach((className: string) => {
          classNames.insert(className);
        });
      });
      return ` // Code generated using gulp-compact-dom
interface CompactDomNode {
 (propertiesOrFirstChild?: maquette.VNodeProperties|maquette.VNodeChild, ...children: maquette.VNodeChild[] ): maquette.VNode;
  
  // Lists every css class used
${ tagNames.array.map((tagName: string) => `  ${tagName}: CompactDomNode;`).join("\n") }
}

declare module maquette.h {
  
  // lists every tagname used
${ classNames.array.map((className: string) => `  export var ${className}: CompactDomNode;`).join("\n") }
}
`;
    } 
  }
}

export {createTypescriptDefinitionsManager}