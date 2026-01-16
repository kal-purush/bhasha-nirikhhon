import path from "path";
import { readFileSync, writeFileSync } from "fs";
import { Parser } from "./ast/parser";
import { Scanner } from "./scanner/scanner";

let args = process.argv.slice(2);

let sourcePath: string = args[0];
let outputPath: string = args[1];

const source = readFileSync(path.resolve(sourcePath), "utf-8");
const scanner = new Scanner(source);
const parser = new Parser(scanner);

const ast = parser.parseProgram();

writeFileSync(path.resolve(outputPath), JSON.stringify(ast));

console.log(`Exported ast to ${outputPath}`);
console.log(
    `Paste output to https://jsoncrack.com/editor OR open in vs-code extension`
);