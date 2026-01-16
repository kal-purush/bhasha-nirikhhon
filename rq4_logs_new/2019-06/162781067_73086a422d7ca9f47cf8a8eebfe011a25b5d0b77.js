import argv from "argv";
import fs from "fs";
import REPL from "./src/repl";

process.stdin.setEncoding("utf8");

const PROMPT = ">>>";
console.log("welcome to simple scheme interpreter\n");
process.stdout.write(PROMPT);

const args = argv
  .option([
    {
      name: "load",
      short: "l",
      type: "list,path",
    },
    {
      name: "exec",
      short: "e",
      type: "string",
    },
  ])
  .run();
const sources = (args.options.load || []).map(path =>
  fs.readFileSync(path, "utf8")
);
const repl = new REPL(sources);
process.stdin.on("readable", () => {
  const chunk = process.stdin.read();
  if (chunk !== null) {
    process.stdout.write(`=> ${repl.run(chunk)}\n${PROMPT}`);
  }
});

process.stdin.on("end", () => {
  process.stdout.write("\ngood bye\n");
});