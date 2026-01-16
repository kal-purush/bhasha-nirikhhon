import * as util from "util";
import * as myutil from "./myutil";

process.stdin.resume();
process.stdin.setEncoding("utf8");

let done = () => {
  console.log("Exiting");
  process.exit();
};

let handleTextInput = (line: string) => {
  let trimmed = line.trim();
  if (trimmed.indexOf("print ") !== -1) {
    let fileName = trimmed.substr(6);
    if (fileName) {
      myutil.printFileContent(fileName);
    }
  }
};

console.log("print file content using 'print filename.ext' and quit via 'quit'");
process.stdin.on("data", (text: string) => {
  console.log("received data:", util.inspect(text));
  if (text === "quit\n") {
    done();
  } else {
    handleTextInput(text);
  }
});