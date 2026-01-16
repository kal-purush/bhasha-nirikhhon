const fs = require("fs");

const textIncomming = fs.readFileSync("./txt/input.txt", "utf-8");
console.log(textIncomming);

const textOutgoing = `This is the text which is being extracted from input.txt ${textIncomming} . 
Dated ${Date.now()}`;
fs.writeFileSync("./txt/output.txt", textOutgoing);