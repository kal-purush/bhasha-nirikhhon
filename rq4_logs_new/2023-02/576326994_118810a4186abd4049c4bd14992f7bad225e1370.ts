import { Infer } from "../../src/types";
import { allNodes } from "../../networks/alarm";
import { createNetwork } from "../../src/utils/network";
import { inferenceAlgorithms } from "../../src";
const reader = require("xlsx");
// const file = reader.readFile("./output.xlsx");

const { likelihoodWeighting } = inferenceAlgorithms;

const network = createNetwork(...allNodes);
const sampleSize = 10000;
console.log("evaluating....");

// const sampleSizes = [
//   10, 100, 500, 1000, 5000, 10000, 50000, 100000, 500000, 1000000,
// ];
const results = [];
const sampleSizes = [10, 100, 500, 1000];
for (let sampleSize = 1; sampleSize < 10; sampleSize++) {
  const observedValues = { BURGLARY: "T" };
  results.push(
    likelihoodWeighting
      .infer(network, { MARY_CALLS: "T" }, observedValues, sampleSize)
      .toFixed(4)
  );
}

console.log("finished!");
// console.log(results);

var csv = results.join(",");
console.log(csv);