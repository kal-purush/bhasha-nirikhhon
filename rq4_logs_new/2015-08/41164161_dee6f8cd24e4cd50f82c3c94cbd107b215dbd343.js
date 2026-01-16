require('./catchUnhandledPromiseRejections');

import fs from 'fs';
import csv from 'csv';
import path from 'path'

var inNames = process.argv.slice(-2).map(name => path.basename(name, '.json')),
    inFiles = inNames.map(name => require(`./feat/${name}.json`)),
    matrix = [];

// First row
matrix.push(['', inNames[1]].concat(inFiles[1].map(item => item.id)));
// Second row: content
matrix.push([inNames[0], 'sim'].concat(inFiles[1].map(item => item.content)));

// Other rows
matrix = matrix.concat(inFiles[0].map((item1) => {
  // Returns a row here
  return [item1.id, item1.content].concat(inFiles[1].map((item2) => {
    return cosineSim(item1, item2);
  }));
}));

var outName = `data/${inNames[0]}-${inNames[1]}.csv`;
csv.stringify(matrix, (err, output) => {
  if(err){throw err;}

  fs.writeFile(outName, output);
});


function cosineSim(vec1, vec2){
  var innerProduct = 0;
  Object.keys(vec1.ngram).forEach((gram) => {
    if(vec2.ngram[gram]){
      innerProduct += vec1.ngram[gram] * vec2.ngram[gram];
    }
  });

  return innerProduct / vec1.length / vec2.length;
}

// Take the count of each gram to be 1.
// That is, we only consider presence of each word, instead of their count.
//
function presenceCosineSim(vec1, vec2) {
  var innerProduct = 0;
  Object.keys(vec1.ngram).forEach((gram) => {
    if(vec2.ngram[gram]){
      innerProduct += 1
    }
  });

  return innerProduct / Math.sqrt(vec1.count * vec2.count);
}
