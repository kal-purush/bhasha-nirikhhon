const Mustache = require('mustache');
const fs = require('fs');

function slurp(filename) {
  return fs.readFileSync(filename, { encoding: 'utf8' });
}

const template = slurp(__dirname + '/README.template.md');

let view = {
  problems: []
};

const folderRe = /^([0-9]{3,})-([0-9a-z\-]+)(\..+)?$/;
const solBaseUrl = 'https://github.com/lge88/kb/tree/master/lc';
const lcBaseUrl = 'https://leetcode.com/problems';

fs.readdirSync(__dirname).map((filename) => {
  const match = filename.match(folderRe);
  if (match !== null) {
    const problemNumber = parseInt(match[1]);
    const problemName = match[2];
    const version = match[3] && match[3].substr(1);
    const solution = `${solBaseUrl}/${filename}`;
    const source = problemNumber > 0 ? `${lcBaseUrl}/${problemName}` : '';
    view.problems.push({ name: filename, solution, source, version });
  }
});

// console.log(view.problems);
console.log(Mustache.render(template, view));