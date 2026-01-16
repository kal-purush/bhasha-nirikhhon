const fs = require('fs');
const CONSOLE_STYLE = {
  BOLD: str => `\x1b[1m${str}\x1b[22m`,
  RED: str => `\x1b[31m${str}\x1b[22m`,
  GREEN: str => `\x1b[32m${str}\x1b[22m`,
}

const matchNumber = process.argv[2].padStart(2, 0);
const directory = `Match ${matchNumber}`;

if (fs.existsSync(directory)) {
  console.log(CONSOLE_STYLE.BOLD(`\nAnalysing ${directory}`));
  const submissions = `${directory}/submissions`;
  let fileSizes = [];

  const files = fs.readdirSync(submissions, { withFileTypes: true })
    .map(item => item.name)
    .filter(item => /.min.js$/.test(item));

  files.forEach(file => {
    const fileSize = fs.statSync(`${submissions}/${file}`).size;
    fileSizes.push([fileSize, file]);
  });

  fileSizes = fileSizes.sort((a, b) => (a[0] - b[0]));
  fileSizes.forEach((data) => {
    const [size, name] = data;
    console.log(
      CONSOLE_STYLE.RED(`${size} bytes`.padStart(10)),
      CONSOLE_STYLE.GREEN(name)
    );
  });
} else {
  console.log(`That match doesn't exist`);
}