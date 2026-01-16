const versiony = require('versiony');

const cmd = process.argv.slice(2)[0];
if (cmd === 'get') {
  console.log(versiony.from('package.json').get());
  process.exit(0);
}

const releases = {
  patch: 'patch',
  minor: 'minor',
  major: 'major',
};
const isValid = Object.keys(releases).some((value) => value === cmd);

if (!isValid) {
  console.error(
    `Must specify a release. [${Object.keys(releases).join('|')}]`,
  );
  process.exit(1);
}

const versionFn = releases[cmd];

versiony[versionFn]().with('package.json');

console.log(versiony.get());