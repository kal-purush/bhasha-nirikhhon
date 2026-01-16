var shell = require('shelljs');

if (!shell.which('git')) {
  shell.echo('Sorry, this script requires git');
  shell.exit(1);
}

// Run external tool synchronously
if (shell.exec('node_modules/.bin/lerna publish').code !== 0) {
  shell.echo('Error: Git commit failed');
  shell.exit(1);
}