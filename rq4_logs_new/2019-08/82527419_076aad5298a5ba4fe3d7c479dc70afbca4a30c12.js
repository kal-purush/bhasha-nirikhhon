
const yaml = require('js-yaml');
const fs = require('fs');

const check = require('offensive').default;
require('offensive/assertions/allElementsThat/register');
require('offensive/assertions/fieldThat/register');
require('offensive/assertions/aString/register');
require('offensive/assertions/allElementsThat/register');

cwd(__dirname);
const config = yaml.safeLoad(fs.readFileSync('./config.yml', 'utf-8'));

console.info('Validating config.yml');
check(config, 'config')
  .has.fieldThat('pkgs', field => field
    .contains.allElementsThat(elem => elem
      .has.fieldThat('name', name => name.is.aString)
      .and.fieldThat('repoUrl', repoUrl => repoUrl.is.aString)
      .and.fieldThat('pwdVar', pwdVar => pwdVar.is.aString)
      .and.fieldThat('testTask', testTask => testTask.is.aString)
    )
  )
;
console.info('[success]');

function cwd(dir) {
  console.info(`cwd: ${__dirname}`);
  process.cwd(dir);
}
