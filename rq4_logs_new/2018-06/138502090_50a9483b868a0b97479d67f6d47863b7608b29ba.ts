import program from 'commander';
import config from '../package.json';
import { init } from './shared/init';

program
  .version(config.version)
  .option('-i, --init', 'init default bootstrap files')
  .option('-d, --dry-run', 'preview of execution')
  .parse(process.argv);

(async () => {
  if (program.init) {
    await init('angular', program);
  }
})();