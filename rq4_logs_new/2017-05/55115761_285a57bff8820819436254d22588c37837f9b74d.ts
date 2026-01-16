import { Runner, RunnerWithSchema, writeFile } from '../common';
import { ReplaceInFileConfig } from '../schemas';

const chalk = require('chalk');
const replace = require('replace-in-file');

export class ReplaceInFile implements Runner {

  static forLoader(): RunnerWithSchema {
    return {
      $tool: 'replace-in-file',
      schema: require('../schemas/replace-in-file.json'),
      runner: ReplaceInFile,
    };
  }

  constructor(public config: ReplaceInFileConfig) { }

  run(): Promise<any> {
    return new Promise((resolve, reject) => {
      try {
        const changedFiles = replace.sync(this.config);
        if (changedFiles.length > 0) {
          console.log(chalk.green(changedFiles.length, 'file(s) were changed'));
          changedFiles.forEach(file => console.log(chalk.grey('-', file)));
        } else {
          console.log(chalk.yellow('No files were changed'));
        }
        resolve();
      } catch (err) {
        reject(err);
      }
    });
  }

}