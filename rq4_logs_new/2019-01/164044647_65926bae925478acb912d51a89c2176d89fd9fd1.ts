import Listr from 'listr';
import path from 'path';
import {Arguments} from 'yargs';
import gulp from 'gulp';
import {CommandFactory} from 'types/command-factory';
import * as gulpPlugins from '../gulp-plugin';
import signale from '../instances/signale';
import {yarn} from '../helpers/yarn';

export const prettier: CommandFactory<Arguments<{}>> = config => async () => {
  const PACKAGE_PATH = path.join(config.filesRoot, 'prettier/prettier.config.js');

  gulp
    .src(PACKAGE_PATH)
    .pipe(gulpPlugins.prettier())
    .pipe(gulp.dest('.'))
    .pipe(gulpPlugins.log.copied())
    .on('finish', () => {
      new Listr([
        {
          title: 'Install devDependencies',
          task: ctx =>
            yarn(['prettier', '@nju33/prettier'], {type: 'dev', ctx}),
        },
      ])
        .run({postProcesses: []})
        .catch(err => {
          signale.error(err);
        })
        .then((ctx: {postProcesses: Function[]}) => {
          ctx.postProcesses.forEach(postProcess => {
            postProcess();
          });
        });
    });
};