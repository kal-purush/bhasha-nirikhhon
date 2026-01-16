import path from 'path';
import {Arguments} from 'yargs';
import gulp from 'gulp';
import pupa from 'gulp-pupa';
import rename from 'gulp-rename';
import prompts from 'prompts';
import {Package} from 'types/package';
import {CommandFactory} from 'types/command-factory';
import * as gulpPlugin from '../gulp-plugin';
import signale from '../instances/signale';

type Badge =
  | 'github'
  | 'npm'
  | 'typescript'
  | 'circleci'
  | 'typedoc'
  | 'license'
  | 'browserslist'
  | 'prettier';

interface Answers {
  repoName: string;
  packageName: string;
  badges: Partial<Record<Badge, string>>;
}

const inquire: (pkg: Partial<Package>) => Promise<Answers> = pkg =>
  prompts([
    {
      type: 'text',
      name: 'repoName',
      message: 'リポジトリの名前',
      validate: (val: string) => (val.length > 0 ? true : '必須です'),
    },
    {
      type: 'text',
      name: 'packageName',
      message: 'パッケージの名前',
      initial: pkg.name,
      validate: (val: string) => (val.length > 0 ? true : '必須です'),
    },
    {
      type: 'text',
      name: 'packageDescription',
      message: 'パッケージの説明',
      initial: pkg.description,
    },
    {
      type: 'multiselect',
      name: 'badges',
      message: '表示するバッジ',
      choices: [
        {title: 'GitHub', value: 'github', selected: true},
        {title: 'npm', value: 'npm', selected: true},
        {title: 'TypeScript', value: 'typescript', selected: true},
        {title: 'CircleCI', value: 'circleci', selected: true},
        {title: 'TypeDoc', value: 'typedoc', selected: true},
        {title: 'license', value: 'license', selected: true},
        {title: 'Browserslist', value: 'license', selected: true},
        {title: 'Prettier', value: 'prettier', selected: true},
      ] as any, // because strange types,
      format: ((
        badgeNames: Badge[],
        {repoName, packageName}: Pick<Answers, 'repoName' | 'packageName'>,
      ) => {
        return badgeNames
          .map(badgeName => {
            switch (badgeName) {
              case 'github': {
                return `[![github](https://badgen.net/badge//nju33,${repoName}/000?icon=github&list=1)](https://github.com/nju33/${repoName})`;
              }
              case 'npm': {
                return `[![npm:version](https://badgen.net/npm/v/${packageName}?icon=npm&label=)](https://www.npmjs.com/package/${packageName})`;
              }
              case 'typescript': {
                return '[![typescript](https://badgen.net/badge/lang/typescript/0376c6?icon=npm)](https://www.typescriptlang.org/)';
              }
              case 'circleci': {
                return `[![ci:status](https://badgen.net/circleci/github/nju33/${repoName})](https://circleci.com/gh/nju33/${repoName})`;
              }
              case 'typedoc': {
                return `[![document:typedoc](https://badgen.net/badge/document/typedoc/9602ff)](https://docs--${repoName}.netlify.com/)`;
              }
              case 'license': {
                return `[![license](https://badgen.net/npm/license/${repoName})](https://github.com/nju33/${repoName}/blob/master/LICENSE)`;
              }
              case 'browserslist': {
                return '[![browserslist](https://badgen.net/badge/browserslist/chrome,edge/ffd539?list=1)](https://browserl.ist/?q=last+1+chrome+version%2C+last+1+edge+version)';
              }
              case 'prettier': {
                return '[![code style:prettier](https://badgen.net/badge//prettier/ff69b3?label=code%20style)](https://github.com/prettier/prettier)';
              }
            }
          })
          .join('\n');
      }) as any,
    },
  ]) as any;

export const readme: CommandFactory<Arguments<{}>> = config => async () => {
  const PACKAGE_PATH = path.join(config.filesRoot, 'readme/README');

  const answers = await inquire(config.pkg);

  if (answers.badges === undefined) {
    return;
  }

  gulp
    .src(PACKAGE_PATH)
    .pipe(pupa(answers))
    .pipe(rename({extname: '.md'}))
    .pipe(gulpPlugin.prettier())
    .pipe(gulp.dest('.'))
    .on('finish', () => {
      signale.generated('README.md');
    });
};