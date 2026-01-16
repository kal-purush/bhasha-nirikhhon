import path from 'path';
import {Arguments} from 'yargs';
import gulp from 'gulp';
import {CommandFactory} from './command-factory';
import * as gulpPlugins from '../gulp-plugins';
import {yarn} from '../helpers/yarn';

interface TsArgs {
  type: 'bin' | 'module' | 'react-module';
}

const DEPENDENCY_LIST: {[x: string]: any} = {
  bin: {
    dependencies: [],
    devDependencies: [
      'typescript',
      'tslint',
      'ts-node',
      '@nju33/tsconfig-main',
    ],
    peerDependencies: [],
  },
  module: {
    dependencies: [],
    devDependencies: [
      'typescript',
      'tslint',
      'typedoc',
      'jest',
      'ts-jest',
      'rollup',
      '@types/jest',
      '@nju33/tsconfig-main',
      '@nju33/tsconfig-module',
      '@nju33/tsconfig-types',
    ],
    peerDependencies: [],
  },
  'react-module': {
    dependencies: [],
    devDependencies: [
      'typescript',
      'tslint',
      'typedoc',
      'jest',
      '@nju33/tsconfig-react-main',
      '@nju33/tsconfig-react-module',
      '@nju33/tsconfig-react-types',
    ],
    peerDependencies: [],
  },
};

const SCRIPTS_LIST: {[x: string]: any} = {
  bin: {
    prepare: 'yarn build',
    dev: 'ts-node -w',
    build: 'tsc',
  },
  module: {
    prepare: 'yarn build',
    test: 'jest',
    typedoc:
      'typedoc --theme minimal --out docs src/index.ts --ignoreCompilerErrors',
    'build.main': 'tsc -P tsconfig.main.json',
    'build.module': 'tsc -P tsconfig.module.json',
    'build.iife':
      'rollup dist/module/index.js --format iife --name Name --file file.js',
    'build.types': 'tsc -P tsconfig.types.json --emitDeclarationOnly',
    build:
      'yarn build.main; yarn build.module; yarn build.iife; yarn build.types',
  },
  'react-module': {
    prepare: 'yarn build',
    test: 'jest',
    typedoc:
      'typedoc --theme minimal --out docs src/index.ts --ignoreCompilerErrors',
    'build.main': 'tsc -P tsconfig.main.json',
    'build.module': 'tsc -P tsconfig.module.json',
    'build.types': 'tsc -P tsconfig.types.json --emitDeclarationOnly',
    build: 'yarn build.main; yarn build.module; yarn build.types',
  },
};

export const ts: CommandFactory<Arguments<TsArgs>> = config => async args => {
  const {type} = args;

  const FILES_TS = path.join(config.filesRoot, 'typescript');
  const pkgName = path.join(process.cwd(), 'package.json');
  const targetDir = ['typescript', type].join('-');
  const targetPattern = path.join(FILES_TS, targetDir, '*');

  const dependencyList = DEPENDENCY_LIST[type];
  const scriptsList = SCRIPTS_LIST[type];

  gulp.series([
    done =>
      gulp
        .src(pkgName)
        .pipe(
          gulpPlugins.overridePkg({
            scripts: scriptsList,
          }),
        )
        .pipe(gulpPlugins.prettier())
        .pipe(gulp.dest('.')),
    () =>
      gulp
        .src(targetPattern)
        .pipe(gulpPlugins.prettier())
        .pipe(gulp.dest('.'))
        .pipe(gulpPlugins.log.copied),
  ])(async () => {
    await yarn(dependencyList.peerDependencies, {type: 'peer'});
    await yarn(dependencyList.devDependencies, {type: 'dev'});
    await yarn(dependencyList.dependencies);
  });
};