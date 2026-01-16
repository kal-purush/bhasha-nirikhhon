/*
 * @Author: qiansc
 * @Date: 2019-04-24 15:54:24
 * @Last Modified by: qiansc
 * @Last Modified time: 2019-10-17 19:54:02
 */

import { existsSync, readFileSync, unlinkSync } from 'fs';
import * as gulp from 'gulp';
import * as path from 'path';
import { amdWrap } from '../src/hook';

describe('Hook Test', () => {
  it('empty arguments', () => {
    const file = `${__dirname}\/dist\/assert\/empty-arguments-define.js`;
    if (existsSync(file)) {
      unlinkSync(file);
    }

    return new Promise((resolve) => {
      function define(moduleID, deps, func) {
        expect(moduleID).toBe('assert/empty-arguments-define');
        expect(deps).toMatchObject(['require']);
        resolve();
      }

      gulp.src(
          `${__dirname}\/assert/*.js`, {
          base: __dirname,
        },
      //  path.resolve(__dirname, 'assert/minify-define.js')
      ).pipe(amdWrap({})).pipe(
        gulp.dest(`${__dirname}\/dist\/`),
      ).on('end',
        () => {
          const code = readFileSync(file).toString();
          // tslint:disable-next-line:no-eval
          eval(code);
        },
      );

    });
  });
  it('exports arguments', () => {
    const file = `${__dirname}\/dist\/assert\/exports-define.js`;
    if (existsSync(file)) {
      unlinkSync(file);
    }

    return new Promise((resolve) => {
      function define(moduleID, deps, func) {
        expect(moduleID).toBe('assert/exports-define');
        expect(deps).toMatchObject(['require', 'exports', 'module']);
        resolve();
      }

      gulp.src(
          `${__dirname}\/assert/*.js`, {
          base: __dirname,
        },
      //  path.resolve(__dirname, 'assert/minify-define.js')
      ).pipe(amdWrap({})).pipe(
        gulp.dest(`${__dirname}\/dist\/`),
      ).on('end',
        () => {
          const code = readFileSync(file).toString();
          // tslint:disable-next-line:no-eval
          eval(code);
        },
      );

    });
  });
});