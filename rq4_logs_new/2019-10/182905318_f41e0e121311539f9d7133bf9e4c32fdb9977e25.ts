import { readFileSync } from 'fs';
import * as path from 'path';
import { Parser } from '../src/parser';

const root = __dirname;
const filePath = path.resolve(root, './assert/one');
const content = readFileSync(filePath + '.js');
const debugPath = path.resolve(root, './assert/debug/index');
const debugContent = readFileSync(debugPath+ '.js');
const prefix = 'molecule';

describe('Alias Spec Test', () => {
  it('with prefix', () => {

    const debugCode = new Parser(debugContent, debugPath + '.js', root, prefix, [{
      moduleId: 'debug',
      path: debugPath,
      prefix: true
    }]).hook().getContent().toString();

    const code = new Parser(content, filePath + '.js', root, prefix, [{
      moduleId: 'debug',
      path: debugPath,
      prefix: true
    }]).hook().getContent().toString();

    console.log(debugCode);
    console.log(code);

    return new Promise((resolve, reject) => {
      let debugFn: any;
      function require() {
        expect(arguments[0]).toMatchObject(['molecule/debug']);
        expect(arguments[1](debugFn)).toBe('hao123');
        resolve();
      }
      function define(moduleID, deps, func) {
        if (moduleID === 'molecule/assert/one') {
          expect(deps).toMatchObject([
            'require', 'molecule/debug',
          ]);
          func(require, {}, {});
          resolve();
        } else if (moduleID === 'molecule/debug'){
          expect(deps).toMatchObject(['require']);
          debugFn = func(); // return hao123;
        } else {
          reject();
        }
      }
      // tslint:disable-next-line:no-eval
      eval(debugCode);
      eval(code);
    });
  });

  it('without prefix', () => {

    const debugCode = new Parser(debugContent, debugPath + '.js', root, prefix, [{
      moduleId: 'debug',
      path: debugPath,
      prefix: false
    }]).hook().getContent().toString();

    const code = new Parser(content, filePath + '.js', root, prefix, [{
      moduleId: 'debug',
      path: debugPath,
      prefix: false
    }]).hook().getContent().toString();

    console.log(debugCode);
    console.log(code);

    return new Promise((resolve, reject) => {
      let debugFn: any;
      function require() {
        expect(arguments[0]).toMatchObject(['debug']);
        expect(arguments[1](debugFn)).toBe('hao123');
        resolve();
      }
      function define(moduleID, deps, func) {
        if (moduleID === 'molecule/assert/one') {
          expect(deps).toMatchObject([
            'require', 'debug',
          ]);
          func(require, {}, {});
          resolve();
        } else if (moduleID === 'debug'){
          expect(deps).toMatchObject(['require']);
          debugFn = func(); // return hao123;
        } else {
          reject();
        }
      }
      // tslint:disable-next-line:no-eval
      eval(debugCode);
      eval(code);
    });
  });

});
