import { readFileSync } from 'fs';
import * as path from 'path';
import { Parser } from '../src/parser';

const root = __dirname;
const filePath = path.resolve(root, './assert/multi-define');
const content = readFileSync(filePath + '.js');
const prefix = 'molecule';

describe('Alias Spec Test', () => {
  it('with prefix', () => {


    const code = new Parser(content, filePath + '.js', root, prefix).hook().getContent().toString();

    console.log(code);

    return new Promise((resolve, reject) => {
      let num = 0;
      function require() {

      }
      function define(moduleID, deps, func) {
        if (moduleID === 'molecule/assert/multi-define') {
          expect(func()).toEqual('multi');
          expect(num).toEqual(2);
          resolve();
        } else if(moduleID === './xx') {
          num++;
          expect(typeof deps).toEqual('function');
          expect(deps.toString().indexOf("require('one')") > -1).toEqual(true);
          expect(deps.toString().indexOf("require('./two')") > -1).toEqual(true);
        } else if(typeof  moduleID === 'function'){
          num++;
        } else {
          reject();
        }
      }

      eval(code);
    });
  });

});
