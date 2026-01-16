/// <reference path="../typings/bluebird/bluebird.d.ts"/>
/// <reference path="../typings/chai/chai.d.ts"/>
/// <reference path="../typings/debug/debug.d.ts"/>
/// <reference path="../typings/deep-diff/deep-diff.d.ts"/>
/// <reference path="../typings/lodash/lodash.d.ts"/>
/// <reference path="../typings/mocha/mocha.d.ts"/>
/// <reference path="../typings/node/node.d.ts"/>

'use strict';

import _ = require('lodash');
import chai = require('chai');
import childProcess = require('child_process');
import debug = require('debug');
import deepDiff = require('deep-diff');
import P = require('bluebird');
import path = require('path');
import types = require('../bin/types');

import Diff = types.PrettyDiff;

var dlog = debug('xsddiff:test');
var expect = chai.expect;

var script = path.join('bin', 'xsddiff.sh');
var testDataRoot = path.join('test', 'data');

interface ExecResult {
  stdout: string;
  stderr: string;
}

interface ExecCallback {
  (error: Error, result: ExecResult): void;
}

// Provide a standard Node.js async API to promisify.
function execFile(script: string, args: string[], callback: ExecCallback): void {

  // Wrap the callback so that it looks like what childProcess expects.
  function execCallback(error: Error, stdout: Buffer, stderr: Buffer) {
    var result: ExecResult = {
      stdout: stdout.toString(),
      stderr: stderr.toString()
    };
    if (error) {
      dlog('\n<error>\n', error, '\n</error>');
    }
    if (result.stdout.length > 0) {
      dlog('\n<stdout>\n', result.stdout, '\n</stdout>');
    }
    if (result.stderr.length > 0) {
      dlog('\n<stderr>\n', result.stderr, '\n</stderr>');
    }
    callback(error, result);
  }

  childProcess.execFile(script, args, execCallback);
}

var execFileP = P.promisify(execFile);

// Run the xsddiff script on the two files, and return the parsed result file.
function run(before: string, after: string): P<Diff[]> {
  var args: string[] = _.map([before, after], (f: string): string => path.join(testDataRoot, f + '.xsd'));
  return execFileP(script, args)
  // Just look at the stdout
    .then((result: ExecResult): string => result.stdout)
    .then((output: string): Diff[] => {
      var diffs = <Diff[]> JSON.parse(output);
      expect(_.isArray(diffs), 'Output should be JSON array').to.be.true;
      _.forEach(diffs, (diff: Diff): void => {
        expect(diff).to.include.keys(['kind', 'path']);
      });
      return diffs;
    });
}

describe('CLI', () => {

  it('produces no output on identical files', (): P<void> => {
    return run('baseline', 'baseline')
      .then((actual: Diff[]): void => {
        expect(actual).to.deep.equal([]);
      });
  });

  it('produces "N" record on added element', (): P<void> => {
    var expected: Diff[] = [
      {
        kind: 'N',
        path: 'xsd:schema/xsd:element/test/xsd:complexType//xsd:attribute',
        rhs: {
          added: {
            $: {
              name: 'added',
              type: 'test:string256_simpleType',
              use: 'optional'
            }
          }
        }
      }
    ];
    return run('baseline', 'element-added')
      .then((actual: Diff[]): void => {
        expect(actual).to.deep.equal(expected);
      });
  });

  it('produces "D" record on removed element', (): P<void> => {
    var expected: Diff[] = [
      {
        kind: 'D',
        path: 'xsd:schema/xsd:element/test/xsd:complexType//xsd:attribute',
        lhs: {
          added: {
            $: {
              name: 'added',
              type: 'test:string256_simpleType',
              use: 'optional'
            }
          }
        }
      }
    ];
    return run('element-added', 'baseline')
      .then((actual: Diff[]): void => {
        expect(actual).to.deep.equal(expected);
      });
  });

  it('produces "E" record on changed element', (): P<void> => {
    var expected: Diff[] = [
      {
        kind: 'E',
        path: 'xsd:schema/xsd:complexType/child1_complexType/xsd:attribute/datetime/$/type',
        lhs: 'xsd:dateTime',
        rhs: 'test:string256_simpleType'
      }
    ];
    return run('baseline', 'element-changed')
      .then((actual: Diff[]): void => {
        expect(actual).to.deep.equal(expected);
      });
  });

});