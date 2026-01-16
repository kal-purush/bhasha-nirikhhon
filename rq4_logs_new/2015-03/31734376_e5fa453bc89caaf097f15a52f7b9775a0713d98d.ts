/// <reference path="../lib/node-0.11.d.ts" />
/// <reference path="../lib/jasmine.d.ts" />
/// <reference path="../src/shellscript-ts.ts" />

var ssts = require('./shellscript-ts');
var fs = require('fs');
var rmdir = require('rimraf');

describe("shellscript-ts test suite", function() {

  beforeEach(function() {
    try {
      rmdir('/tmp/shellscript-ts/cache', (e)=> {});
    } catch(e) {
    }
  });

  it("ShellScriptTs runs fine with no option", function() {
    var me = new ssts.ShellScriptTs.ShellScriptTs();
    (<any>me).options = { argv: { _: [ './test/dummy.sh' ],
                                  ssts: { 'no-cache': false, verbose: false, help: false },
                                  '$0': 'node ./bin/shellscript-ts' } };
    me.execute();
  });

  it("ShellScriptTs runs fine with --ssts.no-cache", function() {
    var me = new ssts.ShellScriptTs.ShellScriptTs();
    (<any>me).options = { argv: { _: [ './test/dummy.sh' ],
                                  ssts: { 'no-cache': true, verbose: false, help: false },
                                  '$0': 'node ./bin/shellscript-ts' } };
    me.execute();
  });

  it("ShellScriptTs runs fine with --ssts.verbose", function() {
    var me = new ssts.ShellScriptTs.ShellScriptTs();
    (<any>me).options = { argv: { _: [ './test/dummy.sh' ],
                                  ssts: { 'no-cache': false, verbose: true, help: false },
                                  '$0': 'node ./bin/shellscript-ts' } };
    me.execute();
  });
});