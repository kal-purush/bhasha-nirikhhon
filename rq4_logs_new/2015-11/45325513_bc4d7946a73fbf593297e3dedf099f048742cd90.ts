/// <reference path="../typings/tsd.d.ts" />


import {NodePackageManager} from "../lib/packagers/NodePackageManager"

var chai = require('chai');
var del = require('del');
var fs = require('fs');
var path = require('path');
var pkg:any = require(path.resolve('./package.json'));

var testPackagePath:string = path.resolve(__dirname,'./package.json');


class MockNodePackageManager extends NodePackageManager
{
    constructor() {
        super();
        this.configFilePath = testPackagePath;
        this.pkg = require(this.configFilePath);
    }
}


function getPackage():any{
    return require(testPackagePath);
}

describe('NodePackageManager Test cases', () => {

    var expect = chai.expect;
    var npm:MockNodePackageManager;
    var currentNpm:NodePackageManager;

    before(() =>{
      fs.writeFileSync(testPackagePath, JSON.stringify({version:"0.0.1"}, null, '  ') + '\n');
      npm = new MockNodePackageManager();
      currentNpm = new NodePackageManager();

    });

    after(() =>{

        del.sync([testPackagePath]);

    });

    describe('version()', () => {

        it('should return the version number of package', function(done) {

            expect(npm.version()).to.equal('0.0.1');
            done();
        });

        it('should return the version number of current package', function(done) {

            expect(currentNpm.version()).to.equal(pkg.version);
            done();
        });


    });


    describe('bump(version)', () => {

        it('should return a promise', function(done) {
            expect(npm.bump("")).to.be.instanceOf(Promise);
            done();
        });


        it('should reject promise if the version type is not Major, Minor or Patch', function(done) {

            npm.bump("").then((result) =>{}, (error:string) =>{
                expect(error).to.equal(NodePackageManager.ERROR_VERSION_TYPE_NOT_SUPPORTED);
                done();
            })

        });


        it('should bump patch version', function(done) {
            npm.bump("patch").then((result:string) =>{
                expect(result).to.equal("0.0.2");
                expect(result).to.equal(getPackage().version);
                done();
            })
        });


        it('should bump minor version', function(done) {
            npm.bump("minor").then((result:string) =>{
                expect(result).to.equal("0.1.0");
                expect(result).to.equal(getPackage().version);
                done();
            })
        });

        it('should bump major version', function(done) {
            npm.bump("major").then((result:string) =>{
                expect(result).to.equal("1.0.0");
                expect(result).to.equal(getPackage().version); //making sure the package being managed has also changed
                done();
            })
        });

    })

});




