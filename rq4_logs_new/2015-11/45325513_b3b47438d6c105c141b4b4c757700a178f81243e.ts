/// <reference path="../typings/tsd.d.ts" />


import {GitRepository} from "../src/lib/repositories/GitRepository"
import fs = require('fs');
import del = require('del');
import chai = require('chai');
import path = require('path');

var expect = chai.expect;
var execSync = require('child_process').execSync;
var exec = require('child_process').exec;

var currentDirectory = process.cwd();
var tempDir:string = path.resolve("../temp");

function initializeRepository(done:Function):void
{
    fs.writeFileSync("package.json",JSON.stringify({version:"0.0.1"}, null, '  ') + '\n');
    execSync("git init");
    execSync("git add package.json");
    execSync('');
    exec('git commit -m "test commit"', function (err, stdout, stderr) {
        console.log(stdout);
        console.log(stderr);
        done();
    });
}

function unInitializeRepository():void
{
    del.sync(["package.json", ".git"],{force:true});
}
describe('GitRepository Test cases', () => {

    before(function(done){

        del.sync([tempDir],{force:true});
        fs.mkdirSync(tempDir);
        process.chdir(tempDir);
        console.log("changing Directory to: " + process.cwd());
        initializeRepository(done);

    });

    after(function(){
        del.sync([tempDir],{force:true});
        process.chdir(currentDirectory);
        console.log("changing Directory back to: " + process.cwd())
    });

    var git:GitRepository = new GitRepository();

    describe('isValid()', () => {

        beforeEach(function(done){
            initializeRepository(done);
        });

        it('should throe error if not a git repository', function(done) {

            unInitializeRepository();
            expect(git.isValid()).to.equal(false);
            done();
        });

        it('should resolve to true if valid git repository', function(done) {
            expect(git.isValid()).to.equal(true);
            done();
        });

    });

    describe('add()', () => {
        it('should add filenames provided', function(done) {
            done();
        });
    });

    describe('commit()', () => {

        it('should commit any files marked for commit', function(done) {
            done();
        });

        it('should commit with right commit message', function(done) {
            done();
        });
    });

    describe('tag()', () => {

        it('should sucessfully create tag for the repository', function(done) {
            done();
        });

    });

    describe('push()', () => {

        it('should reject if no remote repository configured', function(done) {
            done();
        });

        it('should resolve successfully if a remote repository is configured and the changes are pushed', function(done) {
            done();
        });

        it('should allow to push tags only', function(done) {
            done();
        });


    });
});




