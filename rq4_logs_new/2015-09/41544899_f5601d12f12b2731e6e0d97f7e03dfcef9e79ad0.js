/*jshint expr: true*/
var chai = require('chai'),
  expect = chai.expect,
  command = require('./../lib/command'),
  program = require('commander');
  
  program.option('-u'); // adds the -u flag that is added by mocha

describe('command', function() {
  
  it('should export a command module', function(){
    expect(typeof command.command).to.equal('function');
  });
  
  it('should return an object with a start date', function(){
    program.start = '2015-09-15';
    var cli = command.command([]);
    
    expect(cli.start).to.equal('2015-09-15');
    
  });
  
});