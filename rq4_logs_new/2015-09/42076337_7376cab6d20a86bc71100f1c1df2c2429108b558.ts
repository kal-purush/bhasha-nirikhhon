/// <reference path="../../../typings/mocha/mocha.d.ts" />
/// <reference path="../../../typings/chai/chai.d.ts" />

/**
 * Module dependencies.
 */
import chai = require('chai');
import { Person } from '../../code/models/person'

/**
 * Globals
 */

var expect = chai.expect;
class TestOutput implements Output {
    lastWrite : string;
    
    write(value : string) {
        this.lastWrite = value;
    }
}

/**
 * Unit tests
 */
describe('person', () => {
    var testOutput : TestOutput;
    var person : Person;
    
    beforeEach(() => {
        testOutput = new TestOutput();
        person = new Person("John Doe", 23);
    })
    
    describe('talk', () => {
        it('should include the person\'s name', done => {
           person.talk(testOutput, "stuff");
           
           expect(testOutput.lastWrite).to.equal("John Doe says \"stuff\"");
           done(); 
        });
    });
});