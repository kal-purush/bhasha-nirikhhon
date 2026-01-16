//import hello from './src/tests/hello';
import { expect } from 'chai';
// if you used the '@types/mocha' method to install mocha type definitions, uncomment the following line
// import 'mocha';
describe('Hello function', () => {
    it('should return hello world', () => {
        //const result = hello();
        console.log("kur");
        console.log(__dirname);
        console.log("kur2");
        expect("").to.equal('Hello World!');
  });
});