import longValidator from '../frankulatorValidators/longValidator';
import expect from 'expect';

describe(longValidator, function(){
  it('expects very long number to be passed down', function() {
    expect(longValidator('5.3344115463038834e+243')).toEqual(5.3344115463038834e+243);
  });
  it('expects blank string to prompt Please enter a number', function() {
    expect(longValidator()).toEqual("Please enter a number");
  });
  it('expects string w/ alpha chars. to prompt Please enter a number', function() {
    expect(longValidator("abcDEF")).toEqual("Please enter a number");
  });
});