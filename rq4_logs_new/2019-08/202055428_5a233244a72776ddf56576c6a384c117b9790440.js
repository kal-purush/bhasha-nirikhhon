'use strict';

const validator = require('../lib/validator.js');

// Jo - variables for type validation below
let str = 'yes';
let num = 1;
let arr = ['a'];
let obj = {x:'y'};
let func = () => {};
let bool = false;

describe('validator module performs basic numeric validation', () => {
  it('positive', () => {
    expect(validator.isValid(10, 'positive')).toBeTruthy();
    expect(validator.isValid(-1, 'positive')).toBeFalsy();
    expect(validator.isValid(0, 'positive')).toBeFalsy();
  });

  it('even-numbers', () => {
    expect(validator.isValid(10, 'even-numbers')).toBeTruthy();
    expect(validator.isValid(-1, 'even-numbers')).toBeFalsy();
    expect(validator.isValid(0, 'even-numbers')).toBeFalsy();
  });
})

// TODO: Make this series of tests less repetitive ... DRY it out
describe('validation of string type', () => {
  // Regular cases
  it('valid strings', () => {
    expect(validator.isString(str)).toBeTruthy();
  });

  it('invalid strings', () => {
    let invalidData = [1, [], {}, ()=>{}, true];
    for(let invalidValue of invalidData){
      expect(validator.isString(invalidValue)).toBeFalsy();
    }
  });
});

describe('validation of numeric type', () => {
  it('numbers', () => {
    expect(validator.isNum(str)).toBeFalsy();
    expect(validator.isNum(num)).toBeTruthy();
    expect(validator.isNum(arr)).toBeFalsy();
    expect(validator.isNum(obj)).toBeFalsy();
    expect(validator.isNum(func)).toBeFalsy();
    expect(validator.isNum(bool)).toBeFalsy();
  });
});

describe('validation of arrays', () => {
  it('arrays', () => {
    expect(validator.isArray(str)).toBeFalsy();
    expect(validator.isArray(num)).toBeFalsy();
    expect(validator.isArray(arr)).toBeTruthy();
    expect(validator.isArray(obj)).toBeFalsy();
    expect(validator.isArray(func)).toBeFalsy();
    expect(validator.isArray(bool)).toBeFalsy();
  });
});

xdescribe('validation of objects', () => {
  it('objects', () => {
    expect(true).toBeFalsy();
  });
});

xdescribe('validation of booleans', () => {
  it('booleans', () => {
    expect(true).toBeFalsy();
  });
});

xdescribe('validation of functions', () => {
  it('functions', () => {
    expect(true).toBeFalsy();
  });
});

describe('validator module performs complex validations', () => {
  let person = {
    name: 'me',
    hair: {color: 'black'},
    age: 10,
    favoriteFoods: ['fish', 'chips']
  };

  it('validates the presence of required object properties at any level', () => {
    // i.e. does person.hair.color exist and have a good value, not just person.hair
    expect(validator.checkObjProperty('nationality', person)).toBeFalsy();
    expect(validator.checkObjProperty('hair', person)).toBeTruthy();
  });

  it('validates the proper types of object properties', () => {
    // i.e. person.name must be a string, etc.
    expect(validator.isString(person.name)).toBeTruthy();
    expect(validator.isString(person.age)).toBeFalsy();
  });

  it('validates the types of values contained in an array', () => {
    // i.e. an array of all strings or numbers
    expect(validator.checkArrayValues(validator.isString(), person.favoriteFoods)).toBeTruthy();
  });

  // it('validates a value array against an approved list', () => {
  //   // i.e. a string might only be allowed to be "yes" or "no"
  //   expect(true).toBeFalsy();
  // });

  // TODO: Cover so, so many more cases

});
