import { describe, expect, it } from '@jest/globals';
import { hasProperties } from '../../src/helpers/common';

describe('checkForProps', () => {

  const propCases = [
     { obj: null, props: ['prop1', 'prop2', 'prop3'], expected: ['prop1', 'prop2', 'prop3'] },
     { 
      obj: { prop1: '', prop2: '', prop3: '' }, 
      props: ['prop1', 'prop2', 'prop3'], 
      expected: [] 
    },
    { 
      obj: { prop1: '', prop3: '' }, 
      props: ['prop1', 'prop2', 'prop3'], 
      expected: ['prop2'] 
    },
    { 
      obj: { 
        prop1: '',
        prop2: {
          level1: ''
        },
        prop3: {
          level1: {
            level2: ''
          }
        } 
      }, 
      props: ['prop1', 'prop2.level1', 'prop3.level1.level2'], 
      expected: [] 
    },
    { 
      obj: { 
        prop1: '',
        prop2: '',
        prop3: {
          level1: {
            level2: ''
          }
        } 
      }, 
      props: ['prop1', 'prop2.level1', 'prop3.level1.level2'], 
      expected: ['prop2.level1'] 
    },
    { 
      obj: { 
        prop1: '',
        prop3: {
          level1: {
            level2: ''
          }
        } 
      }, 
      props: ['prop1', 'prop2.level1', 'prop3.level1.level2'], 
      expected: ['prop2.level1'] 
    }
  ]

  it.each(propCases)('check for properties $props on obj $obj', ({ obj, props, expected }) => {
    expect(hasProperties(obj, props)).toEqual(expected);
  });
});