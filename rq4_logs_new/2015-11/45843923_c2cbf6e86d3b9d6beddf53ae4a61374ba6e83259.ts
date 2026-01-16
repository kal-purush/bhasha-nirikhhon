import {describe, expect, it} from 'angular2/testing';

import {Dom} from './dom';

describe('Dom', () => {

  it('should determines the default display value of an DIV element', () => {
    let defaultDisplay = Dom.getDefaultDisplay('div')
  	expect(defaultDisplay).toBe('block');
  });

  it('should determines the default display value of an SPAN element', () => {
    let defaultDisplay = Dom.getDefaultDisplay('span')
  	expect(defaultDisplay).toBe('inline');
  });

  it('should determines the default display value of an IFRAME element', () => {
    let defaultDisplay = Dom.getDefaultDisplay('iframe')
  	expect(defaultDisplay).toBe('inline');
  });
});