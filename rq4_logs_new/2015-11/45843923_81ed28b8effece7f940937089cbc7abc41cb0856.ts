import {
  describe,
  expect
} from 'angular2/testing';

import {CssEffectTiming} from './css-effect-timing';

describe('CssEffectTiming', () => {

  it('should be fine', () => {
  	expect(CssEffectTiming).toBeDefined();
  });
  
  it('should return linear css name of LINEAR timing', () => {
  	expect(CssEffectTiming.getCssValue(CssEffectTiming.LINEAR)).toBe('linear');
  });
  
  it('should return ease css name of EASE timing', () => {
  	expect(CssEffectTiming.getCssValue(CssEffectTiming.EASE)).toBe('ease');
  });
  
  it('should return ease-in css name of EASE_IN timing', () => {
  	expect(CssEffectTiming.getCssValue(CssEffectTiming.EASE_IN)).toBe('ease-in');
  });
  
  it('should return ease-in-out css name of EASE_IN_OUT timing', () => {
  	expect(CssEffectTiming.getCssValue(CssEffectTiming.EASE_IN_OUT)).toBe('ease-in-out');
  });
  
  it('should return ease-out css name of EASE_OUT timing', () => {
  	expect(CssEffectTiming.getCssValue(CssEffectTiming.EASE_OUT)).toBe('ease-out');
  });
});