import {describe, beforeEach, expect, it, fakeAsync, tick} from 'angular2/testing';

import {ShrinkHeightEffect, ShrinkWidthEffect} from './shrink-effect';
import {CssEffectTiming} from './css-effect-timing';

describe('ShrinkHeightEffect', () => {
  let property;
  let timing;
  let cssTimingValue;
  let desiredDuration;
  let element:HTMLElement;
  let shrinkHeightEffect:ShrinkHeightEffect;

  beforeEach(() => {
    property = 'max-height';
    timing = CssEffectTiming.LINEAR;
    cssTimingValue = CssEffectTiming.getCssValue(timing);
    desiredDuration = 50;
    element = document.createElement('div');
    shrinkHeightEffect = new ShrinkHeightEffect();
  });

  it('should be defined', () => {
  	expect(shrinkHeightEffect).toBeDefined();
  });
  
  it('should update style of element after call startShow', () => {
    fakeAsync(() => {
      let actualDuration = shrinkHeightEffect.startShow(element, desiredDuration, timing);

      expect(actualDuration).toBe(desiredDuration);

      expect(element.style.transitionTimingFunction).not.toBe(cssTimingValue);
      expect(element.style.transitionProperty).not.toBe(property);
      expect(element.style.transitionDuration).not.toBe(desiredDuration + 'ms');

      tick(1);

      expect(element.style.transitionTimingFunction).toBe(cssTimingValue);
      expect(element.style.transitionProperty).toBe(property);
      expect(element.style.transitionDuration).toBe(desiredDuration + 'ms');
    })();
  });
  
  it('should update style of element after call startHide', () => {
    fakeAsync(() => {
      let actualDuration = shrinkHeightEffect.startHide(element, desiredDuration, timing);

      expect(actualDuration).toBe(desiredDuration);

      expect(element.style.transitionTimingFunction).not.toBe(cssTimingValue);
      expect(element.style.transitionProperty).not.toBe(property);
      expect(element.style.transitionDuration).not.toBe(desiredDuration + 'ms');

      tick(1);

      expect(element.style.transitionTimingFunction).toBe(cssTimingValue);
      expect(element.style.transitionProperty).toBe(property);
      expect(element.style.transitionDuration).toBe(desiredDuration + 'ms');
    })();
  });
  
  it('should clean style of element after call clearAnimation', () => {
    fakeAsync(() => {
      shrinkHeightEffect.startShow(element, desiredDuration, timing);

      tick(1);

      expect(element.style.transitionTimingFunction).toBe(cssTimingValue);
      expect(element.style.transitionProperty).toBe(property);
      expect(element.style.transitionDuration).toBe(desiredDuration + 'ms');

      shrinkHeightEffect.clearAnimation(element);

      tick(1);

      expect(element.style.transitionTimingFunction).not.toBe(cssTimingValue);
      expect(element.style.transitionProperty).not.toBe(property);
      expect(element.style.transitionDuration).not.toBe(desiredDuration + 'ms');
    })();
  });
});

describe('ShrinkWidthEffect', () => {
  let property;
  let timing;
  let cssTimingValue;
  let desiredDuration;
  let element:HTMLElement;
  let shrinkWidthEffect:ShrinkWidthEffect;

  beforeEach(() => {
    property = 'max-width';
    timing = CssEffectTiming.LINEAR;
    cssTimingValue = CssEffectTiming.getCssValue(timing);
    desiredDuration = 50;
    element = document.createElement('div');
    shrinkWidthEffect = new ShrinkWidthEffect();
  });

  it('should be defined', () => {
  	expect(shrinkWidthEffect).toBeDefined();
  });
  
  it('should update style of element after call startShow', () => {
    fakeAsync(() => {
      let actualDuration = shrinkWidthEffect.startShow(element, desiredDuration, timing);

      expect(actualDuration).toBe(desiredDuration);

      expect(element.style.transitionTimingFunction).not.toBe(cssTimingValue);
      expect(element.style.transitionProperty).not.toBe(property);
      expect(element.style.transitionDuration).not.toBe(desiredDuration + 'ms');

      tick(1);

      expect(element.style.transitionTimingFunction).toBe(cssTimingValue);
      expect(element.style.transitionProperty).toBe(property);
      expect(element.style.transitionDuration).toBe(desiredDuration + 'ms');
    })();
  });
  
  it('should update style of element after call startHide', () => {
    fakeAsync(() => {
      let actualDuration = shrinkWidthEffect.startHide(element, desiredDuration, timing);

      expect(actualDuration).toBe(desiredDuration);

      expect(element.style.transitionTimingFunction).not.toBe(cssTimingValue);
      expect(element.style.transitionProperty).not.toBe(property);
      expect(element.style.transitionDuration).not.toBe(desiredDuration + 'ms');

      tick(1);

      expect(element.style.transitionTimingFunction).toBe(cssTimingValue);
      expect(element.style.transitionProperty).toBe(property);
      expect(element.style.transitionDuration).toBe(desiredDuration + 'ms');
    })();
  });
  
  it('should clean style of element after call clearAnimation', () => {
    fakeAsync(() => {
      shrinkWidthEffect.startShow(element, desiredDuration, timing);

      tick(1);

      expect(element.style.transitionTimingFunction).toBe(cssTimingValue);
      expect(element.style.transitionProperty).toBe(property);
      expect(element.style.transitionDuration).toBe(desiredDuration + 'ms');

      shrinkWidthEffect.clearAnimation(element);

      tick(1);

      expect(element.style.transitionTimingFunction).not.toBe(cssTimingValue);
      expect(element.style.transitionProperty).not.toBe(property);
      expect(element.style.transitionDuration).not.toBe(desiredDuration + 'ms');
    })();
  });
});