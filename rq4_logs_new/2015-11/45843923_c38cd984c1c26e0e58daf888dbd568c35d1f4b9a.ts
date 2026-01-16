import {describe, beforeEach, expect, it, fakeAsync, tick} from 'angular2/testing';

import {DoorEffect} from './door-effect';
import {CssEffectTiming} from './css-effect-timing';

describe('DoorEffect', () => {
  let property;
  let timing;
  let cssTimingValue;
  let desiredDuration;
  let element:HTMLElement;
  let doorEffect:DoorEffect;

  beforeEach(() => {
    property = '-webkit-transform';
    timing = CssEffectTiming.LINEAR;
    cssTimingValue = CssEffectTiming.getCssValue(timing);
    desiredDuration = 50;
    element = document.createElement('div');
    doorEffect = new DoorEffect();
  });

  it('should be defined', () => {
  	expect(doorEffect).toBeDefined();
  });
  
  it('should update style of element after call startShow', () => {
    fakeAsync(() => {
      let actualDuration = doorEffect.startShow(element, desiredDuration, timing);

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
      let actualDuration = doorEffect.startHide(element, desiredDuration, timing);

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
      doorEffect.startShow(element, desiredDuration, timing);

      tick(1);

      expect(element.style.transitionTimingFunction).toBe(cssTimingValue);
      expect(element.style.transitionProperty).toBe(property);
      expect(element.style.transitionDuration).toBe(desiredDuration + 'ms');

      doorEffect.clearAnimation(element);

      tick(1);

      expect(element.style.transitionTimingFunction).not.toBe(cssTimingValue);
      expect(element.style.transitionProperty).not.toBe(property);
      expect(element.style.transitionDuration).not.toBe(desiredDuration + 'ms');
    })();
  });
});