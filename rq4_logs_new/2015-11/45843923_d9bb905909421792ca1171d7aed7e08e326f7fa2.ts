import {beforeEach, expect, fakeAsync, it, tick} from 'angular2/testing';

import {CssTransitionEffect, Css3TransitionEffectValues} from './css-transition-effect';
import {CssEffectTiming} from './css-effect-timing';

describe('Css3TransitionEffectValues', () => {

  let element:HTMLElement;
  let originalValues:Map<string, string>;

  beforeEach(() => {
    element = document.createElement('div');
    originalValues = new Map<string, string>().set('overflow', 'hidden');
  });

  it('should delay start of effect', () => {
    var ran = false;
    fakeAsync(() => {
      Css3TransitionEffectValues.delayStart(element, originalValues, () => {
        ran = true;
      });
      expect(ran).toEqual(false);
      tick(1);
      expect(ran).toEqual(true);
    })();
  });
});

describe('CssTransitionEffect', () => {

  let property;
  let timing;
  let cssTimingValue;
  let desiredDuration;
  let propertyValue;
  let element:HTMLElement;
  let cssTransitionEffect:CssTransitionEffect;

  beforeEach(() => {
    property = 'max-height';
    timing = CssEffectTiming.LINEAR;
    cssTimingValue = CssEffectTiming.getCssValue(timing);
    desiredDuration = 50;
    propertyValue = '';
    element = document.createElement('div');
    cssTransitionEffect = new CssTransitionEffect(property, new Map<string, string>().set('overflow', 'hidden'));
  });

  it('should be defined', () => {
  	expect(cssTransitionEffect).toBeDefined();
  });

  it('should update style of element after call startShow', () => {
    fakeAsync(() => {
      let actualDuration = cssTransitionEffect.startShow(element, desiredDuration, timing);

      expect(actualDuration).toBe(desiredDuration);

      expect(element.style.transitionTimingFunction).not.toBe(cssTimingValue);
      expect(element.style.transitionProperty).not.toBe(property);
      expect(element.style.transitionDuration).not.toBe(desiredDuration + 'ms');
      // expect(element.style.getPropertyValue(property)).not.toBe(propertyValue);

      tick(1);

      expect(element.style.transitionTimingFunction).toBe(cssTimingValue);
      expect(element.style.transitionProperty).toBe(property);
      expect(element.style.transitionDuration).toBe(desiredDuration + 'ms');
      // expect(element.style.getPropertyValue(property)).not.toBe(propertyValue);

    })();
  });

  it('should update style of element after call startHide', () => {
    fakeAsync(() => {
      let actualDuration = cssTransitionEffect.startHide(element, desiredDuration, timing);

      expect(actualDuration).toBe(desiredDuration);

      expect(element.style.transitionTimingFunction).not.toBe(cssTimingValue);
      expect(element.style.transitionProperty).not.toBe(property);
      expect(element.style.transitionDuration).not.toBe(desiredDuration + 'ms');
      // expect(element.style.getPropertyValue(property)).not.toBe(propertyValue);

      tick(1);

      expect(element.style.transitionTimingFunction).toBe(cssTimingValue);
      expect(element.style.transitionProperty).toBe(property);
      expect(element.style.transitionDuration).toBe(desiredDuration + 'ms');
      // expect(element.style.getPropertyValue(property)).not.toBe(propertyValue);

    })();
  });

  it('should clean style of element after call clearAnimation', () => {
    fakeAsync(() => {
      cssTransitionEffect.startShow(element, desiredDuration, timing);

      tick(1);

      expect(element.style.transitionTimingFunction).toBe(cssTimingValue);
      expect(element.style.transitionProperty).toBe(property);
      expect(element.style.transitionDuration).toBe(desiredDuration + 'ms');
      // expect(element.style.getPropertyValue(property)).not.toBe(propertyValue);

      cssTransitionEffect.clearAnimation(element);

      tick(1);

      expect(element.style.transitionTimingFunction).not.toBe(cssTimingValue);
      expect(element.style.transitionProperty).not.toBe(property);
      expect(element.style.transitionDuration).not.toBe(desiredDuration + 'ms');
      // expect(element.style.getPropertyValue(property)).not.toBe(propertyValue);


    })();
  });
});