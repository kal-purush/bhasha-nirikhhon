import { buildElement } from '../utils.spec';
import { getTestWindow, removeTestWindow } from '../window';
import getElement from './get-element';
import tap from './tap';

describe('tap', () => {
  afterEach(() => {
    removeTestWindow();
  });

  describe('non-focusable element types', () => {
    test('tapping a div via selector', async () => {
      let element = await buildElement('<div id="hola" />');

      await tap('#hola');

      expect(element.getEvents()).toEqual([
        'touchstart',
        'touchend',
        'mousedown',
        'mouseup',
        'click',
      ]);
    });

    test('tapping a div via element', async () => {
      let element = await buildElement('<div id="hola" />');
      let div = getElement('#hola');

      await tap(div);

      expect(element.getEvents()).toEqual([
        'touchstart',
        'touchend',
        'mousedown',
        'mouseup',
        'click',
      ]);
    });

    test('does not run sync', async () => {
      let element = await buildElement('<div id="hola" />');

      let promise = tap('#hola');

      expect(element.getEvents()).toEqual([]);

      await promise;

      expect(element.getEvents()).toEqual([
        'touchstart',
        'touchend',
        'mousedown',
        'mouseup',
        'click',
      ]);
    });

    test('rejects if selector is not found', async () => {
      await buildElement('<div />');

      try {
        await tap('#it-does-not-exist');
      } catch (e) {
        expect(e.message).toMatch(
          "Element not found when calling `tap('#it-does-not-exist')`"
        );
      }
    });
  });

  describe('focusable element types', () => {
    let tapSteps = [
      'touchstart',
      'touchend',
      'mousedown',
      'focus',
      'focusin',
      'mouseup',
      'click',
    ];

    test('tapping an input via selector', async () => {
      let element = await buildElement('<input id="hola" />');
      let input = getElement('#hola');

      await tap('#hola');

      expect(element.getEvents()).toEqual(tapSteps);
      expect(getTestWindow().document.activeElement).toBe(input);
    });

    test('tapping an input via element', async () => {
      let element = await buildElement('<input id="hola" />');
      let input = getElement('#hola');

      await tap(input);

      expect(element.getEvents()).toEqual(tapSteps);
      expect(getTestWindow().document.activeElement).toBe(input);
    });
  });
});