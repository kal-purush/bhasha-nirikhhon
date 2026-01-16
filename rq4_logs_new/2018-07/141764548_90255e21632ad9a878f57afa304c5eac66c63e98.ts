import { TestWindow, getTestWindow, removeTestWindow } from '../window';
import { InstrumentedElement } from '../../components/instrumented-element/instrumented-element';
import click from './click';
import getElement from './get-element';

async function buildElement(
  innerHtml: string
): Promise<HTMLInstrumentedElementElement> {
  let window = new TestWindow();
  let element = await window.load({
    components: [InstrumentedElement],
    html: `<instrumented-element>${innerHtml}</instrumented-element>`,
  });

  return element as HTMLInstrumentedElementElement;
}

describe('click', () => {
  afterEach(() => {
    removeTestWindow();
  });

  describe('non-focusable element types', () => {
    test('clicking a div via selector', async () => {
      let element = await buildElement('<div id="hola"></div>');

      await click('#hola');

      expect(element.getEvents()).toEqual(['mousedown', 'mouseup', 'click']);
    });

    test('clicking a div via element', async () => {
      let element = await buildElement('<div id="hola"></div>');

      let holaDiv = element.querySelector('#hola');

      await click(holaDiv);

      expect(element.getEvents()).toEqual(['mousedown', 'mouseup', 'click']);
    });

    test('does not run sync', async () => {
      let element = await buildElement('<div id="hola"></div>');

      let promise = click('#hola');

      expect(element.getEvents()).toEqual([]);

      await promise;

      expect(element.getEvents()).toEqual(['mousedown', 'mouseup', 'click']);
    });

    test('rejects if selector is not found', async () => {
      await buildElement('<div id="hola"></div>');

      try {
        await click('#adios');
        expect(false).toBeTruthy();
      } catch (e) {
        expect(true).toBeTruthy();
      }
    });
  });

  describe('focusable element types', () => {
    let clickSteps = ['mousedown', 'focus', 'focusin', 'mouseup', 'click'];

    test('clicking a input via selector', async () => {
      let element = await buildElement('<input id="hola"></input>');

      await click('#hola');

      expect(element.getEvents()).toEqual(clickSteps);
      expect(getTestWindow().document.activeElement).toEqual(
        element.querySelector('#hola')
      );
    });

    test('clicking a input via element', async () => {
      let element = await buildElement('<input id="hola"></input>');
      let input = getElement('#hola');

      await click(input);

      expect(element.getEvents()).toEqual(clickSteps);
      expect(getTestWindow().document.activeElement).toEqual(input);
    });

    test('clicking a disabled input does nothing', async () => {
      let element = await buildElement('<input id="hola" disabled></input>');
      let input = getElement('#hola');

      await click(element);

      expect(element.getEvents()).toEqual([]);
      expect(getTestWindow().document.activeElement).not.toEqual(input);
    });
  });

  describe('elements in different realms', () => {
    test('clicking an element in a different realm', async () => {
      let element = await buildElement(
        '<div><iframe></iframe></div>' // need the wrapper div for attaching the event listeners
      );
      let iframeDocument = element.querySelector('iframe').contentDocument;
      let iframeElement = iframeDocument.createElement('div');
      element.instrumentElement(iframeElement);

      await click(iframeElement);

      expect(element.getEvents()).toEqual(['mousedown', 'mouseup', 'click']);
    });
  });
});