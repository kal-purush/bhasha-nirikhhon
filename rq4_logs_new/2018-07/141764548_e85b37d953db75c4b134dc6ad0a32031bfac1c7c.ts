import { TestWindow, removeTestWindow, getTestWindow } from '../window';
import { InstrumentedElement } from '../../components/instrumented-element/instrumented-element';
import doubleClick from './double-click';

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

describe('doubleClick', () => {
  afterEach(() => {
    removeTestWindow();
  });

  describe('non-focusable element types', () => {
    let clickSteps = [
      'mousedown',
      'mouseup',
      'click',
      'mousedown',
      'mouseup',
      'click',
      'dblclick',
    ];

    test('double-clicking a div via selector', async () => {
      let element = await buildElement('<div id="hola"></div>');

      await doubleClick('#hola');

      expect(element.getEvents()).toEqual(clickSteps);
    });

    test('double-clicking a div via element', async () => {
      let element = await buildElement('<div id="hola"></div>');
      let div = element.querySelector('#hola');

      await doubleClick(div);

      expect(element.getEvents()).toEqual(clickSteps);
    });

    test('does not run sync', async () => {
      let element = await buildElement('<div id="hola"></div>');

      let promise = doubleClick('#hola');

      expect(element.getEvents()).toEqual([]);

      await promise;

      expect(element.getEvents()).toEqual(clickSteps);
    });

    test('rejects if selector is not found', async () => {
      await buildElement('<div/>');

      try {
        await doubleClick('#this-element-does-not-exist');
        expect(false).toBeTruthy();
      } catch (e) {
        expect(true).toBeTruthy();
      }
    });
  });

  describe('focusable element types', () => {
    let clickSteps = [
      'mousedown',
      'focus',
      'focusin',
      'mouseup',
      'click',
      'mousedown',
      'mouseup',
      'click',
      'dblclick',
    ];

    test('double-clicking a input via selector', async () => {
      let element = await buildElement('<input id="hola" />');

      await doubleClick('#hola');

      expect(element.getEvents()).toEqual(clickSteps);
      expect(getTestWindow().document.activeElement).toEqual(
        element.querySelector('#hola')
      );
    });

    test('double-clicking a input via element', async () => {
      let element = await buildElement('<input id="hola" />');
      let input = element.querySelector('#hola');

      await doubleClick(input);

      expect(element.getEvents()).toEqual(clickSteps);
      expect(getTestWindow().document.activeElement).toEqual(input);
    });
  });

  describe('elements in different realms', () => {
    test('clicking an element in a different realm', async () => {
      let element = await buildElement(
        '<iframe></iframe>' // need the wrapper div for attaching the event listeners
      );
      let iframeDocument = element.querySelector('iframe').contentDocument;
      let iframeElement = iframeDocument.createElement('div');
      element.instrumentElement(iframeElement);

      await doubleClick(iframeElement);

      expect(element.getEvents()).toEqual([
        'mousedown',
        'mouseup',
        'click',
        'mousedown',
        'mouseup',
        'click',
        'dblclick',
      ]);
    });
  });
});