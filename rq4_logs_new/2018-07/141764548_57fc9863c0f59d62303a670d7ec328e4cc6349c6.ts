import { InstrumentedElement } from '../../components/instrumented-element/instrumented-element';
import { TestWindow, getTestWindow, removeTestWindow } from '../window';
import blur from './blur';
import focus from './focus';

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

describe('blur', () => {
  let focusSteps = ['focus', 'focusin'];
  let blurSteps = ['blur', 'focusout'];

  let element: HTMLInstrumentedElementElement;
  let inputElement: HTMLInputElement;
  beforeEach(async () => {
    element = await buildElement(
      '<input id="hola" /><div id="not-focusable" />'
    );
    inputElement = element.querySelector('#hola') as HTMLInputElement;

    await focus('#hola');

    expect(element.getEvents()).toEqual(focusSteps);
    expect(getTestWindow().document.activeElement).toEqual(inputElement);

    element.resetEvents();
  });

  afterEach(() => {
    removeTestWindow();
  });

  test('does not run sync', async () => {
    let promise = blur('#hola');

    expect(element.getEvents()).toEqual([]);

    await promise;

    expect(element.getEvents()).toEqual(blurSteps);
  });

  test('rejects if selector is not found', async () => {
    try {
      await blur('#this-selector-does-not-exist');

      expect(false).toBeTruthy();
    } catch (e) {
      expect(e.message).toMatch(
        "Element not found when calling `blur('#this-selector-does-not-exist')`."
      );
    }
  });

  test('rejects if selector is not focusable', async () => {
    try {
      await blur('#not-focusable');

      expect(false).toBeTruthy();
    } catch (e) {
      expect(e.message).toMatch(/is not focusable/);
    }
  });

  test('bluring via selector', async () => {
    await blur('#hola');

    expect(element.getEvents()).toEqual(blurSteps);
    expect(getTestWindow().document.activeElement).not.toEqual(inputElement);
  });

  test('bluring via element', async () => {
    await blur(inputElement);

    expect(element.getEvents()).toEqual(blurSteps);
    expect(getTestWindow().document.activeElement).not.toEqual(inputElement);
  });
});