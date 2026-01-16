import { buildElement } from '../utils.spec';
import { removeTestWindow } from '../window';
import getElement from './get-element';
import triggerEvent from './trigger-event';

describe('trigger event', () => {
  afterEach(() => {
    removeTestWindow();
  });

  test('can trigger arbitrary event types', async () => {
    let element = await buildElement('<div id="hola" />');
    element.listenTo('fliberty', getElement('#hola'));

    await triggerEvent('#hola', 'fliberty');
  });

  test('triggering event via selector fires the given event type', async () => {
    let element = await buildElement('<div id="hola" />');

    await triggerEvent('#hola', 'mouseenter');

    expect(element.getEvents()).toEqual(['mouseenter']);
  });

  test('triggering event via element fires the given event type', async () => {
    let element = await buildElement('<div id="hola" />');
    let div = getElement('#hola');

    await triggerEvent(div, 'mouseenter');

    expect(element.getEvents()).toEqual(['mouseenter']);
  });

  test('does not run sync', async () => {
    let element = await buildElement('<div id="hola" />');
    let promise = triggerEvent('#hola', 'mouseenter');

    expect(element.getEvents()).toEqual([]);

    await promise;

    expect(element.getEvents()).toEqual(['mouseenter']);
  });

  test('rejects if selector is not found', async () => {
    await buildElement('<div id="hola" />');

    try {
      await triggerEvent('#it-does-not-exist', 'mouseenter');
    } catch (e) {
      expect(e.message).toMatch(
        "Element not found when calling `triggerEvent('#it-does-not-exist', ...)`"
      );
    }
  });

  test('events properly bubble upwards', async () => {
    let element = await buildElement(
      '<div><div id="outer"><div id="inner"></div></div></div>'
    );
    let outer = getElement('#outer');
    let inner = getElement('#inner');

    expect(outer).toBeTruthy();
    expect(inner).toBeTruthy();

    element.listenTo('mouseenter', outer, e => `outer: ${e}`);
    element.listenTo('mouseenter', inner, e => `inner: ${e}`);

    await triggerEvent('#inner', 'mouseenter');

    expect(element.getEvents()).toEqual([
      'inner: mouseenter',
      'outer: mouseenter',
      'mouseenter',
    ]);
  });
});