import test from 'ava';

import './_helpers';

import { createCommand } from '../Command';
import { createEvent } from '../Event';

test('applyCommand: events array', async (t: any) => {
  const { aggregate, initialState, reject } = t.context;
  const byValues = [1, 2, 3, 4, 5];
  const command = createCommand('incrementMultiple', reject, { byValues });
  const events = await aggregate.applyCommand(initialState, command);

  t.deepEqual(events, byValues.map(v => createEvent('incremented', { by: v })));
});

test('applyCommand: single event', async (t: any) => {
  const { aggregate, initialState, reject } = t.context;
  const command = createCommand('increment', reject, { by: 2 });
  const events = await aggregate.applyCommand(initialState, command);

  t.deepEqual(events, [createEvent('incremented', { by: 2 })]);
});

test('applyCommand: noop', async (t: any) => {
  const { aggregate, initialState, reject } = t.context;
  const command = createCommand('noop', reject);
  const events = await aggregate.applyCommand(initialState, command);
  t.deepEqual(events, []);
});