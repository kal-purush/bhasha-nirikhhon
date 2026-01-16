import atest, { TestInterface } from 'ava';

import './_helpers';

import { createCommand } from '../Command';
import { makeVersionedEntity } from '../Entity';
import { createEvent } from '../Event';
import { IAggregate } from '../interfaces';

/* tslint:disable-next-line no-duplicate-imports */
import { ICounter } from './_helpers';

const test = atest as TestInterface<{ aggregate: IAggregate<ICounter> }>;

test('rehydrate', t => {
  const { aggregate } = t.context;
  const events = [1, 2, 3].map(by => createEvent('incremented', { by }));
  const entity = aggregate.rehydrate(events);
  t.is(entity.version, 3);
  t.is(entity.state.value, 6);
});

test('rehydrate from snapshot', t => {
  const { aggregate } = t.context;
  const events = [1, 2, 3].map(by => createEvent('incremented', { by }));

  const snapshottedEntity = makeVersionedEntity({
    state: { value: 100 },
    version: 25
  });

  const snapshot = makeVersionedEntity(snapshottedEntity);
  const entity = aggregate.rehydrate(events, snapshot);
  t.is(entity.version, snapshottedEntity.version + events.length);
  t.deepEqual(entity.state, { value: snapshottedEntity.state.value + 6 });
});

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