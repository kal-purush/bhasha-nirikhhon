/* tslint:disable-next-line no-var-requires */
const test = require('ninos')(require('ava'))

import { createCommand } from './Command';

test('createCommand', (t: any) => {
  const reject = t.context.stub();

  const aggregate = { name: 'counter', id: 'dummyId' }

  const { id, ...command } = createCommand(aggregate, 123, 'increment', reject, { step: 4 });

  const expectedCommand = {
    aggregate,
    reject,
    data: { step: 4 },
    name: 'increment',
    user: undefined,
    version: 123  
  }

  t.is(id.length, 36);
  t.deepEqual(command, expectedCommand);
})