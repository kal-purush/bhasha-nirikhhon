
import * as test from 'tape';
import {Test} from 'tape';
import {v4 as uuid} from 'uuid';
import { stub } from 'sinon';

import {token} from './token';
import {Expression, isExpression, and} from './expression';
import { Node } from './node';

test('Expression', (t: Test) => {

  t.ok(isExpression(and({left: token('1'), right: token('2')})), 'isExpression returns true on Expression');

  t.end();
});