import { afterEach, beforeEach, describe, expect, it } from '@jest/globals';
import { noop } from '@proc7ts/primitives';
import { OrderTest } from '../../testing';
import { Unit } from '../../unit';
import { HandlerCommProtocol } from './handler.comm-protocol';

describe('HandlerCommProtocol', () => {

  beforeEach(() => {
    OrderTest.setup();
  });
  afterEach(() => {
    OrderTest.reset();
  });

  it('handles missing channel processor', () => {

    const protocol = new HandlerCommProtocol({
      channelProcessor: noop,
    });

    expect(protocol.channelProcessor(new Unit())).toBeUndefined();
  });
});