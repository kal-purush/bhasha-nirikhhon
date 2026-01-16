import { beforeEach, describe, expect, it, jest } from '@jest/globals';
import { CommBuffer } from '../comm-buffer';
import { fifoCommBuffer } from './fifo.comm-buffer';

describe('fifoCommBuffer', () => {
  it('evicts commands on overflow', () => {

    const onEvict = jest.fn();
    const buffer = fifoCommBuffer<number>(2);

    buffer.onEvict(onEvict);

    buffer.addRequest('request', {}, 1);
    expect(onEvict).not.toHaveBeenCalled();

    buffer.addSignal('signal', {}, 2);
    expect(onEvict).not.toHaveBeenCalled();

    buffer.addSignal('signal', {}, 3);
    expect(onEvict).toHaveBeenLastCalledWith(1);

    expect(buffer.pull()).toBe(2);
    buffer.addRequest('request', {}, 4);
    expect(onEvict).toHaveBeenCalledTimes(1);

    buffer.addSignal('signal', {}, 5);
    expect(onEvict).toHaveBeenLastCalledWith(3);
  });
  it('evicts commands from one-element buffer on overflow', () => {

    const onEvict = jest.fn();
    const buffer = fifoCommBuffer<number>(0);

    buffer.onEvict(onEvict);

    buffer.addSignal('signal', {}, 1);
    expect(onEvict).not.toHaveBeenCalled();

    buffer.addRequest('signal', {}, 2);
    expect(onEvict).toHaveBeenLastCalledWith(1);

    buffer.addSignal('signal', {}, 3);
    expect(onEvict).toHaveBeenLastCalledWith(2);

    expect(buffer.pull()).toBe(3);
    buffer.addRequest('signal', {}, 4);
    expect(onEvict).toHaveBeenCalledTimes(2);

    buffer.addSignal('signal', {}, 5);
    expect(onEvict).toHaveBeenLastCalledWith(4);
  });
  it('has `256` commands capacity by default', () => {

    const onEvict = jest.fn();
    const buffer = fifoCommBuffer<number>();

    buffer.onEvict(onEvict);

    for (let i = 0; i < 260; ++i) {
      buffer.addSignal(`signal-${i}`, {}, i);
    }

    expect(onEvict).toHaveBeenCalledTimes(4);
  });

  describe('pull', () => {

    let buffer: CommBuffer;

    beforeEach(() => {
      buffer = fifoCommBuffer();
    });

    it('returns `undefined` initially', () => {
      expect(buffer.pull()).toBeUndefined();
    });
    it('pulls values until exhausted', () => {
      buffer.addSignal('signal', {}, 1);
      buffer.addSignal('signal', {}, 2);
      buffer.addSignal('signal', {}, 3);
      expect(buffer.pull()).toBe(1);
      expect(buffer.pull()).toBe(2);
      expect(buffer.pull()).toBe(3);
      expect(buffer.pull()).toBeUndefined();
    });
  });
});