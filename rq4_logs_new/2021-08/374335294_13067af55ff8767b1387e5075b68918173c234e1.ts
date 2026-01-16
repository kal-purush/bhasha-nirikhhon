import { beforeEach, describe, expect, it } from '@jest/globals';
import { CxBuilder } from '@proc7ts/context-builder';
import Order from '@sqdn/order';
import { Formation$Order } from './formation.order';

describe('Formation$Order', () => {

  let order: Order;

  beforeEach(() => {
    order = new CxBuilder<Formation$Order>(get => new Formation$Order('test-order', get)).context;
  });

  describe('entry', () => {
    it('is the same as `Order.entry`', () => {
      expect(order.entry).toBe(Order.entry);
    });
  });

  describe('current', () => {
    it('is the same as `Order.current`', () => {
      expect(order.current).toBe(Order.current);
    });
  });

  describe('active', () => {
    it('is always `true`', () => {
      expect(order.active).toBe(true);
    });
  });

  describe('orderId', () => {
    it('equals to provided one', () => {
      expect(order.orderId).toBe('test-order');
    });
  });

  describe('toString', () => {
    it('provides string representation', () => {
      expect(String(order)).toBe('[Order test-order]');
    });
  });
});