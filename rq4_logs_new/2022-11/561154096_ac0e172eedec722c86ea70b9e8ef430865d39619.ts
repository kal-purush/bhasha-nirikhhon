import { DEFAULT_STATE, reducer } from '../reducer.js';
import {
  founders,
  investors,
  seriesA,
  seriesB,
  seriesC,
  buildInitialState,
} from '../fixtures.js';
import { InvestorType } from '../types.js';

describe('reducer', () => {
  it('throws an error when an unknown action is received', () => {
    expect(() => {
      reducer(DEFAULT_STATE, {
        // @ts-expect-error Extra safety measure for JS consumers
        // (TS should catch unknown actions)
        type: 'UNKNOWN_ACTION',
      });
    }).toThrow('Unknown reducer action "UNKNOWN_ACTION"');
  });

  it('set the initital state', () => {
    const result = reducer(DEFAULT_STATE, {
      type: 'SET_INITIAL_STATE',
      payload: {
        exitDistribution: 60_000_000,
        investors,
      },
    });

    expect(result).toMatchObject({
      exitRemainder: 60_000_000,
      totalSharesRemainder: 3_000_000,
    });
  });

  describe('CHECK_AND_CONVERT_TO_COMMON', () => {
    it('converts preferred share class to common stock', () => {
      const initialState = buildInitialState({ exitDistribution: 60_000_000 });

      const result = reducer(initialState, {
        type: 'CHECK_AND_CONVERT_TO_COMMON',
        payload: seriesA,
      });

      expect(result.distribution[seriesA.shareClass]).toMatchObject({
        convertedToCommon: true,
      });
    });
  });

  describe('COMPUTE_PREFERRED_SHARE', () => {
    const initialState = buildInitialState({ exitDistribution: 60_000_000 });

    it('computes exit amount for preferred shares', () => {
      const result = reducer(initialState, {
        type: 'COMPUTE_PREFERRED_SHARE',
        payload: seriesC,
      });

      expect(result.exitRemainder).toEqual(45_000_000);
      expect(result.distribution[seriesC.shareClass]).toMatchObject({
        exitAmount: 15_000_000,
      });
    });

    it('does not compute exit amount for common stock', () => {
      const result = reducer(initialState, {
        type: 'COMPUTE_PREFERRED_SHARE',
        payload: founders,
      });

      expect(result.distribution[founders.shareClass]).toMatchObject({
        exitAmount: 0,
      });
    });

    it('does not compute exit amount for converted shares', () => {
      const result = reducer(
        reducer(initialState, {
          type: 'CHECK_AND_CONVERT_TO_COMMON',
          payload: seriesA,
        }),
        {
          type: 'COMPUTE_PREFERRED_SHARE',
          payload: seriesA,
        },
      );

      expect(result.distribution[seriesA.shareClass]).toMatchObject({
        exitAmount: 0,
      });
    });
  });

  describe('CHECK_AND_APPLY_CAP', () => {
    // TODO: handle common shares
    it('does not apply the cap to converted shares', () => {
      const initialState = buildInitialState({ exitDistribution: 60_000_000 });

      const result = reducer(
        reducer(
          reducer(initialState, {
            type: 'CHECK_AND_CONVERT_TO_COMMON',
            payload: seriesB,
          }),
          { type: 'COMPUTE_PREFERRED_SHARE', payload: seriesB },
        ),
        { type: 'CHECK_AND_APPLY_CAP', payload: seriesB },
      );

      expect(result.distribution[seriesB.shareClass]).toMatchObject({
        convertedToCommon: true,
        exitAmount: 0,
        isCapped: false,
      });
    });

    it('does not apply the cap to uncapped shares', () => {
      const initialState = buildInitialState({ exitDistribution: 60_000_000 });

      const uncappedShare: InvestorType = {
        ...seriesB,
        cap: false,
      };

      const result = reducer(
        reducer(initialState, {
          type: 'COMPUTE_PREFERRED_SHARE',
          payload: uncappedShare,
        }),
        {
          type: 'CHECK_AND_APPLY_CAP',
          payload: uncappedShare,
        },
      );

      expect(result.distribution[uncappedShare.shareClass]).toMatchObject({
        isCapped: false,
      });
    });

    it('applies the cap where necessary', () => {
      const initialState = buildInitialState({ exitDistribution: 60_000_000 });

      const result = reducer(initialState, {
        type: 'CHECK_AND_APPLY_CAP',
        payload: seriesA,
      });

      expect(result.distribution[seriesA.shareClass]).toMatchObject({
        isCapped: true,
        exitAmount: 1_800_000,
      });

      expect(result.exitRemainder).toBe(60_000_000 - 1_800_000);
      expect(result.totalSharesRemainder).toBe(3_000_000 - 200_000);
    });

    it('does not apply the cap if not necessary', () => {
      const initialState = buildInitialState({ exitDistribution: 10_000_000 });

      const result = reducer(initialState, {
        type: 'CHECK_AND_APPLY_CAP',
        payload: seriesB,
      });

      expect(result.distribution[seriesB.shareClass]).toMatchObject({
        isCapped: false,
      });
    });
  });

  describe('COMPUTE_PRO_RATA_SPLIT', () => {
    it('ignores capped shares', () => {
      const initialState = buildInitialState({ exitDistribution: 60_000_000 });

      const result = reducer(
        reducer(
          // I left out the convert-to-common step to keep the test simple
          reducer(initialState, {
            type: 'COMPUTE_PREFERRED_SHARE',
            payload: seriesB,
          }),
          {
            type: 'CHECK_AND_APPLY_CAP',
            payload: seriesB,
          },
        ),
        { type: 'COMPUTE_PRO_RATA_SPLIT', payload: seriesB },
      );

      expect(result.distribution[seriesB.shareClass]).toMatchObject({
        exitAmount: seriesB.purchasePrice * (seriesB.cap as number),
      });
    });

    // TODO: handle common shares
    it('performs pro-rata distribution for converted shares', () => {
      const initialState = buildInitialState({ exitDistribution: 60_000_000 });
      const expectedReturn = 6_000_000;

      const result = reducer(
        reducer(
          reducer(
            reducer(initialState, {
              type: 'CHECK_AND_CONVERT_TO_COMMON',
              payload: seriesB,
            }),
            { type: 'COMPUTE_PREFERRED_SHARE', payload: seriesB },
          ),
          { type: 'CHECK_AND_APPLY_CAP', payload: seriesB },
        ),
        { type: 'COMPUTE_PRO_RATA_SPLIT', payload: seriesB },
      );

      expect(result.distribution[seriesB.shareClass]).toMatchObject({
        exitAmount: expectedReturn,
      });
    });
  });
});