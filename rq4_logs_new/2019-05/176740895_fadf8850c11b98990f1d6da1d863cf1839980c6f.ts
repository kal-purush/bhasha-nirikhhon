import { TypeBase } from './base';

describe('base class', (): void => {
  class SUT extends TypeBase {
    validate(): string {
      return null;
    }
    compare(): number {
      return 0;
    }
  }

  const sut = new SUT();

  describe('with negative compare value', (): void => {
    beforeEach((): void => {
      //Actual values passed to compare are not used!
      jest
        .spyOn(sut, 'compare')
        .mockReturnValue(-1)
        .mockClear();
    });

    it('equal', (): void => {
      expect(sut.eq('ignored', 'ignored')).toBe(false);
      expect(sut.compare).toBeCalledTimes(1);
    });
    it('lt', (): void => {
      expect(sut.lt('ignored', 'ignored')).toBe(true);
      expect(sut.compare).toBeCalledTimes(1);
    });
    it('lte', (): void => {
      expect(sut.lte('ignored', 'ignored')).toBe(true);
      expect(sut.compare).toBeCalledTimes(1);
    });
    it('gt', (): void => {
      expect(sut.gt('ignored', 'ignored')).toBe(false);
      expect(sut.compare).toBeCalledTimes(1);
    });
    it('gte', (): void => {
      expect(sut.gte('ignored', 'ignored')).toBe(false);
      expect(sut.compare).toBeCalledTimes(1);
    });
  });

  describe('with zero compare value', (): void => {
    beforeEach((): void => {
      //Actual values passed to compare are not used!
      jest
        .spyOn(sut, 'compare')
        .mockReturnValue(0)
        .mockClear();
    });

    it('equal', (): void => {
      expect(sut.eq('ignored', 'ignored')).toBe(true);
      expect(sut.compare).toBeCalledTimes(1);
    });
    it('lt', (): void => {
      expect(sut.lt('ignored', 'ignored')).toBe(false);
      expect(sut.compare).toBeCalledTimes(1);
    });
    it('lte', (): void => {
      expect(sut.lte('ignored', 'ignored')).toBe(true);
      expect(sut.compare).toBeCalledTimes(1);
    });
    it('gt', (): void => {
      expect(sut.gt('ignored', 'ignored')).toBe(false);
      expect(sut.compare).toBeCalledTimes(1);
    });
    it('gte', (): void => {
      expect(sut.gte('ignored', 'ignored')).toBe(true);
      expect(sut.compare).toBeCalledTimes(1);
    });
  });

  describe('with positive compare value', (): void => {
    beforeEach((): void => {
      //Actual values passed to compare are not used!
      jest
        .spyOn(sut, 'compare')
        .mockReturnValue(1)
        .mockClear();
    });

    it('equal', (): void => {
      expect(sut.eq('ignored', 'ignored')).toBe(false);
      expect(sut.compare).toBeCalledTimes(1);
    });
    it('lt', (): void => {
      expect(sut.lt('ignored', 'ignored')).toBe(false);
      expect(sut.compare).toBeCalledTimes(1);
    });
    it('lte', (): void => {
      expect(sut.lte('ignored', 'ignored')).toBe(false);
    });
    it('gt', (): void => {
      expect(sut.gt('ignored', 'ignored')).toBe(true);
      expect(sut.compare).toBeCalledTimes(1);
    });
    it('gte', (): void => {
      expect(sut.gte('ignored', 'ignored')).toBe(true);
      expect(sut.compare).toBeCalledTimes(1);
    });
  });
});