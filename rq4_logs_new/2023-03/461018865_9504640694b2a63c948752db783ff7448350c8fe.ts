import type { Equal } from '../equal.js';
import type { TestingLibrary } from './type.js';

export function describeEqual({ describe, it, expect }: TestingLibrary) {
  return <T>(
    subject: Equal<T>,
    properties: {
      base: () => T;
      equivalent?: (base: T) => T | T[];
      different: (base: T) => T | T[];
    }
  ) => {
    const { base, equivalent, different } = properties;
    const toArray = (arrayOrValue: T | T[]) => (Array.isArray(arrayOrValue) ? arrayOrValue : [arrayOrValue]);

    describe('==', () => {
      const baseValue = base();
      it('should return true when left === right', () => {
        expect(subject['=='](baseValue, baseValue)).toBe(true);
      });
      if (equivalent != null) {
        it.each(toArray(equivalent(baseValue)))('should return true when left == right', (equivalentValue) => {
          expect(subject['=='](baseValue, equivalentValue)).toBe(true);
          expect(subject['=='](equivalentValue, baseValue)).toBe(true);
        });
      }
      it.each(toArray(different(baseValue)))('should return false when left != right', (differentValue) => {
        expect(subject['=='](baseValue, differentValue)).toBe(false);
        expect(subject['=='](differentValue, baseValue)).toBe(false);
      });
    });
    describe('!=', () => {
      const baseValue = base();

      it('should return true when left === right', () => {
        expect(subject['!='](baseValue, baseValue)).toBe(false);
      });
      if (equivalent != null) {
        it.each(toArray(equivalent(baseValue)))('should return false when left == right', (differentValue) => {
          expect(subject['!='](baseValue, differentValue)).toBe(false);
          expect(subject['!='](differentValue, baseValue)).toBe(false);
        });
      }
      it.each(toArray(different(baseValue)))('should return true when left != right', (differentValue) => {
        expect(subject['!='](baseValue, differentValue)).toBe(true);
        expect(subject['!='](differentValue, baseValue)).toBe(true);
      });
    });
  };
}