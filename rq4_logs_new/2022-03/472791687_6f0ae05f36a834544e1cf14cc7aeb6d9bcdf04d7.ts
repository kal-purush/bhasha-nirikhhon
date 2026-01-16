import { higherThan, hasSubstring, multiply } from './Functions';

describe('Функции', () => {
    it('higherThan', () => {
        expect([1, 3, 10, 0, 2, 6].filter(higherThan(5))).toStrictEqual([
            10, 6,
        ]);
        expect([15, 99, 2, 5, 100].filter(higherThan(100))).toStrictEqual([]);

        expect([].filter(higherThan(-1))).toStrictEqual([]);
    });

    it('hasSubstring', () => {
        expect(
            ['ABC', 'bc', 'abc', 'bCa', 'bca'].filter(hasSubstring('bc')),
        ).toStrictEqual(['bc', 'abc', 'bca']);

        expect(
            ['ABC', 'bc', 'abc', 'bCa', 'bca'].filter(hasSubstring('')),
        ).toStrictEqual(['ABC', 'bc', 'abc', 'bCa', 'bca']);

        expect(
            ['ghfdlwq', 'bc', 'abc', 'iytrtio', 'rtiieu'].filter(
                hasSubstring('rti'),
            ),
        ).toStrictEqual(['iytrtio', 'rtiieu']);
    });

    it('multiply', () => {
        expect(multiply(5)(10)).toBe(50);
        expect(multiply(3)(5)).toBe(15);
        expect(multiply(2)(-5)).toBe(-10);
    });
});