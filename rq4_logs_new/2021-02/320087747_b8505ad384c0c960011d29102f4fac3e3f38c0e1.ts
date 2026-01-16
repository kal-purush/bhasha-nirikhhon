import { removeFromArray } from '../utils/ArrayUtils';

const ORIGINAL_ARRAY_INPUT = [5, 10, 15];

describe('ArrayUtils', () => {
    describe('removeFromArray', () => {
        describe(`with an array of ${ORIGINAL_ARRAY_INPUT.length} entries`, () => {
            let copiedInput: number[];
            let output: number[];

            beforeEach(() => {
                copiedInput = [...ORIGINAL_ARRAY_INPUT];
            });
    
            describe('when first entry is removed', () => {
                beforeEach(() => {
                    output = removeFromArray(copiedInput[0], copiedInput);
                });
    
                test(`should have a length of ${ORIGINAL_ARRAY_INPUT.length - 1}`, () => {
                    expect(output.length).toBe(ORIGINAL_ARRAY_INPUT.length - 1);
                })

                test(`should no longer have the removed value`, () => {
                    expect(output.indexOf(ORIGINAL_ARRAY_INPUT[0])).toBe(-1);
                })

                test(`should have the remaining values`, () => {
                    expect(output[0]).toBe(ORIGINAL_ARRAY_INPUT[1]);
                    expect(output[1]).toBe(ORIGINAL_ARRAY_INPUT[2]);
                });

                test(`should be reference equal to input`, () => {
                    expect(copiedInput).toEqual(output);
                })
            });
        });
    });
});