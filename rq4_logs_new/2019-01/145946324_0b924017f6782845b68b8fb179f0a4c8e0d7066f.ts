import {MinMaxValue} from '../../src/Models/MinMaxValue';

describe('Class MinMaxValue tests', () => {
    it('should exist', () => {
        const minMax = new MinMaxValue(50, 0, 100);
        expect(minMax).toBeDefined();
    });

    it('should return expected values for values >= 0', () => {
        const minMax = new MinMaxValue(50, 0, 100);
        expect(minMax.transformation(0, 50)).toBe(25);
        expect(minMax.transformation(0, 100)).toBe(50);
        expect(minMax.transformation(10, 100)).toBe(55);
        expect(minMax.transformation(60, 100)).toBe(80);
    });

    it('should return expected values for min value < 0', () => {
        const minMax = new MinMaxValue(50, 0, 100);
        expect(minMax.transformation(20, 100)).toBe(60);
    });

    it('should return expected values for min value < 0', () => {
        const minMax = new MinMaxValue(0, -100, 100);
        expect(minMax.transformation(50, 100)).toBe(75);
    });

    it('should return expected values reverseTransformation for values >= 0', () => {
        const minMax = new MinMaxValue(50, 0, 100);
        expect(minMax.reverseTransformation(40, 20, 80)).toBe(Math.round(100 / 3));
        expect(minMax.reverseTransformation(30, 20, 60)).toBe(25);
        expect(minMax.reverseTransformation(50, 0, 100)).toBe(50);
        expect(minMax.reverseTransformation(50, 0, 50)).toBe(100);
    });

    it('should return expected values reverseTransformation for values < 0', () => {
        const minMax = new MinMaxValue(20, -100, 100);
        expect(minMax.reverseTransformation(20, 20, 80)).toBe(-100);
        expect(minMax.reverseTransformation(50, 20, 80)).toBe(0);
        expect(minMax.reverseTransformation(25, 0, 100)).toBe(-50);
    });
});