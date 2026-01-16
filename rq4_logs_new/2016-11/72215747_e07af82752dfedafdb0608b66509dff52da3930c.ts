import { ObjectMapper } from '../src/';
import { BooleanTests } from './models/';
import { testValues } from './constants/';

describe('boolean tests', () => {
    let mapper: ObjectMapper;
    let booleanTests: BooleanTests;

    beforeAll(() => {
        mapper = new ObjectMapper();
        booleanTests = mapper.readValue<BooleanTests>(testValues, BooleanTests);
    });

    it('should correctly handle a null value', () => {
        expect(booleanTests.nullValue).toBeNull();
    });

    it('should correctly handle an undefined value', () => {
        expect(booleanTests.undefinedValue).toBeUndefined();
    });

    it('should correctly handle a false value', () => {
        expect(booleanTests.falseValue).toEqual(false);
    });

    it('should correctly handle a true value', () => {
        expect(booleanTests.trueValue).toEqual(true);
    });

    it('should correctly handle an empty string', () => {
        expect(booleanTests.emptyString).toEqual(false);
    });

    it('should correctly handle a non-empty string', () => {
        expect(booleanTests.nonemptyString).toEqual(true);
    });

    it('should correctly handle an empty array', () => {
        expect(booleanTests.emptyArray).toEqual(true);
    });

    it('should correctly handle a non-empty array', () => {
        expect(booleanTests.nonemptyArray).toEqual(true);
    });

    it('should correctly handle an object', () => {
        expect(booleanTests.object).toEqual(true);
    });

    it('should correctly handle a -1', () => {
        expect(booleanTests.negativeOne).toEqual(true);
    });

    it('should correctly handle a 0', () => {
        expect(booleanTests.zero).toEqual(false);
    });

    it('should correctly handle a 1', () => {
        expect(booleanTests.one).toEqual(true);
    });
});