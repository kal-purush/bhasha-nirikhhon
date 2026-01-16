import {CREDIT_CARD_NUMBER_PATTERN } from './regex';

describe('credit card number', () => {
    it('card number containing 15 numbers should be valid', () => {
        let expression = new RegExp(CREDIT_CARD_NUMBER_PATTERN);
        let result = expression.test('123456789012345');
        expect(result).toBeTruthy();
    });
    it('card number containing 16 numbers should be valid', () => {
        let expression = new RegExp(CREDIT_CARD_NUMBER_PATTERN);
        let result = expression.test('1234567890123456');
        expect(result).toBeTruthy();
    });
    it('card number containing letter should not be valid', () => {
        let expression = new RegExp(CREDIT_CARD_NUMBER_PATTERN);
        let result = expression.test('12345678901234aa');
        expect(result).toBeFalsy();
    });
    it('card number with less characters should be invalid', () => {
        let expression = new RegExp(CREDIT_CARD_NUMBER_PATTERN);
        let result = expression.test('12345678901234');
        expect(result).toBeFalsy();
    });
});
