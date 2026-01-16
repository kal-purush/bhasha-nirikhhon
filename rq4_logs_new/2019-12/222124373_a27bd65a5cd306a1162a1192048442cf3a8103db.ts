import { isNumber } from '@helper';
import { expect } from 'chai';

export default (): void => {
    describe('isNumber (value: number)', () => {
        it('should', () => {
            const result = isNumber(-1);

            expect(result).to.be.true;
        });

        it('should', () => {
            const result = isNumber(0);

            expect(result).to.be.true;
        });

        it('should', () => {
            const result = isNumber(1);

            expect(result).to.be.true;
        });
    });

    describe('isNumber (value: object)', () => {
        it('should', () => {
            const result = isNumber(undefined);

            expect(result).to.be.false;
        });

        it('should', () => {
            const result = isNumber(null);

            expect(result).to.be.false;
        });

        it('should', () => {
            const result = isNumber({});

            expect(result).to.be.false;
        });
    });

    describe('isNumber (value: string)', () => {
        it('should', () => {
            const result = isNumber('-1');

            expect(result).to.be.true;
        });

        it('should', () => {
            const result = isNumber('0');

            expect(result).to.be.true;
        });

        it('should', () => {
            const result = isNumber('1');

            expect(result).to.be.true;
        });

        it('should', () => {
            const result = isNumber('');

            expect(result).to.be.false;
        });

        it('should', () => {
            const result = isNumber('asdf');

            expect(result).to.be.false;
        });

        it('should', () => {
            const result = isNumber('0asdf');

            expect(result).to.be.false;
        });

        it('should', () => {
            const result = isNumber('0asdf0');

            expect(result).to.be.false;
        });

        it('should', () => {
            const result = isNumber('asdf0');

            expect(result).to.be.false;
        });
    });
};