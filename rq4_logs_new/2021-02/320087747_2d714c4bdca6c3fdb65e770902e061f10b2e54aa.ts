import { calculateStartOfDate } from '../../../utils/DateUtils';

const referenceDate = new Date('Fri Feb 01 2021 00:00:00 GMT+0100')
const startDate = new Date('Fri Feb 01 2021 17:00:00 GMT+0100')

describe('DateUtils', () => {
    describe(`using date ${startDate}`, () => {
        describe('calling calculateStartOfDate', () => {
           let result: Date;

           beforeEach(() => {
               result = calculateStartOfDate(startDate);
           })

           test(`should normalize to ${referenceDate}`, () => {
               expect(Number(result)).toBe(Number(referenceDate));
           })
        });
    })
});