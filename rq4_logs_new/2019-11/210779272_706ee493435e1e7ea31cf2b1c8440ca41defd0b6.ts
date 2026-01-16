import { getCleanStatistics } from '../../../src/statistics/helper';
import { Statistics } from '../../../src/statistics';

describe('getCleanStatistics', () => {
    it('getCleanStatistics_partial_complete', () => {
        expect(getCleanStatistics({ students: 10 })).toEqual({
            internships: { total: 0, availables: 0, validated: 0 },
            students: 10,
            mentors: 0,
        });
    });

    it('getCleanStatistics_empty_complete', () => {
        expect(getCleanStatistics({})).toEqual({
            internships: { total: 0, availables: 0, validated: 0 },
            students: 0,
            mentors: 0,
        });
    });

    it('getCleanStatistics_full_same', () => {
        const VALID_COMPLET: Statistics = {
            internships: { total: 250, availables: 150, validated: 100 },
            students: 150,
            mentors: 200,
            campaign: 0,
        };
        expect(getCleanStatistics(VALID_COMPLET)).toEqual(VALID_COMPLET);
    });
});