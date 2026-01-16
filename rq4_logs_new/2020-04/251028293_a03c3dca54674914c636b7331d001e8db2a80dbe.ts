import validateUrl from '../../../../../pages/api/services/bakalari/utils/validateUrl';

describe('validateUrl', () => {
    it('should be defined', () => {
        expect(validateUrl).toBeDefined();
    });

    it('should validate a url', () => {
        expect(validateUrl('some random string')).toBe(false);
    });

    it('should only accept https', () => {
        expect(validateUrl('http://some.website')).toBe(false);
    });

    it('should not accept trailing /', () => {
        expect(validateUrl('https://some.website/')).toBe(false);
    });

    it('should pass with a valid input', () => {
        expect(validateUrl('https://some.website')).toBe(true);
    });
});