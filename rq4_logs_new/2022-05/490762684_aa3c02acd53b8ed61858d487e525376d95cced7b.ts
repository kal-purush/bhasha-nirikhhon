import Preloader from 'core/boot/preloader';
import * as dotenv from 'dotenv-safe';

describe('Testing preloading functions', () => {
    let preloader;
    beforeAll(() => {
        dotenv.config();
        preloader = Preloader.init('en-US', 'Europe/Lisbon');
    });

    describe('handling preloader and verifying settings', () => {
        test('expect return locale en-uS', () => {
            expect(preloader.settings).toBeDefined();
            expect(preloader.settings.locale).toBe('en-US');
            expect.assertions(2);
        });

        test('expect return timezone Europe/Lisbon', () => {
            expect(preloader.settings).toBeDefined();
            expect(preloader.settings.timezone).toBe('Europe/Lisbon');
            expect.assertions(2);
        });
        test('expect return timezone Europe/Lisbon', () => {
            expect(preloader.settings).toBeDefined();
            expect(preloader.settings.timezone).toBe('Europe/Lisbon');
            expect.assertions(2);
        });
    });

    describe('handling preloading()', () => {
        test('expect return dir with getProjectDir()', () => {
            expect(preloader.preloading(() => 'testing')).toBe(undefined);
            expect.assertions(1);
        });
    });
});