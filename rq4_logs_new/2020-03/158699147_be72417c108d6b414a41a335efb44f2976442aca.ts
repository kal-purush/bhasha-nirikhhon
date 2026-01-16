import { lagModiacontextholderUrl } from './api';
import { withLocation } from '../utils/test.utils';

describe('api', () => {
    describe('modiacontextholder-url', () => {
        it('skal ha riktig url ved kjøring lokalt', () => {
            withLocation('localhost', () => {
                expect(lagModiacontextholderUrl()).toBe('/modiacontextholder/api');
            });
        });

        it('skal ha riktig nais-url ved kjøring på nais-preprod', () => {
            withLocation('https://navn-q6.nais.preprod.local/contextpath', () => {
                expect(lagModiacontextholderUrl()).toBe(
                    'https://modiacontextholderapp-q6.nais.preprod.local/modiacontextholder/api'
                );
            });

            withLocation('https://navn.nais.preprod.local/contextpath', () => {
                expect(lagModiacontextholderUrl()).toBe(
                    'https://modiacontextholderapp-q0.nais.preprod.local/modiacontextholder/api'
                );
            });
        });

        it('skal ha riktig nais-url ved kjøring på nais-prod', () => {
            withLocation('https://navn-q.nais.adeo.no/contextpath', () => {
                expect(lagModiacontextholderUrl()).toBe(
                    'https://modiacontextholder.nais.adeo.no/modiacontextholder/api'
                );
            });
        });

        it('skal ha riktig url ved kjøring på app-XX.adeo.no', () => {
            withLocation('https://app-q6.adeo.no/contextpath', () => {
                expect(lagModiacontextholderUrl()).toBe(
                    'https://app-q6.adeo.no/modiacontextholder/api'
                );
            });
        });

        it('skal ha riktig url ved kjøring på app.adeo.no', () => {
            withLocation('https://app.adeo.no/contextpath', () => {
                expect(lagModiacontextholderUrl()).toBe(
                    'https://app.adeo.no/modiacontextholder/api'
                );
            });
        });

        it('skal ha fallback til prod om alt feiler', () => {
            withLocation('https://vg.no/contextpath', () => {
                expect(lagModiacontextholderUrl()).toBe(
                    'https://app.adeo.no/modiacontextholder/api'
                );
            });
        });
    });
});