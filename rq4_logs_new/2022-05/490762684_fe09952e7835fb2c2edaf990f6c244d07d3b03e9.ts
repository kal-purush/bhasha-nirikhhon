import * as dotenv from 'dotenv-safe';
import momentTz from 'moment-timezone';
import moment, {} from 'moment';

interface IPreloaderSettings {
    locale: string;
    timezone: string;
}

class Preloader {
    protected settings: IPreloaderSettings;

    constructor(locale: string, timezone?: string) {
        this.settings = {
            locale,
            timezone: timezone || (process.env.TZ || 'Europe/Lisbon'),
        };
    }

    /**
     * Returns a new instance of Preloader class;
     * @param locale string
     * @param timezone string
     * @returns new Preloader();
     */
    static init(locale: string, timezone?: string) {
        return new this(locale, timezone);
    }

    /**
     * Load environment variables and set timezone and locale.
     */
    preloading(application: () => any): void {
        dotenv.config({
            allowEmptyValues: true,
        });

        moment.locale(this.settings.locale);

        momentTz.tz(this.settings.timezone);

        application();
    }
}

export default Preloader;