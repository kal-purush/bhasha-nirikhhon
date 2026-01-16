// Dead simple translate module

interface Translations  {
    [locale: string]: {
        [phrase: string]: string
    }
}

class TranslateError extends Error {
    constructor(massage: string) {
        super(massage);
        this.message = massage;
    }
}

const translations : Translations= {
    fa: require('../locales/fa.json')
};
const locale = 'fa'; // Hard coded for now

export function t(phrase: string): string {
    if (!translations[locale]) {
        throw new TranslateError(`Unknonw locale ${locale}`);
    }

    if (!translations[locale][phrase]) {
        console.warn(`Translation for ${phrase} was not found.`);
        return `t(${phrase})`;
    }

    return translations[locale][phrase];
}