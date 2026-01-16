import { TRANSLATIONS, TRANSLATIONS_FORMAT, LOCALE_ID } from '@angular/core';

const translationMessageCollection = {
  es: require('./locale/messages.es'),
};

export function getTranslationProviders() {
    const languageCode = document['locale'].substring(0, 2);

    const translationMessageFile = translationMessageCollection[languageCode];

    if (translationMessageFile) {
        const translationMessages = translationMessageFile['MESSAGES'];
        return [
            {provide: TRANSLATIONS, useValue: translationMessages},
            {provide: TRANSLATIONS_FORMAT, useValue: 'xlf'},
            {provide: LOCALE_ID, useValue: languageCode}
        ];
    }
    return [];
}