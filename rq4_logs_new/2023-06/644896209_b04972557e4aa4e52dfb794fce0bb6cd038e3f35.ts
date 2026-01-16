import i18n, { changeLanguage, ModuleType } from 'i18next';
import { initReactI18next } from 'react-i18next';
import * as resources from './resources';

import en from './resources/en';
import hi from './resources/hi';

const LANGUAGES = {
  en,
  hi,
};
type Languages = keyof typeof LANGUAGES;
interface LanguageDetector {
  type: ModuleType;
  async: boolean;
  detect: (callback: (code: string) => unknown) => void;
  init: () => unknown;
  cacheUserLanguage: (language: Languages) => void;
}
const LANG_CODES = Object.keys(LANGUAGES);
const LANGUAGE_DETECTOR: LanguageDetector = {
  type: 'languageDetector',
  async: true,
  detect: (callback) => {
    callback('en');
  },

  init: () => {},
  cacheUserLanguage: (language) => {
    // AsyncStorage.setItem("user-language", language);
  },
};

i18n
  // detect language
  .use(LANGUAGE_DETECTOR)
  // pass the i18n instance to react-i18next.
  .use(initReactI18next)
  // set options
  .init({
    compatibilityJSON: 'v3', //Add this line
    fallbackLng: 'en',
    resources: {
      ...Object.entries(resources).reduce(
        (acc, [key, value]) => ({
          ...acc,
          [key]: {
            translation: value,
          },
        }),
        {}
      ),
      LANGUAGES,
    },
    react: {
      useSuspense: false,
    },
    interpolation: {
      escapeValue: false,
    },
  });

export default i18n;