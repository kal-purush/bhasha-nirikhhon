import i18n from 'i18next';
import { initReactI18next } from 'react-i18next';
import LanguageDetector from 'i18next-browser-languagedetector';
import enLocale from '../locales/en/translation.json';
import uaLocale from '../locales/ua/translation.json';

i18n
  .use(initReactI18next)
  .use(LanguageDetector)
  .init({
    lng: 'ua',
    interpolation: {
      escapeValue: false,
    },
    detection: {
      lookupLocalStorage: 'locale',
    },
    resources: {
      en: { translation: enLocale },
      ua: { translation: uaLocale },
    },
  });