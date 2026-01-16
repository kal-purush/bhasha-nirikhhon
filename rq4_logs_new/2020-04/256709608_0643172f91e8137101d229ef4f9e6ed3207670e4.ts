import i18n, { Resource } from 'i18next';
import { initReactI18next } from 'react-i18next';
import Backend from 'i18next-xhr-backend';
import LanguageDetector from 'i18next-browser-languagedetector';
import translationEN from '../locales/en/translation.json';
import translationVI from '../locales/vi/translation.json';

export const languageDefault = 'en';

const resources: Resource = {
  en: {
    translation: translationEN,
  },
  vi: {
    translation: translationVI,
  },
};

const initI18nService = () => {
  i18n
    .use(Backend)
    .use(LanguageDetector)
    .use(initReactI18next)
    .init({
      fallbackLng: 'en',
      lng: languageDefault,
      debug: false, // debug = true for showing console log in browser console
      resources,
      interpolation: {
        escapeValue: false,
      },
    });
};

export default initI18nService;