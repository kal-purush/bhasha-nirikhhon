import i18n from 'i18next';
import { initReactI18next } from 'react-i18next';
import COMMON_JP from '../../Assets/I18n/Jp/common.json';
import WEB_APP_JP from '../../Assets/I18n/Jp/web-app.json';
import COMMON_EN from '../../Assets/I18n/En/common.json';
import WEB_APP_EN from '../../Assets/I18n/En/web-app.json';
import LanguageDetector from 'i18next-browser-languagedetector';

const resources = {
  jp: {
    common: COMMON_JP,
    web_app: WEB_APP_JP
  },
  en: {
    common: COMMON_EN,
    web_app: WEB_APP_EN
  }
}

i18n.use(initReactI18next)
.use(LanguageDetector)
.init({
  resources,
  fallbackLng: 'en',
  interpolation: {
    escapeValue: false
  }
})

export default i18n;