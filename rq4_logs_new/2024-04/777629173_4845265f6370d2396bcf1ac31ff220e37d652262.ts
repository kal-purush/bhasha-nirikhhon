import i18n from "i18next";
import { initReactI18next } from "react-i18next";
import Backend from "i18next-http-backend";

i18n
  .use(initReactI18next)
  .use(Backend)
  .init({
    backend: {
      loadPath: "/locales/{{lng}}/{{ns}}.json",
    },
    defaultNS: "common",
    lng: "sv-SE",
    fallbackLng: "sv-SE",
    debug: true,

    interpolation: {
      escapeValue: false,
    },
  });

export default i18n;