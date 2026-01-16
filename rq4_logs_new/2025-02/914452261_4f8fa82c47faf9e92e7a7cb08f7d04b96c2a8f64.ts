import i18next from "i18next";
import { initReactI18next } from "react-i18next";
import translation from "./fr/translation.json";

i18next
  .use(initReactI18next)
  .init({
    lng: "fr",
    debug: true,
    resources: {
      fr: {
        translation,
      },
    },
    // if you see an error like: "Argument of type 'DefaultTFuncReturn' is not assignable to parameter of type xyz"
    // set returnNull to false (and also in the i18next.d.ts options)
    // returnNull: false,
  })
  .then(() => {
    console.log("i18next initialized successfully");
  })
  .catch((error) => {
    console.error("i18next initialization failed", error);
  });

export default i18next;