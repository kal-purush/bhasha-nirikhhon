import i18next from "i18next";
import { initReactI18next } from "react-i18next"
import PTBR from "../languages/pt-br.json"
import ENUS from "../languages/en-us.json"

const resources = {
    'pt-br' : PTBR,
    'en-us' : ENUS
}

i18next
    .use(initReactI18next)
    .init({
        resources,
        lng: navigator.language,
        interpolation: {
            escapeValue: false, 
        }
    })

export default i18next;