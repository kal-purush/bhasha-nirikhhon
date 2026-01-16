import { language } from "@/constants/language";

export function getCurrentLanguage(lang: string) {
    return lang.toLowerCase() === language.ua.toLowerCase() ? language.ua : language.en;
}