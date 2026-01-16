declare module '../backend/languageManager' {
    export let currentLanguage: string;
    export function setLanguage(lang: string): void;
    export function getTranslation(key: string): string;
  }