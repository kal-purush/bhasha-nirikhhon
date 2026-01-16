import i18n, { TOptionsBase } from 'i18next'
import { useTranslation as i18UseTranslation, initReactI18next } from 'react-i18next'

import * as EN_TRANSLATIONS from './en'
import * as SV_TRANSLATIONS from './sv'

export enum SupportedAppLanguage {
  ENGLISH = 'ENGLISH',
  SWEDISH = 'SWEDISH',
}

export type ResourceLangType = 'common' | 'forms'

export type AppLanguages = 'en-US' | 'sv-SE'

export const appLanguageToCode: Record<SupportedAppLanguage, AppLanguages> = {
  [SupportedAppLanguage.ENGLISH]: 'en-US',
  [SupportedAppLanguage.SWEDISH]: 'sv-SE',
}

export const languageCodeToSupportedLanguage: Record<AppLanguages, SupportedAppLanguage> = {
  'en-US': SupportedAppLanguage.ENGLISH,
  'sv-SE': SupportedAppLanguage.SWEDISH,
}

type TOptions = TOptionsBase & { [key: string]: string | number | undefined }

i18n
  .use(initReactI18next)
  .init({
    compatibilityJSON: 'v3',
    lng: 'en-US',
    simplifyPluralSuffix: false,
    pluralSeparator: '_',
    resources: {
      'en-US': EN_TRANSLATIONS,
      'sv-SE': SV_TRANSLATIONS,
    },
    defaultNS: 'common',
    returnNull: false,
    interpolation: {
      escapeValue: false,
    },
  })
  .catch((error) => console.error('[i18n] init error', error))

export default i18n

export const useTranslation = (ns?: ResourceLangType | ResourceLangType[]) => {
  const { t: translate, i18n } = i18UseTranslation()

  const t = (i18nKey: string, options?: TOptions): string => {
    return translate(i18nKey, { ...options, ns })
  }

  return { t, i18n }
}