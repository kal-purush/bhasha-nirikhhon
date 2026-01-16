import i18n from 'i18next'
import { initReactI18next } from 'react-i18next'
import { languageStore } from '@/utils/language-store'

// Import resources
import commonEN from './locales/en/common.json'
import transactionsEN from './locales/en/transactions.json'
import cashierEN from './locales/en/cashier.json'
import commonID from './locales/id/common.json'
import transactionsID from './locales/id/transactions.json'
import cashierID from './locales/id/cashier.json'

// Configure resources
const resources = {
  en: {
    common: commonEN,
    transactions: transactionsEN,
    cashier: cashierEN
  },
  id: {
    common: commonID,
    transactions: transactionsID,
    cashier: cashierID
  }
}

// Initialize i18next with a default language, then update when we load the preference
i18n.use(initReactI18next).init({
  resources,
  lng: 'en', // Default initial language
  fallbackLng: 'en',
  supportedLngs: ['en', 'id'],
  debug: import.meta.env.DEV,
  interpolation: {
    escapeValue: false // React already safes from xss
  },
  react: {
    useSuspense: false
  }
})

// Load the language preference from the store (async)
languageStore.get().then((language) => {
  if (language && language !== i18n.language) {
    i18n.changeLanguage(language)
  }
})

// Save language preference when it changes
i18n.on('languageChanged', (lng) => {
  languageStore.set(lng)
})

export default i18n