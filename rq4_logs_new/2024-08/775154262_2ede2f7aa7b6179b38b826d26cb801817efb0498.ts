import i18n from 'i18next';
import { initReactI18next } from 'react-i18next';
import auth0EN from './locales/en/auth0.json';
import auth0HE from './locales/he/auth0.json';
import websiteH from './locales/he/website.json';
import websiteE from './locales/en/website.json';
import commonE from './locales/en/common.json';
import commonH from './locales/he/common.json';
import orderE from './locales/en/order.json';
import orderH from './locales/he/order.json';
import inventoryE from './locales/en/inventory.json';
import InventoryH from './locales/he/inventory.json';
import workersE from './locales/en/workers.json';
import workersH from './locales/he/workers.json';

const resources = {
	en: {
		translation: {
			...auth0EN,
			...websiteE,
			...commonE,
			...orderE,
			...inventoryE,
			...workersE,
		},
	},
	he: {
		translation: {
			...auth0HE,
			...websiteH,
			...commonH,
			...orderH,
			...InventoryH,
			...workersH,
		},
	},
};
i18n.use(initReactI18next).init({
	resources,
	lng: 'en', // השפה ההתחלתית
	fallbackLng: 'en',
	interpolation: {
		escapeValue: false, // React כבר מבצע escaping לערכי הטקסט
	},
});
export default i18n;