import { GlobalState } from 'Store';

const REDUX_STORE_LOCAL_STORAGE_KEY = 'OurHouse_Store';

export default class LocalStorageService {
	public static LoadReduxStore(): GlobalState | undefined {
		try {
			const json = localStorage.getItem(REDUX_STORE_LOCAL_STORAGE_KEY);

			if (json === null) {
				return undefined;
			}

			return JSON.parse(json);
		} catch (error) {
			console.error('Error Loading Redux Store from Local Storage:', error);
			return undefined; // eslint-disable-line  no-undefined
		}
	}

	public static PersistReduxStore(state: GlobalState): void {
		try {
			const json = JSON.stringify(state, null, 4);

			localStorage.setItem(REDUX_STORE_LOCAL_STORAGE_KEY, json);
		} catch (error) {
			console.error(error);
		}
	}
}