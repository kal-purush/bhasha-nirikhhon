import { applyMiddleware, compose, createStore, Store, Middleware } from 'redux';
import { persistStore, persistReducer, Persistor } from 'redux-persist';
import storage from 'redux-persist/lib/storage';
import createSagaMiddleware from 'redux-saga';

import rootSaga from './sagas';
import rootReducer from './reducers';

type StorePersistor = Store & { persistor: Persistor };
const configureStore = (): StorePersistor => {
	const isClient = typeof window !== 'undefined';
	const sagaMiddleware = createSagaMiddleware();
	const middleware: Middleware[] = [sagaMiddleware];
	let composeEnhancers;

	if (process.env.NODE_ENV !== 'production') {
		composeEnhancers =
			(typeof window !== 'undefined' && (window as any).__REDUX_DEVTOOLS_EXTENSION_COMPOSE__) || compose;
	}

	if (process.env.NODE_ENV === 'production') {
		composeEnhancers = compose;
	}

	let store: StorePersistor = createStore(rootReducer, composeEnhancers(applyMiddleware(...middleware)));

	if (isClient) {
		const persistConfig = { key: 'root', storage };
		const persistedReducer = persistReducer(persistConfig, rootReducer);
		store = createStore(persistedReducer, composeEnhancers(applyMiddleware(...middleware)));
		store.persistor = persistStore(store);
	}

	sagaMiddleware.run(rootSaga);

	return store;
};

const store = configureStore();

(global as any).store = store;

export default store;