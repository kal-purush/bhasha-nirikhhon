import { combineReducers, createStore } from 'redux';
import soknadReducer from './reducers/soknadReducer';
import commonReducer from './reducers/commonReducer';
import apiReducer from './reducers/apiReducer';
import sagas from './sagas/sagas';
import middleware, { sagaMiddleware } from './middleware';

const reducers = combineReducers({ soknadReducer, commonReducer, apiReducer });

const store = createStore(
    reducers,
    // tslint:disable-next-line no-any
    (<any> window).__REDUX_DEVTOOLS_EXTENSION__ && (<any> window).__REDUX_DEVTOOLS_EXTENSION__(),
    middleware
);

sagaMiddleware.run(sagas);

export default store;