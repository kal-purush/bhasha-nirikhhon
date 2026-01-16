import {
  Store,
  combineReducers,
  applyMiddleware,
  createStore as originalCreateStore,
} from 'redux';
import { History } from 'history';
import thunkMiddleware from 'redux-thunk';
import { routerReducer } from 'react-router-redux';
import { connectRouter, routerMiddleware } from 'connected-react-router';
import application, { ApplicationState } from '../../reducers/application';

export const createStore = (history: History): Store => {
  const createStoreFunc = applyMiddleware(
    thunkMiddleware,
    routerMiddleware(history)
  )(originalCreateStore),
    allReducers = combineReducers({
      router: connectRouter(history),
      routing: routerReducer,
      applicationState: application.reducer,
      // todo:  able to add more reducers here
    });

  return createStoreFunc(allReducers, {
    applicationState: ({
      // todo:  this is where we can default values for the app on load
      testValue: 'DEFAULTING_FOR_TEST'
    } as unknown) as ApplicationState,
  });
};

export interface State {
  applicationState: ApplicationState;
}