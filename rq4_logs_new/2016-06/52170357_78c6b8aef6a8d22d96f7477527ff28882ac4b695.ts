import { Action } from './actions/actionTypes';
import { applyMiddleware, compose, createStore } from 'redux';
import thunk from 'redux-thunk';
import { rootReducer } from './rootReducer';
import { State } from './records';

declare var module: { hot: any };

const socketMiddleware = (socket) => {
  return (store: any) => (next: Function) => (action: Action) => {
    if (action.meta && action.meta.socket) {
      socket.send(JSON.stringify(action.payload));
      return next(action);
    }
    next(action);
  };
};

const configureStore = (socket: WebSocket, initialState: State) => {
  const win: any = window;
  const finalCreateStore = compose(
    applyMiddleware(thunk, socketMiddleware(socket)),
    // eslint-disable-next-line no-undef, no-process-env
    win.devToolsExtension && process.env.NODE_ENV !== 'production' ? win.devToolsExtension() : f => f
  )(createStore);
  const store = finalCreateStore(rootReducer, initialState);

  if (module.hot) {
    module.hot.accept('./rootReducer', () => {
      const nextRootReducer = require('./rootReducer').default;
      store.replaceReducer(nextRootReducer);
    });
  }

  return store;
};

export default configureStore;