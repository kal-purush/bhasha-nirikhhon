import { AnyAction, Dispatch, Middleware } from 'redux';

import { Application } from '..';
const NAMESPACE_SEP = '/';

export default function createPromiseMiddleware(app: Application) {
  const middleware: Middleware<{}, any, Dispatch<AnyAction>> = () => next => action => {
    const { type } = action;
    if (isEffect(type)) {
      // tslint:disable-next-line:no-shadowed-variable
      return new Promise((resolve, reject) => {
        next({
          resolve,
          reject,
          ...action
        });
      });
    }
    return next(action);
  };

  function isEffect(type: string) {
    if (!type || typeof type !== 'string') {
      return false;
    }
    const [namespace] = type.split(NAMESPACE_SEP);
    const module = app.modules.filter(m => m.namespace === namespace)[0];
    if (module) {
      if (module.effects && module.effects[type]) {
        return true;
      }
    }
    return false;
  }

  return middleware;
}