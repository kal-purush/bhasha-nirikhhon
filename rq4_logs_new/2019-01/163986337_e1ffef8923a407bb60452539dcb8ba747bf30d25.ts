import { ReducerManager } from '@ngrx/store';

export function createMockReducer<T>(initialState: T) {
  return function reducer(state: any = initialState, action: { payload: any }): T {
    return { ...state, ...action.payload };
  };
}

export function initReducer(featureName: string, initialState: any) {
  return (reducer: ReducerManager) => {
    return () =>
      new Promise((resolve, reject) => {
        reducer.addReducer(featureName, createMockReducer(initialState));
        resolve('mocked reducer');
      });
  };
}