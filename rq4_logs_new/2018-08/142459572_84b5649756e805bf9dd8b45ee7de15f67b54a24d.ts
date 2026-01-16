import { ReducersMapObject, createStore, combineReducers, compose, Middleware } from 'redux';

export interface ModulePlugin {
  (m: ReduxModule): ReduxModule;
}

export interface CreateModuleStoreConfig {
  plugins?: Array<ModulePlugin>;
}

export interface ReduxModule {
  name: string;
  reducers: ReducersMapObject;
  middleware?: Middleware[];
  [index: string]: any;
}

const createModuleStore = (config: CreateModuleStoreConfig) => {
  const applyPlugins = config.plugins ? compose<ReduxModule>(...config.plugins) : (x: ReduxModule) => x;
  return (modules: ReduxModule[]) => {
    const newModules = modules.map(applyPlugins);
    const allReducers = newModules.reduce((acc, mod) => ({ ...acc, ...mod.reducers }), {});
    return createStore(combineReducers(allReducers));
  };
};

export default createModuleStore;