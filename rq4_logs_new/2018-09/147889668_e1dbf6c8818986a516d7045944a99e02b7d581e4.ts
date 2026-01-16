import { StateManager, LoaderConfig, TigerModuleDef, ModuleRegistry, Server, LoaderResult } from "./types";

import * as log4js from "log4js";

import Executor from "./ExecutorFactory"

const LOGGER = log4js.getLogger("ModuleLoader");
LOGGER.level = "info";

export default (stateManager: StateManager,
  loaderConfig: LoaderConfig,
  moduleRegistry: ModuleRegistry,
  server: Server) => {
  return (module: string, force: boolean = false): LoaderResult => {
    let path = `/modules/${module}`;
    let status = true
    try {
      let moduleDef: TigerModuleDef = require(`${loaderConfig.basePath}/${module}`);

      moduleRegistry.update(module, moduleDef)

      if (force) {
        stateManager(module, moduleDef.state || {});
      }

      LOGGER.info(`Mounting module on ${path}`);
      server[moduleDef.method](path, Executor(path, module, stateManager, moduleRegistry));
      LOGGER.info(`Mounted module on ${path}`);

    } catch (e) {
      LOGGER.error(`Error found when loading module ${module}: ${e}`, e);
      status = false;
    }

    return { status, path }
  }
}