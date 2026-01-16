import { StateManager, ModuleRegistry, Request, Response } from "./types";

import * as log4js from "log4js";

const LOGGER = log4js.getLogger("Executor");
LOGGER.level = "info";

export default (path: string, module: string, stateManager: StateManager, moduleRegistry: ModuleRegistry) => {
  return (req: Request, res: Response) => {
    if (!moduleRegistry.valid(module)) {
      res.status(404).send({error: `module ${module} no longer valid`});
      return;
    }
    
    let { moduleDef } = moduleRegistry.retrieve(module);
    let state = stateManager.get(module);
    LOGGER.info(`Handle request on ${path}`);
    moduleDef.handler(req, res, state);
    LOGGER.info(`Request ${path} processed successfully`);
    stateManager.set(module, state);
  }
}