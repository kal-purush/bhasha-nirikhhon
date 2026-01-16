
import {TigerModuleDef, ModuleRegistry, TigerModule} from "./types"

import * as log4js from "log4js"

const LOGGER = log4js.getLogger("ModuleRegistry");
LOGGER.level = "info"

export class DefaultModuleRegistry implements ModuleRegistry {

  modules: { [name: string]: TigerModuleDef | undefined } = {}

  update(name: string, moduleDef: TigerModuleDef): TigerModule {
    this.modules[name] = moduleDef;
    return { name, moduleDef }
  }  
  unload(name: string): TigerModule {
    let moduleDef = this.modules[name];
    this.modules[name] = undefined;
    return {name, moduleDef}
  }

  valid(name: string): boolean {
    return this.modules[name] !== null && this.modules[name] !== undefined;
  }

}