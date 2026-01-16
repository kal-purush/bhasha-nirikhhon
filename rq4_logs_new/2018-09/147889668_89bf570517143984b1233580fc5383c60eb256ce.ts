import { DefaultModuleRegistry } from "./ModuleRegistry";
import { StateManager, ModuleDef, isTigerModuleDef } from "./types";

export default (stm: StateManager, registry: DefaultModuleRegistry) => {
    return (module: string) => {
        stm(module, {});

        let moduleDef: ModuleDef = registry.retrieve(module).moduleDef;
        if (isTigerModuleDef(moduleDef)) {
        } else {
            if (moduleDef._worker) moduleDef._worker.destroy();
        }
        registry.unload(module);
    }
}