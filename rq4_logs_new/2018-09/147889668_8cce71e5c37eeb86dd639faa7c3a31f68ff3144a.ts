
import { DefaultModuleRegistry } from "./ModuleRegistry"


import { expect } from "chai"
import { TigerModule, ModuleDef } from "./types";

const registry = new DefaultModuleRegistry();

class TestModule implements TigerModule {
  name = "TestModule";  
  moduleDef: ModuleDef = { method: "get", state: {}, handler: (req, res, st) => st };
  valid: boolean = true;
  destroy(): void {
    this.valid = false;
  }

}

describe("Module Registry", () => {
  it("should successfully register a module", () => {
    registry.update("testmodule", new TestModule());


    expect(registry.valid("testmodule")).is.true;
    expect(registry.retrieve("testmodule").name).eql("TestModule");
  });

  it("should successfully unload a module", () => {
    let module2 = new TestModule();
    registry.update("moduleToBeUnloaded", module2);

    registry.unload("moduleToBeUnloaded").destroy();

    expect(module2.valid).is.false;
    expect(registry.valid("moduleToBeUnloaded")).is.false;
    expect(registry.retrieve("moduleToBeUnloaded")).is.undefined;
  })
});