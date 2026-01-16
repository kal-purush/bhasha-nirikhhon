import { Value } from "@sinclair/typebox/value";
import { startStopSchema, StartStopSettings } from "../src/types";
import cfg from "./__mocks__/valid-configuration.json";

describe("Configuration tests", () => {
  it("Should decode the configuration", () => {
    const settings = Value.Default(startStopSchema, { maxConcurrentTasks: { admin: 20, member: 10, contributor: 2 } }) as StartStopSettings;
    expect(settings).toEqual(cfg);
  });
  it("Should default the admin to infinity if missing from config when decoded", () => {
    const settings = Value.Default(startStopSchema, {}) as StartStopSettings;
    const decodedSettings = Value.Decode(startStopSchema, settings);
    expect(decodedSettings.maxConcurrentTasks["admin"]).toEqual(Infinity);
  });
});