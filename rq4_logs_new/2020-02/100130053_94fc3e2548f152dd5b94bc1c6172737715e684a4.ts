import EventProcessor from "../../../eventprocessor";
import { KeysPressedState, RichEventData } from "../../types";
import mapKeys from "./mapper";

describe("mapKeys", () => {
  const processor = new EventProcessor<RichEventData, KeysPressedState>();
  let data: RichEventData;
  const mapper = () => ({ type: "test" });

  beforeEach(() => {
    processor.set("keysPressed", { b: true, Control: true, "+": true });

    data = {
      args: [],
      device: "key",
      event: new KeyboardEvent("keydown", {
        key: "b",
      }),
      eventType: "start",
    };
  });

  it("should do nothing for an unclassified event", () => {
    data.device = undefined;
    expect(mapKeys({ keys: ["b"], mapper })(data, processor)).toBe(undefined);
    expect(data.actions).toBe(undefined);
  });

  it("should not generate an action from an unrecognized key", () => {
    expect(mapKeys({ keys: ["a"], mapper })(data, processor)).toBe(undefined);
    expect(data.actions).toBe(undefined);
  });

  it("should not generate an action from an unrecognized key combination", () => {
    expect(mapKeys({ keys: ["Shift", "b"], mapper })(data, processor)).toBe(
      undefined,
    );
    expect(data.actions).toBe(undefined);
  });

  it("should not generate an action from a key combination that is not currently triggered", () => {
    expect(mapKeys({ keys: ["Control", "+"], mapper })(data, processor)).toBe(
      undefined,
    );
    expect(data.actions).toBe(undefined);
  });

  it("should generate an action from a single key", () => {
    expect(mapKeys({ keys: ["b"], mapper })(data, processor)).toBe(undefined);
    expect(data.actions).toEqual([{ type: "test" }]);
  });

  it("should generate an action from a key combination", () => {
    expect(mapKeys({ keys: ["Control", "b"], mapper })(data, processor)).toBe(
      undefined,
    );
    expect(data.actions).toEqual([{ type: "test" }]);
  });
});