import EventProcessor from "../../../eventprocessor";
import { KeysPressedState, RichEventData } from "../../types";
import keyAdapter from "./adapter";

describe("keyAdapter", () => {
  const processor = new EventProcessor<RichEventData, KeysPressedState>();

  beforeEach(() => {
    processor.set("keysPressed", undefined);
  });

  it("should do nothing for an unclassified event", () => {
    const data: RichEventData = {
      args: [],
      event: new KeyboardEvent("keydown", { key: "a" }),
    };

    expect(keyAdapter()(data, processor)).toBe(undefined);

    expect(processor.get("keysPressed")).toBe(undefined);
  });

  it("should do nothing for an unrecognized event", () => {
    const data: RichEventData = {
      args: [],
      device: "key",
      event: new KeyboardEvent("keydown", { key: "a" }),
      eventType: "undef" as any,
    };

    expect(keyAdapter()(data, processor)).toBe(undefined);

    expect(processor.get("keysPressed")).toEqual({});
  });

  it("should register a key being pressed", () => {
    const data: RichEventData = {
      args: [],
      device: "key",
      event: new KeyboardEvent("keydown", { key: "a" }),
      eventType: "start",
    };

    expect(keyAdapter()(data, processor)).toBe(undefined);

    data.event = new KeyboardEvent("keydown", { key: "Control" });
    expect(keyAdapter()(data, processor)).toBe(undefined);

    expect(processor.get("keysPressed")).toEqual({ a: true, Control: true });
  });

  it("should register a key being lifted", () => {
    processor.set("keysPressed", { a: true, Control: true });
    const data: RichEventData = {
      args: [],
      device: "key",
      event: new KeyboardEvent("keyup", { key: "+" }),
      eventType: "end",
    };

    expect(keyAdapter()(data, processor)).toBe(undefined);

    data.event = new KeyboardEvent("keyup", { key: "Control" });
    expect(keyAdapter()(data, processor)).toBe(undefined);

    expect(processor.get("keysPressed")).toEqual({
      a: true,
      Control: false,
      "+": false,
    });
  });
});