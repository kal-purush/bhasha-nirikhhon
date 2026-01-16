import EventProcessor from "../../../eventprocessor";
import { RichEventData } from "../../types";
import mouseMapper from "./mapper";

describe("mouseMapper", () => {
  const processor = new EventProcessor<RichEventData>();
  let data: RichEventData;

  beforeEach(() => {
    data = {
      args: [],
      device: "mouse",
      event: new MouseEvent("mousedown", {
        button: 3,
      }),
      eventType: "start",
    };
  });

  it("should do nothing for an unclassified event", () => {
    data.device = undefined;
    expect(mouseMapper({ 3: () => ({ type: "test" }) })(data, processor)).toBe(
      undefined,
    );
    expect(data.actions).toBe(undefined);
  });

  it("should do nothing for an unrecognized interaction", () => {
    expect(mouseMapper({})(data, processor)).toBe(undefined);
    expect(data.actions).toBe(undefined);
  });

  it("should generate an action", () => {
    expect(mouseMapper({ 3: () => ({ type: "test" }) })(data, processor)).toBe(
      undefined,
    );
    expect(data.actions).toEqual([{ type: "test" }]);
  });

  it("should do nothing for a move event", () => {
    data.event = new MouseEvent("mousemove", {
      button: 3,
    });
    data.eventType = "move";

    expect(mouseMapper({ 3: () => ({ type: "test" }) })(data, processor)).toBe(
      undefined,
    );
    expect(data.actions).toBe(undefined);
  });
});