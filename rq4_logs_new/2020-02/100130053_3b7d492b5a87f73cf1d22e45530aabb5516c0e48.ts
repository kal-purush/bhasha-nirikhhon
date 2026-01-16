import EventProcessor from "../../../eventprocessor";
import { RichEventData, MousePositionState } from "../../types";
import trackMousePosition from "./trackPosition";

describe("trackMousePosition", () => {
  const processor = new EventProcessor<RichEventData, MousePositionState>();
  let data: RichEventData;

  beforeEach(() => {
    data = {
      args: [],
      device: "mouse",
      event: new MouseEvent("mousemove", { clientX: 100, clientY: 250 }),
      eventType: "move",
    };
  });

  it("should do nothing for an unclassified event", () => {
    data.device = undefined;
    expect(trackMousePosition()(data, processor)).toBe(undefined);
    expect(processor.get("mousePosition")).toBe(undefined);
  });

  it("should do nothing for a non-mouse event", () => {
    data.device = "touch";
    expect(trackMousePosition()(data, processor)).toBe(undefined);
    expect(processor.get("mousePosition")).toBe(undefined);
  });

  it("should record the current mouse position for a mouse event", () => {
    expect(trackMousePosition()(data, processor)).toBe(undefined);
    expect(processor.get("mousePosition")).toEqual({
      clientX: 100,
      clientY: 250,
    });
  });
});