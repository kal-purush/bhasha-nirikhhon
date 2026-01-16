import EventProcessor, { EventLike } from ".";
import {
  adapters,
  RichEventData,
  PointerState,
  classify,
  preventDefault,
  mapPointer,
  forAction,
  Pointer,
} from "./middleware";

// Create event processor
const processor = new EventProcessor<RichEventData, PointerState>();

const buildPointerAction = (type: string, pointer: Pointer) => ({
  type,
  id: pointer.id,
  device: pointer.device.type,
  clientX: pointer.detail.clientX,
  clientY: pointer.detail.clientY,
});

// Register middleware
processor.use(
  classify(),
  adapters(),
  preventDefault(),
  mapPointer((pointer) => buildPointerAction("GRAB", pointer), "start"),
  mapPointer((pointer) => buildPointerAction("MOVE", pointer), "move"),
  mapPointer((pointer) => buildPointerAction("DROP", pointer), "end"),
  forAction((action) => {
    console.log(`${action.device} ${action.type}s ${action.id}`);
  }),
);

// Add global event listeners
document.addEventListener("mousemove", processor.dispatch);
document.addEventListener("mouseup", processor.dispatch);

// Add element event listeners
const listener = (event: EventLike) => processor.dispatch(event, "id");
document.getElementById("elementId")?.addEventListener("mousedown", listener);

// Clicking and moving `#elementId` now results in:
// mouse GRABs id
// mouse MOVEs id
// ...
// mouse DROPs id