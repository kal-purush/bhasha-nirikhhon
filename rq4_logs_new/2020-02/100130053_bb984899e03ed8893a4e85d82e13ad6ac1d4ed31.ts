import { GestureEvent } from ".";
import { doesMatchFilter } from "../../utils";
import { Action, EventType, RichEventData, RichMiddleware } from "../types";

/** Gesture mapper, generates actions from gestures. */
const mapGestures = <
  ID extends string | number | symbol = string,
  T extends { [key: string]: any } = { [key: string]: any }
>(
  mappingFunction: (
    data: RichEventData<ID, GestureEvent<ID>>,
  ) => Action | void | undefined,
  filter?: EventType | EventType[],
): RichMiddleware<T, ID, GestureEvent<ID>> => (data) => {
  if (data.device !== "_gesture" || !doesMatchFilter(data.eventType, filter)) {
    return;
  }

  const action = mappingFunction(data);

  if (!action) return;

  if (data.actions) {
    data.actions.push(action);
  } else {
    data.actions = [action];
  }
};

export default mapGestures;