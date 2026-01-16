import { dummySlot } from "@/__testData__/dummyData";

import { SlotType } from "eisbuk-shared";
import { CustomerRoute } from "@/enums/routes";

/**
 * Slot structure received from store
 */
export const dummySlotsFromStore = {
  "2021-03-01": {
    ["slot-0"]: { ...dummySlot, id: "slot-0", type: SlotType.Ice },
    ["slot-1"]: { ...dummySlot, id: "slot-1", type: SlotType.OffIceDancing },
    ["slot-2"]: { ...dummySlot, id: "slot-2", type: SlotType.OffIceGym },
  },
  "2021-03-02": {
    ["slot-3"]: { ...dummySlot, id: "slot-3", type: SlotType.Ice },
    ["slot-4"]: { ...dummySlot, id: "slot-4", type: SlotType.OffIceDancing },
    ["slot-5"]: { ...dummySlot, id: "slot-5", type: SlotType.OffIceGym },
  },
};

/**
 * The result we're expecting from `splitSlotsByCustomerRoute(dummySlotsFromStore)`
 */
export const dummySlotsSplitByRoute = {
  [CustomerRoute.BookIce]: {
    "2021-03-01": {
      ["slot-0"]: { ...dummySlot, id: "slot-0", type: SlotType.Ice },
    },
    "2021-03-02": {
      ["slot-3"]: { ...dummySlot, id: "slot-3", type: SlotType.Ice },
    },
  },
  [CustomerRoute.BookOffIce]: {
    "2021-03-01": {
      ["slot-1"]: { ...dummySlot, id: "slot-1", type: SlotType.OffIceDancing },
      ["slot-2"]: { ...dummySlot, id: "slot-2", type: SlotType.OffIceGym },
    },
    "2021-03-02": {
      ["slot-4"]: { ...dummySlot, id: "slot-4", type: SlotType.OffIceDancing },
      ["slot-5"]: { ...dummySlot, id: "slot-5", type: SlotType.OffIceGym },
    },
  },
};