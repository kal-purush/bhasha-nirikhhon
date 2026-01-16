import { useParams } from "react-router-dom";
import {
  useFirestoreConnect,
  useFirestore,
  WhereOptions,
} from "react-redux-firebase";
import { DateTime } from "luxon";

import { OrgSubCollection } from "eisbuk-shared";

import { wrapOrganization } from "@/utils/firestore";
import { getMonthStr } from "@/utils/helpers";

/**
 * A hook we're using to synchronize the local store (Redux) and firestore
 * with slots and bookings for current, plus/minus one timeframe:
 * - For `slots`, we're connecting prevoius, current and next month
 * - For `bookings`, we're connecting prevoius, current and next `week`
 *
 * **Usage:** needs to only be instantiated within the context of `<Router />`,
 * with `date` and `secretKey` route params in the same context.
 * All of the functionallity is handled internally.
 */
const useConnectSlotsAndBookgins = (): void => {
  const firestore = useFirestore();

  const { date: isoDate, secretKey } = useParams<{
    date?: string;
    secretKey: string;
  }>();

  // get date from param, if not provided, fall back to now
  const date = isoDate ? DateTime.fromISO(isoDate) : DateTime.now();

  /** @TODO Check this and maybe aggregate and use the same logic as for slots */
  // connect bookings for next 14 days and last 14 days
  const where: WhereOptions[] = [
    ["date", ">=", date?.minus({ days: 14 }).toJSDate()],
    ["date", "<", date?.plus({ days: 14 }).toJSDate()],
  ];

  const monthsToQuery = [
    getMonthStr(date, -1),
    getMonthStr(date, 0),
    getMonthStr(date, 1),
  ];

  useFirestoreConnect([
    wrapOrganization({
      collection: OrgSubCollection.SlotsByDay,
      /** @TEMP find a way to write this propperly */
      where: [(firestore.FieldPath as any).documentId(), "in", monthsToQuery],
    }),
    wrapOrganization({
      collection: OrgSubCollection.Bookings,
      doc: secretKey,
      storeAs: "subscribedSlots",
      subcollections: [
        {
          collection: "data",
          where,
        },
      ],
    }),
  ]);
};

export default useConnectSlotsAndBookgins;