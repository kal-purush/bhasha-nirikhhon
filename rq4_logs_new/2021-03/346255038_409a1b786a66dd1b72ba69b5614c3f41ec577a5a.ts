import type { PriceReceiver } from "../interfaces/actions";
import type { Price } from "../interfaces/price";

import firebase from "firebase/app";

import { getRef } from "./internal/get-ref";


const latestPrices: PriceReceiver = async () => {
  const latest = await getRef(firebase, "price").get()
  const key = latest.val() ?? "unknown"

  if (key === "unknown") {
    return {
      key,
      amount: -1,
      timestamp: -1
    }
  }


  const data = await getRef(firebase, "prices").child(key).get()
  const obj: Price = data.val()
  return {
    key,
    amount: obj.amount,
    timestamp: obj.timestamp
  }
}

export default latestPrices;