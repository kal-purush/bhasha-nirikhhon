import fetchWriteAndIntervalUpdate from "https://raw.githubusercontent.com/OverGlass/fetchWriteAndIntervalUpdate/v0.0.1/fetchWriteAndIntervalUpdate.ts";
import { config } from "./deps.ts";

export const LOGIC_AUTO_STOCK_JSON_PATH = `${Deno.cwd()}/logicAutoStockDB.json`;

export default async function () {
  await fetchWriteAndIntervalUpdate(
    config().LOGIC_AUTO_JSON_STOCK_URL,
    LOGIC_AUTO_STOCK_JSON_PATH,
    hourToMilliseconds(5)
  );
  console.log("Logic Auto Stock is update");
}

function hourToMilliseconds(hour: number) {
  return hour * 3.6e6;
}