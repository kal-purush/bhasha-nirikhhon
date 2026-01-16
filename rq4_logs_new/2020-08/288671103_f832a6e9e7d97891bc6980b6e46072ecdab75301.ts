import low, { LowdbSync } from "lowdb";
import FileSync from "lowdb/adapters/FileSync";
import path from "path";

import { Scheme } from "../types";

const emptyScheme: Scheme = {
  stakes: [],
  lastReport: new Date("1970")
};

export default class DB {
  private static collections: LowdbSync<Scheme>;

  private constructor() {}

  static async getInstance(): Promise<LowdbSync<Scheme>> {
    if (!DB.collections) {
      const adapter = new FileSync<Scheme>(path.join(__dirname + "/../../" + "db.json"));
      DB.collections = low(adapter);
      DB.collections.defaults(emptyScheme).write();
    }

    return DB.collections;
  }
}