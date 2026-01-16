import { IDBPDatabase } from "idb";

import { openDb } from "../initialize.db";
import { RequestBody } from "../types";

export class BodyRepository {
  private db?: IDBPDatabase;

  private async getDb(): Promise<IDBPDatabase> {
    if (!this.db) {
      this.db = await openDb();
    }
    return this.db;
  }

  async createBody(id: string): Promise<void> {
    const db = await this.getDb();
    const tx = db.transaction("body", "readwrite");
    const store = tx.objectStore("body");
    const body: RequestBody={
        id: crypto.randomUUID(),
        requestId: id,
        type: "none",
        content: {}
    }
    await store.add(body);
    await tx.done;
  }

  async getBodyByRequestId(requestId: string): Promise<RequestBody> {
    const db = await this.getDb();
    const tx = db.transaction("body", "readonly");
    const store = tx.objectStore("body");
    const index = store.index("requestIndex");

    return index.get(requestId);
  }

  async updateBody(body: RequestBody): Promise<void> {
    const db = await this.getDb();
    const tx = db.transaction("body", "readwrite");
    const store = tx.objectStore("body");
    await store.put(body);
    await tx.done;
  }
  async deleteBodyByRequestId(requestId: string): Promise<void> {
    const db = await this.getDb();
    const tx = db.transaction("body", "readwrite");
    const store = tx.objectStore("body");
    const index = store.index("requestIndex");

    const body = await index.get(requestId);

    if (body) {
      await store.delete(body.id);
    }

    await tx.done;
  }
}