import { IDBPDatabase } from "idb";

import { openDb } from "../initialize.db";
import { RequestAuth } from "../types";

export class AuthRepository {
  private db?: IDBPDatabase;

  private async getDb(): Promise<IDBPDatabase> {
    if (!this.db) {
      this.db = await openDb();
    }
    return this.db;
  }

  async createAuth(id: string): Promise<void> {
    const db = await this.getDb();
    const tx = db.transaction("auth", "readwrite");
    const store = tx.objectStore("auth");
    const auth: RequestAuth={
        id: crypto.randomUUID(),
        requestId: id,
        type: "none",
        value : {}
    }
    await store.add(auth);
    await tx.done;
  }

  async getAuthByRequestId(requestId: string): Promise<RequestAuth> {
    const db = await this.getDb();
    const tx = db.transaction("auth", "readonly");
    const store = tx.objectStore("auth");
    const index = store.index("requestIndex");

    return index.get(requestId);
  }

  async updateauth(auth: RequestAuth): Promise<void> {
    const db = await this.getDb();
    const tx = db.transaction("auth", "readwrite");
    const store = tx.objectStore("auth");
    await store.put(auth);
    await tx.done;
  }

  async deleteAuthByRequestId(requestId: string): Promise<void> {
    const db = await this.getDb();
    const tx = db.transaction("auth", "readwrite");
    const store = tx.objectStore("auth");
    const index = store.index("requestIndex");

    const auth = await index.get(requestId);

    if (auth) {
      await store.delete(auth.id);
    }

    await tx.done;
  }
}