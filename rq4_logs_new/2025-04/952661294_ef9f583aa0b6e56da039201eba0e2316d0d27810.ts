import { IDBPDatabase } from "idb";

import { openDb } from "../initialize.db";
import { RequestDocs } from "../types/docs.type";

export class DocsRepository {
  private db?: IDBPDatabase;

  private async getDb(): Promise<IDBPDatabase> {
    if (!this.db) {
      this.db = await openDb();
    }
    return this.db;
  }

  async createDocs(requestId: string): Promise<void> {
    const db = await this.getDb();
    const tx = db.transaction("docs", "readwrite");
    const store = tx.objectStore("docs");

    const docs: RequestDocs = {
      id: crypto.randomUUID(),
      requestId: requestId,
      coments: "",
    };

    await store.add(docs);
    await tx.done;
  }

  async getDocsByRequestId(requestId: string): Promise<RequestDocs> {
    const db = await this.getDb();
    const tx = db.transaction("docs", "readonly");
    const store = tx.objectStore("docs");
    const index = store.index("requestIndex");

    return index.get(requestId);
  }

  async updateDocs(docs: RequestDocs): Promise<void> {
    const db = await this.getDb();
    const tx = db.transaction("docs", "readwrite");
    const store = tx.objectStore("docs");
    await store.put(docs);
    await tx.done;
  }

  async deleteDocsByRequestId(requestId: string): Promise<void> {
    const db = await this.getDb();
    const tx = db.transaction("docs", "readwrite");
    const store = tx.objectStore("docs");
    const index = store.index("requestIndex");

    const docs = await index.get(requestId);

    if (docs) {
      await store.delete(docs.id);
    }

    await tx.done;
  }
}