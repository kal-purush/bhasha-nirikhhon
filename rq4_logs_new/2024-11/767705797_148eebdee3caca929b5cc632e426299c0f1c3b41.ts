import { watch } from "vue";

import { state } from "@/state";

class Database {
  dbName = "morbo";
  version = 1;
  stores = ["state"];

  async open() {
    return new Promise<IDBDatabase>((resolve, reject) => {
      const request = indexedDB.open(this.dbName, this.version);
      request.onupgradeneeded = (event) => {
        const db = (event.target as IDBOpenDBRequest).result;
        for (const store of this.stores) {
          if (!db.objectStoreNames.contains(store)) {
            db.createObjectStore(store);
          }
        }
      };
      request.onsuccess = () => {
        resolve(request.result);
      };
      request.onerror = () => {
        reject(new Error(request.error?.message ?? `[IndexedDB] Failed to open a connection`));
      };
    });
  }
}

const database = new Database();

class StateStore {
  storeName = "state";
  ready = this.init();

  async init() {
    await this.loadState();
    this.setupWatchers();
  };

  private async loadState() {
    const db = await database.open();
    return new Promise<void>((resolve, reject) => {
      const transaction = db.transaction(this.storeName, "readonly");
      const store = transaction.objectStore(this.storeName);

      let pending = 0;

      for (const [ref, property] of state) {
        pending++;

        const request = store.get(property);
        request.onsuccess = () => {
          const value: unknown = request.result;
          if (value !== undefined && typeof value === typeof ref.value) {
            ref.value = value;
          } else {
            void this.putState(ref.value, property);
          }

          pending--;
          if (pending === 0) {
            resolve();
          }
        };
        request.onerror = () => {
          reject(new Error(request.error?.message ?? `[IndexedDB] [${this.storeName}] Failed to get state`));
        };
      }
    });
  }

  private setupWatchers() {
    for (const [ref, property] of state) {
      watch(ref, () => {
        void this.putState(ref.value, property);
      });
    }
  }

  private async putState(value: unknown, property: string) {
    const db = await database.open();
    return new Promise<void>((resolve, reject) => {
      const transaction = db.transaction(this.storeName, "readwrite");
      const store = transaction.objectStore(this.storeName);

      const request = store.put(value, property);
      request.onsuccess = () => {
        resolve();
      };
      request.onerror = () => {
        reject(new Error(request.error?.message ?? `[IndexedDB] [${this.storeName}] Failed to put state`));
      };
    });
  }
}

export const stateStore = new StateStore();