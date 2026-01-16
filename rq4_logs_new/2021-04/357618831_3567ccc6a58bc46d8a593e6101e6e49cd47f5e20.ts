import { IArticle } from "../types";
import { DataAccess } from "./DataAccess";

export class ArticleAccess extends DataAccess<IArticle> {
  async getUserArticles(uid: string) {
    const db = await this.connection;
    const store = db
      .transaction([this.storeName], "readonly")
      .objectStore(this.storeName)
      .index("a_idx");

    return new Promise<IArticle[]>((resolve, reject) => {
      const result: any[] = [];
      store.openCursor().onsuccess = (event) => {
        const cursor = (event.target as any).result;
        if (cursor) {
          if (cursor.value.userId === uid) {
            result.push(cursor.value);
          }
          cursor.continue();
        } else {
          return resolve(result);
        }
      };
    });
  }
}