import { environment } from '@env/environment';
import { ITab } from '@app/interfaces/tab';
import * as uuid from 'uuid';

export class Persistence {
  protected static database: any = null;

  protected static syncHandler: any = null;

  protected static url = 'https://couchdb.offline-notepad.com';

  public static async findAllTabs(account: string): Promise<Array<ITab>> {
    const result: any = await (await Persistence.getDatabase(account)).allDocs({
      include_docs: true,
    });

    return result.rows
      .filter((row: any) => row.doc.account === account)
      .map((row: any) => {
        return {
          content: row.doc.content,
          deleted: row.doc.deleted,
          editMode: row.doc.editMode,
          id: row.doc._id,
          lineNumbers: row.doc.lineNumbers,
          name: row.doc.name,
          order: row.doc.order,
          selected: row.doc.selected,
        } as ITab;
      })
      .sort((a: ITab, b: ITab) => a.order - b.order);
  }

  public static async upsert(tab: ITab, account: string): Promise<void> {
    if (!(await Persistence.update(tab, account))) {
      await Persistence.insert(tab, account);
    }
  }

  protected static getDatabase(account: string): Promise<any> {
    if (Persistence.database) {
      return Persistence.database;
    }

    if (Persistence.syncHandler) {
      Persistence.syncHandler.cancel();
    }

    const databaseName = `offline-notepad-${environment.version}`;

    Persistence.database = new (window as any).PouchDB(databaseName, { auto_compaction: true });

    Persistence.syncHandler = (window as any).PouchDB.sync(databaseName, `${Persistence.url}/offline-notepad`, {
      live: true,
      retry: true,
    }).on('change', (info: any) => {
      if (
        info.change.direction === 'pull' &&
        info.change.docs.filter((doc: any) => doc.account === account).length > 0
      ) {
        // TODO
      }
    });

    return Persistence.database;
  }

  protected static async insert(tab: ITab, account: string): Promise<ITab> {
    if (!tab.id) {
      tab.id = uuid.v4();
    }

    await (await Persistence.getDatabase(account)).put({
      account,
      content: tab.content,
      deleted: tab.deleted,
      editMode: tab.editMode,
      _id: tab.id,
      lineNumbers: tab.lineNumbers,
      name: tab.name,
      order: tab.order,
      selected: tab.selected,
    });

    return tab;
  }

  protected static async update(tab: ITab, account: string): Promise<ITab> {
    try {
      const document: any = await (await Persistence.getDatabase(account)).get(tab.id);

      if (!document) {
        return null;
      }

      await (await Persistence.getDatabase(account)).put({
        account,
        content: tab.content,
        deleted: tab.deleted,
        editMode: tab.editMode,
        _id: tab.id,
        lineNumbers: tab.lineNumbers,
        name: tab.name,
        order: tab.order,
        _rev: document._rev,
        selected: tab.selected,
      });

      return tab;
    } catch {
      return null;
    }
  }
}