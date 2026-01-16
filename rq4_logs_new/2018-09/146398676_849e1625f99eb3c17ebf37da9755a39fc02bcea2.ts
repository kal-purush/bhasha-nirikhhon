import { IRepository } from '../interfaces/repository';
import { Tab } from '../models/tab';
import { environment } from '../../environments/environment';
import { Injectable } from '@angular/core';

@Injectable()
export class Repository implements IRepository {
  protected account: string = null;

  protected database = null;

  protected onChangesFn: () => void = null;

  protected syncHandler: any = null;

  protected url = 'https://couchdb.offline-notepad.com';

  constructor() {
    this.resetAccount();

    this.initialize();
  }

  public async getSelectedTabId(): Promise<string> {
    return localStorage.getItem('selected-tab-id');
  }

  public async list(): Promise<Tab[]> {
    const result: any = await this.database.allDocs({
      include_docs: true,
    });

    return result.rows
      .filter((row: any) => row.doc.account === this.account)
      .map((row: any) => new Tab(row.doc._id, row.doc.name, row.doc.content, row.doc.order, row.doc.deleted));
  }

  public onChanges(fn: () => Promise<void>): void {
    this.onChangesFn = fn;
  }

  public async resetAccount(): Promise<void> {
    this.account = localStorage.getItem('account');

    if (!this.account) {
      this.account = this.genereateUUID();

      localStorage.setItem('account', this.account);
    }
  }

  public async setAccount(account: string): Promise<void> {
    this.account = account;
  }

  public async upsert(tab: Tab): Promise<Tab> {
    if (!tab.id) {
      await this.insert(tab);
    }

    const document: any = await this.database.get(tab.id);

    if (!document) {
      return null;
    }

    await this.database.put({
      _id: tab.id,
      _rev: document._rev,
      account: this.account,
      content: tab.content,
      deleted: tab.deleted,
      name: tab.name,
      order: tab.order,
    });
  }

  protected genereateUUID(): string {
    return 'xxxxxxxx-xxxx-4xxx-yxxx-xxxxxxxxxxxx'.replace(/[xy]/g, (str) => {
      // tslint:disable-next-line:no-bitwise
      const randomNumber: number = (Math.random() * 16) | 0;

      // tslint:disable-next-line:no-bitwise
      const value: number = str === 'x' ? randomNumber : (randomNumber & 0x3) | 0x8;
      return value.toString(16);
    });
  }

  protected initialize(): void {
    if (this.syncHandler) {
      this.syncHandler.cancel();
    }

    const databaseName = `offline-notepad-${environment.version}`;

    this.database = new (window as any).PouchDB(databaseName, { auto_compaction: true });

    this.syncHandler = (window as any).PouchDB.sync(databaseName, `${this.url}/offline-notepad`, {
      live: true,
      retry: true,
    })
      .on('change', (info: any) => {
        if (this.onChangesFn && info.change.docs.filter((doc: any) => doc.account === this.account).length > 0) {
          this.onChangesFn();
        }
      })
      .on('paused', (error: Error) => {})
      .on('active', () => {})
      .on('denied', (error: Error) => {})
      .on('complete', (info: any) => {})
      .on('error', (error: Error) => {});
  }

  protected async insert(tab: Tab): Promise<void> {
    if (!tab.id) {
      tab.id = this.genereateUUID();
    }

    await this.database.put({
      _id: tab.id,
      account: this.account,
      content: tab.content,
      deleted: tab.deleted,
      name: tab.name,
      order: tab.order,
    });
  }
}