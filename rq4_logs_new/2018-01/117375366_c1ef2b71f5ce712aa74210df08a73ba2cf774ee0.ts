import { Injectable } from '@angular/core';

import { BaseStorageService } from './shared/base-storage.service';
import { LocalStorageMessage } from './shared/local-storage.message';
import { LocalStorageType } from './shared/local-storage.type';
import { StorageWindowRefService } from './storage-window-ref.service';

@Injectable()
export class SessionStorageService extends BaseStorageService {

  window: Window;
  storageType: string;

  constructor(private storageWindowRef: StorageWindowRefService) {
    super();
    this.storageType = LocalStorageType.SESSIONSTORAGE;

    try {
      this.window = storageWindowRef.nativewindow;
    } catch (exception) {
      console.error(`${LocalStorageMessage.WINDOW_UNAVAILABLE} EXCEPTION::`, exception);
    }
  }

}