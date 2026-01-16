import { Injectable } from '@angular/core';
import { StorageWindowRefService } from './storage-window-ref.service';
import { LocalStorageMessage } from './shared/local-storage.message';

@Injectable()
export class LocalStorageService {

  window: Window;

  constructor(private storageWindowRef: StorageWindowRefService) {
    try {
      this.window = storageWindowRef.nativewindow;
    } catch (exception) {
      console.error(`${LocalStorageMessage.WINDOW_UNAVAILABLE} EXCEPTION::`, exception);
    }
  }




}