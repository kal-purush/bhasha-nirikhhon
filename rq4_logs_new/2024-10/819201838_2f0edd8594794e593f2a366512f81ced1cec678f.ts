import { TelegramPlatform } from '@portkey/did-ui-react';

export const STORAGE_KEYS = {
  PORTKEY_LOGIN_CHAIN_ID_KEY: 'portkeyLoginChainId',
  PORTKEY_LOGIN_SESSION_ID_KEY: 'portkeyLoginSessionId',
  WALLET_KEY_NAME: 'BEAN_GO_TOWN',
};

export class StorageUtils {
  static getStorageKey(keyName: string) {
    return TelegramPlatform.isTelegramPlatform() ? `${keyName}-${TelegramPlatform.getTelegramUserId()}` : keyName;
  }

  static getSessionStorage() {
    return localStorage.getItem(StorageUtils.getStorageKey(STORAGE_KEYS.PORTKEY_LOGIN_SESSION_ID_KEY));
  }
  static setSessionStorage(data: string) {
    return localStorage.setItem(StorageUtils.getStorageKey(STORAGE_KEYS.PORTKEY_LOGIN_SESSION_ID_KEY), data);
  }
  static removeSessionStorage() {
    return localStorage.removeItem(StorageUtils.getStorageKey(STORAGE_KEYS.PORTKEY_LOGIN_SESSION_ID_KEY));
  }

  static getOriginChainId() {
    return localStorage.getItem(StorageUtils.getStorageKey(STORAGE_KEYS.PORTKEY_LOGIN_CHAIN_ID_KEY));
  }
  static setOriginChainId(data: string) {
    return localStorage.setItem(StorageUtils.getStorageKey(STORAGE_KEYS.PORTKEY_LOGIN_CHAIN_ID_KEY), data);
  }
  static removeOriginChainId() {
    return localStorage.removeItem(StorageUtils.getStorageKey(STORAGE_KEYS.PORTKEY_LOGIN_CHAIN_ID_KEY));
  }

  static removeWallet() {
    window.localStorage.removeItem(STORAGE_KEYS.WALLET_KEY_NAME);
    window.localStorage.removeItem(StorageUtils.getStorageKey(STORAGE_KEYS.WALLET_KEY_NAME));
  }
}