export type WalletProvider = 'auro' | 'pallard' | 'clorio';

export type NetworkID = 'mainnet' | 'testnet' | 'berkeley';

// Wallet event types
export type WalletEventType = 'accountsChanged' | 'networkChanged' | 'disconnect' | 'connect';

export type WalletEventPayload = {
  accountsChanged: string[];
  networkChanged: NetworkInfo;
  disconnect: void;
  connect: void;
};

export interface WalletInfo {
  address: string;
  provider: WalletProvider;
  publicKey: string;
  network?: NetworkID | null;
}

export type WalletStatus = 'disconnected' | 'connecting' | 'connected' | 'error';

export interface WalletState {
  status: WalletStatus;
  wallet: WalletInfo | null;
  error: string | null;
  lastConnected: Date | null;
}

export interface WalletContextType {
  state: WalletState;
  connect: (provider: WalletProvider) => Promise<void>;
  disconnect: () => Promise<void>;
  isConnected: boolean;
}

// Auro wallet specific types
export interface NetworkInfo {
  networkID: NetworkID;
}

export interface AuroWallet {
  requestAccounts(): Promise<string[]>;
  getAccounts?(): Promise<string[]>;
  requestNetwork?(): Promise<NetworkInfo>;
  on?<T extends WalletEventType>(
    event: T,
    handler: (payload: WalletEventPayload[T]) => void
  ): void;
  removeListener?<T extends WalletEventType>(
    event: T,
    handler: (payload: WalletEventPayload[T]) => void
  ): void;
}

declare global {
  interface Window {
    mina?: AuroWallet;
  }
} 