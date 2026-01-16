export type Metadata = Record<string, unknown>;

export interface MessageData {
  message: string;
  id: string;
  displayName?: string;
  senderId: string;
  timestamp: number;
  event?: Event;
  metadata?: Metadata;
}
export enum Event {
  HAS_JOINED = 'hasJoined',
  HAS_LEFT = 'hasLeft',
}

export interface User {
  displayName?: string;
  userMetadata?: Metadata;
}

export enum ConnectionState {
  FAILED = 'failed',
  CLOSED = 'closed',
  DISCONNECT = 'disconnected',
}