export interface TokenStorage {
	storeSyncToken(syncToken: string);
	getSyncToken(callback: ((string) => void));
}