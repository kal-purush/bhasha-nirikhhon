export interface BlobRepo {
  put(data: Uint8Array): Promise<{ ref: string }>;
  get(ref: string): Promise<{ data: AsyncIterable<Uint8Array> }>;
}

export * from "./blob-repo/ipfs";