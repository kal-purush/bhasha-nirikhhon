import type { Ed25519PrivateKey } from "@libp2p/interface";

import { hexToBytes, bytesToHex } from "@waku/utils/bytes";
import { generateKeyPairFromSeed } from "@libp2p/crypto/keys";

import { sha256, generateRandomNumber } from "./utils";

export class Local {
  public static LOCAL_ID_KEY = "local-id";

  public static async getIdentity(): Promise<Ed25519PrivateKey> {
    // @ts-ignore
    window.hexToBytes = hexToBytes;
    // @ts-ignore
    window.bytesToHex = bytesToHex;

    let seed = localStorage.getItem(this.LOCAL_ID_KEY);

    if (!seed) {
      const hash = await sha256(generateRandomNumber());
      seed = hash.slice(0, 64);
      localStorage.setItem(this.LOCAL_ID_KEY, seed);
    }

    return await generateKeyPairFromSeed("Ed25519", hexToBytes(seed));
  }
}