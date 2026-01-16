import jose from "node-jose";

import { JWTPayload } from "../types";
import { generateHS256JWS, generateRS256JWS } from "./generateJWS";

async function generateJWE(
  JWTPayload: JWTPayload,
  publicKeyForEncryption: string,
  options?: { secretKey?: string; privateKeyForSignature?: string },
): Promise<string> {
  const josePublicKeyForEncryption = await jose.JWK.asKey(
    publicKeyForEncryption,
    "pem",
  );

  let JWSToken;

  if (options) {
    if (options.secretKey) {
      JWSToken = generateHS256JWS(JWTPayload, options.secretKey);
    }
    if (options.privateKeyForSignature) {
      JWSToken = generateRS256JWS(JWTPayload, options.privateKeyForSignature);
    }
  }

  return await jose.JWE.createEncrypt(
    { format: "compact" },
    josePublicKeyForEncryption,
  )
    .update(Buffer.from(options ? JWSToken : JSON.stringify(JWTPayload)))
    .final();
}

export { generateJWE };