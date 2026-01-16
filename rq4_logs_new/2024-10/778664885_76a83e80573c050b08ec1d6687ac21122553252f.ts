"use server";
import crypto from "crypto";

const algorithm = "aes-256-cbc";

const secretKey = process.env.SECRET_KEY;
const iv = crypto.randomBytes(16);

const key = crypto
  .createHash("sha256")
  .update(String(secretKey))
  .digest("base64")
  .slice(0, 32);

export async function encryptToken(text: string) {
  let cipher = crypto.createCipheriv(algorithm, Buffer.from(key), iv);
  let encrypted = cipher.update(text);
  encrypted = Buffer.concat([encrypted, cipher.final()]);
  return iv.toString("hex") + ":" + encrypted.toString("hex");
}

export async function decryptToken(text: string) {
  let textParts = text.split(":");
  let iv = Buffer.from(textParts.shift(), "hex");
  let encryptedText = Buffer.from(textParts.join(":"), "hex");
  let decipher = crypto.createDecipheriv(algorithm, Buffer.from(key), iv);
  let decrypted = decipher.update(encryptedText);
  decrypted = Buffer.concat([decrypted, decipher.final()]);
  return decrypted.toString();
}