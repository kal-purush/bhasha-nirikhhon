import { AES } from "crypto-js";

export const generatedTokenBySub: Function = (sub: string) =>
  AES.encrypt(
    sub + process.env.DISTINGUISHER + Date.now().toString(),
    process.env.ACCESS_TOKEN_KEY
  ).toString();