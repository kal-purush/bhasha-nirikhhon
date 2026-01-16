// js-base64.d.ts
declare module "js-base64" {
  export const Base64: {
    fromByteArray: (bytes: Uint8Array) => string;
    toByteArray: (base64: string) => Uint8Array;
    // Include other methods you may need
    atob: (asc: string) => string;
    btoa: (bin: string) => string;
    fromBase64: (src: string) => string;
    toBase64: (src: string) => string;
    // etc.
  };
}