// authenticates you with the API standard library
// type `await lib.` to display API autocomplete

const lib = require("lib")({ token: process.env.STDLIB_SECRET_TOKEN });
const nacl = require("tweetnacl");
const base58 = require("bs58");

const fromHexString = (hexString) =>
  new Uint8Array(hexString.match(/.{1,2}/g).map((byte) => parseInt(byte, 16)));

const signatureUint8 = fromHexString(context.params.msg);
const nonceUint8 = new TextEncoder().encode(
  "Please sign this message for proof of address ownership."
);
const pubKeyUint8 = base58.decode(context.params.publicKey);

if (nacl.sign.detached.verify(nonceUint8, signatureUint8, pubKeyUint8)) {
  // check if user has nft
  if (true) {
    await lib.discord.guilds["@0.1.0"].members.roles.update({
      role_id: "ROLE_ID",
      user_id: context.params.user,
      guild_id: "GUILD_ID",
    });
  }
}