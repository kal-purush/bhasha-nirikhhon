import { encryptSafeUrlPart, decryptSafeUrlPart } from "./crypto.actions";

it("decryptSafeUrlPart should correctly decrypt a string encrypted using encryptSafeUrlPart ", async () => {
  // key should be 16 bytes long
  const secretKey = "458d3d798766e2379387e67885a96f39";
  const str = "test::string::123";

  const encryptedStr = await encryptSafeUrlPart(str, secretKey);
  const decryptedStr = await decryptSafeUrlPart(encryptedStr, secretKey);
  expect(decryptedStr).toEqual(str);
});