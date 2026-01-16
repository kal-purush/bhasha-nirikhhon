import "mocha";
import { expect } from "chai";
import { PinManager } from "./pin";
  
describe("Pin class unit tests", () => {

  it("Should return false with the wrong pin", done => {
    (async () => {
      let pin = "12345";
      let salt = await PinManager['getSalt']();
      let pinhash = await PinManager['generatePinHash'](pin, salt);
      expect(await PinManager.verifyPinHash("12346", pinhash)).to.equal(false);
      done();
    })();
  });

  it("Should return true with the right pin", done => {
    (async () => {
      let pin = "12345";
      let salt = await PinManager['getSalt']();
      let pinhash = await PinManager['generatePinHash'](pin, salt);
      expect(await PinManager.verifyPinHash("12345", pinhash)).to.equal(true);
      done();
    })();
  });

  it("Should return true with the right and random pin", done => {
    (async () => {
      let { pin, hash } = await PinManager.createNewPin();
      expect(await PinManager.verifyPinHash(pin, hash)).to.equal(true);
      done();
    })();
  });

});