import { VerifyPinByCardNumberRoute } from './verify-pin-card-number';
import { NotAuthorisedException } from "./../exceptions";
import {
  exampleValidRfid,
  exampleValidPin,
  exampleValidClientId,
  exampleValidCard
} from "./../classes/validators";
import { RouteTestSuite } from "./../classes/route-test-suite";
import { PinManager } from "../classes/pin";

new RouteTestSuite(new VerifyPinByCardNumberRoute())
  .testInvalidParameters()
  .testMissingParameters()

  .add({
    name: "Fail to find missing card number",
    params: {
      cardNumber: exampleValidCard,
      pin: exampleValidPin
    },
    test: async (res, expect, db) => {
      expect(res.body.message).to.equal(new NotAuthorisedException().message);
      expect(res.body.validCard).to.equal(false);
    }
  })

  .add({
    name: "Provide incorrect pin",
    params: {
      cardNumber: exampleValidCard,
      pin: "99999"
    },
    preamble: async db => {
      await db.addCard(
        exampleValidClientId,
        exampleValidRfid,
        exampleValidCard,
        await PinManager.generatePinHash(exampleValidPin)
      );
    },
    test: async (res, expect, db) => {
      expect(res.body.message).to.equal(new NotAuthorisedException().message);
      expect(res.body.clientId).to.equal(exampleValidClientId);
      expect(res.body.validCard).to.equal(true);
    }
  })

  .add({
    name: "Provide correct pin",
    params: {
      cardNumber: exampleValidCard,
      pin: exampleValidPin
    },
    test: async (res, expect, db) => {
      expect(res.body.clientId).to.equal(exampleValidClientId);
      expect(res.body.validCard).to.equal(true);
    }
  })

  .run();