import { Database } from "./database";
import "mocha";
import { expect } from "chai";
import { CardManager } from "./card"
import { ClientCard } from "../models/client-card";
  
const db = Database.getInstance();
  
describe("Card class unit tests", () => {

  let clientID = "2011";//TODO: Modify to recieve a valid client from CIS

  it("Should create a valid card number", done => {
    (async () => {
      let card = await CardManager['generateNewCard'](true);
      expect(CardManager.isCardNumberValid(card)).to.equal(true);
      done();
    })();
  });

  it("Should create and save a valid card", done => {
    (async () => {
      let card = await CardManager.createNewCard(clientID);
      let result: ClientCard;
      result = await db.getByCardNumber(card.cardNumber);
      expect(result.clientId).to.equal(clientID);
      done();
    })();
  });

  it("Should create and save valid card with rfid number", done => {
    (async () => {
      let card = await CardManager.createNewCard(clientID, true);
      let result: ClientCard;
      result = await db.getByRfid(card.rfid);
      expect(result.clientId).to.equal(clientID);
      done();
    })();
  });

  it("Should deactivate a created card using the card's number", done => {
    (async () => {
      let card = await CardManager.createNewCard(clientID);
      let result: ClientCard;
      result = await db.getByCardNumber(card.cardNumber);
      expect(result.isActivated).to.equal(true);
      await CardManager.deactivateCardByNumber(card.cardNumber);
      result = await db.getByCardNumber(card.cardNumber);
      expect(result.isActivated).to.equal(false);
      done();
    })();
  });

  it("Should deactivate a created card using the card's rfid number", done => {
    (async () => {
      let card = await CardManager.createNewCard(clientID, true);
      let result: ClientCard;
      result = await db.getByRfid(card.rfid);
      expect(result.isActivated).to.equal(true);
      await CardManager.deactivateCardByRfID(card.rfid);
      result = await db.getByRfid(card.rfid);
      expect(result.isActivated).to.equal(false);
      done();
    })();
  });

  it("Should deactivate all of a client's cards", done => {
    (async () => {
      let card1 = await CardManager.createNewCard(clientID);
      let card2 = await CardManager.createNewCard(clientID);
      let result: ClientCard;
      let card = card1;
      for(let i = 0; i < 2; i++){
        result = await db.getByCardNumber(card.cardNumber);
        expect(result.isActivated).to.equal(true);
        card = card2;
      }
      await CardManager.deactivateAllCards(clientID);
      card = card1;
      for(let i = 0; i < 2; i++){
        result = await db.getByCardNumber(card.cardNumber);
        expect(result.isActivated).to.equal(false);
        card = card2;
      }
      done();
    })();
  });

});
  