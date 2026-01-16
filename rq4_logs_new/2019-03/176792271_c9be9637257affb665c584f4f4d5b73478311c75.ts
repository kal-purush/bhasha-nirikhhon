import { INV_ICBM_ETH_M_HAS_KYC_DUP } from "../fixtures";
import { fillForm } from "../utils/forms";
import { assertWallet, goToWallet } from "../utils/index";
import { formField, tid } from "../utils/selectors";
import { createAndLoginNewUser } from "../utils/userHelpers";
import { assertBankAccountDetails } from "./assertions";

// Requires dedicated fixture with NEUR available to redeem
describe.skip("Redeem NEUR", () => {
  beforeEach(() => {
    createAndLoginNewUser({
      type: "investor",
      kyc: "business",
      seed: INV_ICBM_ETH_M_HAS_KYC_DUP,
      hdPath: "m/44'/60'/0'/0",
    }).then(() => {
      goToWallet();
      assertWallet();

      assertBankAccountDetails();

      cy.get(tid("wallet-balance.neur.redeem-button")).click();
    });
  });

  it("should not allow to use value below 5 NEUR", () => {
    fillForm(
      {
        amount: "2.22",
      },
      { submit: false },
    );

    cy.get(tid("bank-transfer.reedem-init.continue")).should("be.disabled");
  });

  it("should not allow to use value above NEUR balance", () => {
    fillForm(
      {
        amount: "9999999999.99",
      },
      { submit: false },
    );

    cy.get(tid("bank-transfer.reedem-init.continue")).should("be.disabled");
  });

  it("should correctly format input", () => {
    fillForm(
      {
        amount: "-3s32aa@fax2.24@#2535%s9sf92",
      },
      { submit: false },
    );

    cy.get(formField("amount")).should("have.value", "3 322.24");

    fillForm(
      {
        amount: "123456789.99",
      },
      { submit: false },
    );

    cy.get(formField("amount")).should("have.value", "123 456 789.99");
  });
});