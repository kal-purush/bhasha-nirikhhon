import { tid } from "../../../test/testUtils";
import { INV_ICBM_ETH_M_HAS_KYC_DUP } from "../fixtures";
import { etoFixtureAddressByName, goToPortfolio } from "../utils/index";
import { createAndLoginNewUser } from "../utils/userHelpers";

describe("Portfolio", () => {
  describe("Reserved assets", () => {
    it("should populate on initial view", () => {
      const etoId = etoFixtureAddressByName("ETOInPayoutState");

      createAndLoginNewUser({
        type: "investor",
        kyc: "business",
        seed: INV_ICBM_ETH_M_HAS_KYC_DUP,
        hdPath: "m/44'/60'/0'/0",
        clearPendingTransactions: true,
      });

      goToPortfolio();

      cy.get(tid(`portfolio-my-assets-token-${etoId}`)).should("exist");
    });
  });
});