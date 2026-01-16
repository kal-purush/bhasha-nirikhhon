import { URLS_GET_WALKTHROUGH } from "@repo/constants/src/urls";
import {
  EnumBuildingTypes,
  EnumWalkthroughIds,
} from "@repo/constants/src/constants";
import { runWalkthrough } from "../../support/helpers";
import { GET_TESTID_STEP_TRACKER_WALKTHROUGH_HEADER } from "@repo/constants/src/testids";
import { walkthroughs } from "../../fixtures/multi-dwelling/multi-dwelling-9.10.14-test-data.json";
import { results } from "../../fixtures/results-data.json";

describe("multi dwelling: 9.10.14 walkthrough", () => {
  beforeEach(() => {
    cy.visit(
      URLS_GET_WALKTHROUGH(EnumBuildingTypes.MULTI_DWELLING, [
        EnumWalkthroughIds._9_10_14,
      ]),
    );
  });

  // Test all walkthroughs defined in test data
  walkthroughs.forEach((walkthrough) => {
    it(walkthrough.title, () => {
      runWalkthrough(walkthrough, results.multi_dwelling_workflow_91014);
    });
  });

  it("step tracker doesn't show section headers for single section", () => {
    cy.getByTestID(
      GET_TESTID_STEP_TRACKER_WALKTHROUGH_HEADER(EnumWalkthroughIds._9_10_14),
    ).should("be.hidden");
  });

  it("default state should be accessible", () => {
    cy.injectAxe();
    cy.checkA11yWithErrorLogging();
  });
});