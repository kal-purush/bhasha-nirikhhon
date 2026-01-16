import {
  answerCurrentQuestion,
  getWalkthrough,
  navigateToNextQuestion,
} from "../../support/helpers";
import { walkthroughs } from "../../fixtures/multi-dwelling/multi-dwelling-9.10.14-test-data.json";
import { URLS_GET_WALKTHROUGH } from "@repo/constants/src/urls";
import {
  EnumBuildingTypes,
  EnumWalkthroughIds,
} from "@repo/constants/src/constants";
import { GET_TESTID_RESULT_ITEM } from "@repo/constants/src/testids";

describe("multi dwelling: 9.10.14 results", () => {
  beforeEach(() => {
    cy.visit(
      URLS_GET_WALKTHROUGH(EnumBuildingTypes.MULTI_DWELLING, [
        EnumWalkthroughIds._9_10_14,
      ]),
    );
  });

  it("validate math for result pages for result 5_0", () => {
    // This walkthrough should have the result 5_0 which has computed values
    const walkthrough5_0Result = getWalkthrough(walkthroughs, 1);

    walkthrough5_0Result.steps.forEach((step) => {
      answerCurrentQuestion(step);
      navigateToNextQuestion();
    });

    const unobstructedOpeningArea = 333;
    const totalAllowedWindowArea = 133.2;
    const validationString = `Based on your selections the maximum aggregate percentage of unprotected openings in this exposing building face is 40%*, based on an exposing building face of ${unobstructedOpeningArea} m2, the maximum area would be ${totalAllowedWindowArea} m2 per Table 9.10.14.4.-A and Sentence 9.10.14.4.(1)`;
    cy.getByTestID(GET_TESTID_RESULT_ITEM(EnumWalkthroughIds._9_10_14)).should(
      "be.visible",
    );
    cy.getByTestID(
      GET_TESTID_RESULT_ITEM(EnumWalkthroughIds._9_10_14),
    ).scrollIntoView();
    cy.getByTestID(
      GET_TESTID_RESULT_ITEM(EnumWalkthroughIds._9_10_14),
    ).contains(validationString);
  });

  it("validate math for result pages for result 6_0", () => {
    // This walkthrough should have the result 6_0 which has computed values
    const walkthrough6_0Result = getWalkthrough(walkthroughs, 3);

    walkthrough6_0Result.steps.forEach((step) => {
      answerCurrentQuestion(step);
      navigateToNextQuestion();
    });

    const exposedBuildingFace = 333;
    const totalAllowedArea = 253.08;
    const validationString = `Based on your selections the maximum percentage of unprotected openings in this exterior wall is 76%*, based on an exposing building face of ${exposedBuildingFace} m2, the maximum area would be: ${totalAllowedArea} m2.`;
    cy.getByTestID(GET_TESTID_RESULT_ITEM(EnumWalkthroughIds._9_10_14)).should(
      "be.visible",
    );
    cy.getByTestID(
      GET_TESTID_RESULT_ITEM(EnumWalkthroughIds._9_10_14),
    ).scrollIntoView();
    cy.getByTestID(
      GET_TESTID_RESULT_ITEM(EnumWalkthroughIds._9_10_14),
    ).contains(validationString);
  });
});