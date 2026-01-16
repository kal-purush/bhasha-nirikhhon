"use strict";

const maxSelectors = require("../lib");
const testRule = require("stylelint-test-rule-tape");

testRule(maxSelectors.rule, {
  ruleName: maxSelectors.ruleName,
  config: [2],

  accept: [
    { code: ".foo {} .bar {}" },
    { code: ".foo .bar {}" },
    { code: ".foo, .bar {}" },
  ],

  reject: [
    {
      code: ".foo, .bar, .baz {}",
      message: `Expected no more than 2 selectors (${maxSelectors.ruleName})`
    },
    {
      code: ".foo {} .bar {} .baz {}",
      message: `Expected no more than 2 selectors (${maxSelectors.ruleName})`
    },
  ],
});