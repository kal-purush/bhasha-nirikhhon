/// <reference path="../../typings/mocha/mocha.d.ts" />
/// <reference path='../../typings/chai/chai.d.ts' />
import { SpreadSheet } from "../../src/models/SpreadSheet";
import { Formula } from "../../src/models/Formula";

import chai = require("chai");
var expect = chai.expect;

describe("#Formula", function() {

  it("executes the expression correctly", function() {
    var ssheet : SpreadSheet = new SpreadSheet();
    var formula : Formula = new Formula("=2 + 2", ssheet);
    expect(formula.value).to.equal(4);
  });

  context("when the formula has cells reference", function() {
    var ssheet : SpreadSheet = new SpreadSheet();
    ssheet.setValue("A", 1, "2");
    ssheet.setValue("B", 1, "2");

    var formula : Formula = new Formula("= A1 + A1 + B1", ssheet);
    it("executes the expression correctly", function() {
      expect(formula.value).to.equal(6);
    });
  });
});