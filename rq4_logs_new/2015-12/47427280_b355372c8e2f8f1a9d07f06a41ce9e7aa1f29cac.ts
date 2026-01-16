/// <reference path="../../typings/mocha/mocha.d.ts" />
/// <reference path='../../typings/chai/chai.d.ts' />
import chai = require("chai");
var expect = chai.expect;

import { SpreadSheet } from "../../src/models/SpreadSheet";

describe("#SpreadSheet", function() {
  describe("setting and getting a value", function() {

    var spreadSheet = new SpreadSheet();
    spreadSheet.setValue(1, "A", 12);

    it("returns the correctly value", function() {
      expect(spreadSheet.getValue(1, "A")).to.equal(12);
    });
  });
});