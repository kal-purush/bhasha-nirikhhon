/// <reference path="../../typings/mocha/mocha.d.ts" />
/// <reference path='../../typings/chai/chai.d.ts' />
import chai = require("chai");
var expect = chai.expect;

import { FormatsHelper } from "../../src/helpers/FormatsHelper";
import { Formats } from "../../src/models/Formats";

describe("#FormatsHelper", function() {
  describe("#guess", function() {
    it("returns the correctly format", function() {
      expect(FormatsHelper.guess("1")).to.equal(Formats.Number);
      expect(FormatsHelper.guess("James Bond")).to.equal(Formats.String);
    });
  });
});