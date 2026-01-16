// tslint:disable:no-unused-expression
import { expect } from "chai";
import "mocha";
import { Fraction } from "../src/Fraction";

describe("Precise calculations with fraction", () => {
  it("fractions with same numerator and denominator are equal", () => {
    const frac1 = new Fraction(1, 3);
    const frac2 = new Fraction(1, 3);
    expect(frac1.sameAs(frac2)).to.be.true;
  });

  it("fractions with same numerator and different denominator are not equal", () => {
    const frac1 = new Fraction(1, 3);
    const frac2 = new Fraction(1, 4);
    expect(frac1.sameAs(frac2)).to.be.false;
  });

  it("fractions reducing to the same fraction are equal", () => {
    const frac1 = new Fraction(1, 2);
    const frac2 = new Fraction(2, 4);
    expect(frac1.sameAs(frac2)).to.be.true;
  });

  it("can add fractions", () => {
    const frac1 = new Fraction(1, 3);
    const frac2 = new Fraction(1, 4);
    const expected = new Fraction(7, 12);
    expect(frac1.add(frac2).sameAs(expected)).to.be.true;
  });

  it("can compute to float", () => {
    const frac = new Fraction(1, 2);
    expect(frac.valueOf()).to.equal(0.5);
  });
});