import { expect } from "chai";
import { isEmpty } from "../../../src/utils/mydash";

const mock = {
  obj: { a: { b: 1, c: { d: 2 } } },
  arr: [1, 2],
  str: `qwerty`,
  objEmpty: {},
  arrEmpty: [],
  strEmpty: ``,
  numEmpty: 1,
};

describe("MyDash utils / isEmpty", () => {
  it("isEmpty should return false on non empty element", () => {
    expect(isEmpty(mock.obj)).to.equal(false);
    expect(isEmpty(mock.arr)).to.equal(false);
    expect(isEmpty(mock.str)).to.equal(false);
  });

  it("isEmpty should return true on empty element", () => {
    expect(isEmpty(mock.objEmpty)).to.equal(true);
    expect(isEmpty(mock.arrEmpty)).to.equal(true);
    expect(isEmpty(mock.strEmpty)).to.equal(true);
    expect(isEmpty(mock.numEmpty)).to.equal(true);
  });
});