import { doIt } from "./index";

jest.mock("./module", () => ({
  getSomething: () => "mockValue"
}));

describe("doIt with jest.mock", () => {
  it("returns the mocked value", () => {
    expect(doIt()).toBe("mockValue");
  });
});