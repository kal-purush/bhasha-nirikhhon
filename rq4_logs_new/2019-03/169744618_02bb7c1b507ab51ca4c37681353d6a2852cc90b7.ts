import getters from "@/getters/users";
import { initialState } from "@/store";

describe("userRole", () => {
  it("returns null when token is blank", () => {
    const state = {
      ...initialState,
      token: null,
    };
    expect(getters.userRole(state)).toBeNull();
  });

  it("returns null when token is not a valid one", () => {
    const state = {
      ...initialState,
      token: "token",
    };
    expect(getters.userRole(state)).toBeNull();
  });

  it("returns user's role", () => {
    const state = {
      ...initialState,
      token:
        "eyJhbGciOiJIUzI1NiJ9.eyJleHAiOjEyMzQsImlkIjoxLCJyb2xlIjoiYWRtaW4ifQ.QzOnDjT2ECz8WoojpoXxIT86nJh2TCJizbzhqBfjv44",
    };
    expect(getters.userRole(state)).toEqual("admin");
  });
});