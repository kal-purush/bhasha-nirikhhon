import { describe, it, expect } from "vitest";
import { getTokenFromURL, type AuthenticationToken } from "./spotify";

describe("getTokenFromURL", () => {
  describe("when the url is empty", () => {
    it("returns null", () => {
      expect(getTokenFromURL("")).toBe(null);
    });
  });

  describe("when the url is not empty", () => {
    it("returns a token", () => {
      const expectedToken: AuthenticationToken = {
        access_token: "fake_token",
        token_type: "Bearer",
        expires_in: "3600",
      };
      expect(
        getTokenFromURL(
          "#access_token=fake_token&token_type=Bearer&expires_in=3600",
        ),
      ).toEqual(expectedToken);
    });
  });
});