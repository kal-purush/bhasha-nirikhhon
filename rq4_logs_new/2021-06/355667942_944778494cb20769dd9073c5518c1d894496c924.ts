import Government from "./Government";
import isValidNomination from "./isValidNomination";

it("should not restrict eligibility of presidentialCandidate", async () => {
  expect.hasAssertions();

  const numPlayers = 10;
  const lastGovernment: Government = {
    president: 0,
    chancellor: 1,
  };

  let nomination: Government = {
    president: 1,
    chancellor: 2,
  };
  expect(isValidNomination(numPlayers, lastGovernment, nomination)).toBe(true);

  nomination = {
    president: 0,
    chancellor: 2,
  };
  expect(isValidNomination(numPlayers, lastGovernment, nomination)).toBe(true);
});

it("should reject nomination of the same player for both titles", async () => {
  expect.hasAssertions();

  const numPlayers = 10;
  const lastGovernment = null;

  let nomination: Government = {
    president: 2,
    chancellor: 2,
  };
  expect(isValidNomination(numPlayers, lastGovernment, nomination)).toBe(false);
});

it("should prevent previously elected presidentialCandidate from being nominated chancellor if more than 5 players are present", async () => {
  expect.hasAssertions();

  const numPlayers = 10;
  const lastGovernment: Government = {
    president: 0,
    chancellor: 1,
  };

  const nomination: Government = {
    president: 2,
    chancellor: 0,
  };
  expect(isValidNomination(numPlayers, lastGovernment, nomination)).toBe(false);
});

it("should allow previously elected presidentialCandidate being nominated chancellor if exactly 5 players are present", async () => {
  expect.hasAssertions();

  const numPlayers = 5;
  const lastGovernment: Government = {
    president: 0,
    chancellor: 1,
  };

  const nomination: Government = {
    president: 2,
    chancellor: 0,
  };
  expect(isValidNomination(numPlayers, lastGovernment, nomination)).toBe(true);
});

it("should prevent previously elected chancellor from being nominated chancellor", async () => {
  expect.hasAssertions();

  const numPlayers = 10;
  const lastGovernment: Government = {
    president: 0,
    chancellor: 1,
  };

  const nomination: Government = {
    president: 2,
    chancellor: 1,
  };
  expect(isValidNomination(numPlayers, lastGovernment, nomination)).toBe(false);
});