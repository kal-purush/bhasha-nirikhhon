import Player from "./../src/player.js";

describe("Player", () => {

  test("should create a player", () => {
    const gamer = new Player("Benjamin");
    expect(gamer.name).toEqual("Benjamin");
  });
});