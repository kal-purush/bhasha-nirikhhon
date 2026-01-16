const {place} = require("../src/commands");
const {DirectionEnum} = require("../src/types");
const {InvalidLocationError} = require("../src/exceptions");

test("can place the robot", () => {
  const state = {
    location: null,
    facing: null,
    matrixSize: { xSize: 5, ySize: 5 },
  };
  const coordinates = { x: 1, y: 0 };
  const facing = DirectionEnum.NORTH;
  const expected = {
    location: coordinates,
    facing,
  };

  expect(place(state, coordinates, facing)).toEqual(expected);
});

test("cannot place the robot outside the table", () => {
  const state = {
    location: null,
    facing: null,
    matrixSize: { xSize: 5, ySize: 5 },
  };
  const coordinates = { x: 1, y: 6 };
  const facing = DirectionEnum.NORTH;
  const expected = {
    location: coordinates,
    facing,
  };

  expect(() => place(state, coordinates, facing)).toThrow(InvalidLocationError);
});