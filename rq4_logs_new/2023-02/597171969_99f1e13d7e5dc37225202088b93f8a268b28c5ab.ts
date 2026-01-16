import { SpriteGhostTypes } from "./types";

export const sprites: SpriteGhostTypes = {
  blinky: {
    top: [0, 0],
    right: [1, 0],
    bottom: [1, 1],
    left: [0, 1],
  },
  inky: { top: [2, 2], right: [3, 2], bottom: [3, 3], left: [2, 3] },
  pinky: { top: [0, 2], right: [3, 0], bottom: [3, 1], left: [2, 1] },
  clyde: { top: [2, 0], right: [1, 2], bottom: [1, 3], left: [0, 3] },
  eaten: { top: [4, 2], right: [5, 2], bottom: [5, 3], left: [4, 3] },
  scared: { top: [9, 0], right: [9, 0], bottom: [9, 0], left: [9, 0] },
  flash: { top: [8, 0], right: [8, 0], bottom: [8, 0], left: [8, 0] },
};