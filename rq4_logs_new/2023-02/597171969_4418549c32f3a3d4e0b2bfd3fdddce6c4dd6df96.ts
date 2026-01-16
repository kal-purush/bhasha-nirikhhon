import { Boundary } from "./main";

export const fakeVar = "prevent TS Error";
// BEG FAKE TROUGH
const keys = {
  ArrowUp: { pressed: false },
  ArrowDown: { pressed: false },
  ArrowLeft: { pressed: false },
  ArrowRight: { pressed: false },
};
const boundaries: Boundary[] = [];

// END FAKE TROUGH

// Character movement
if (keys.ArrowUp.pressed && lastKey === "ArrowUp") {
  for (let i = 0; i < boundaries.length; i++) {
    const boundary = boundaries[i];
    if (
      circleCollidesWithRectangle({
        circle: { ...player, velocity: { x: 0, y: -5 } },
        rectangle: boundary,
      })
    ) {
      player.velocity.y = 0;
      break;
    } else {
      player.velocity.y = -5;
    }
  }
} else if (!keys.ArrowUp.pressed && lastKey === "ArrowUp") {
  player.velocity.y = 0;
} else if (keys.ArrowDown.pressed && lastKey === "ArrowDown") {
  for (let i = 0; i < boundaries.length; i++) {
    const boundary = boundaries[i];
    if (
      circleCollidesWithRectangle({
        circle: { ...player, velocity: { x: 0, y: 5 } },
        rectangle: boundary,
      })
    ) {
      player.velocity.y = 0;
      break;
    } else {
      player.velocity.y = 5;
    }
  }
} else if (!keys.ArrowDown.pressed && lastKey === "ArrowDown") {
  player.velocity.y = 0;
} else if (keys.ArrowLeft.pressed && lastKey === "ArrowLeft") {
  for (let i = 0; i < boundaries.length; i++) {
    const boundary = boundaries[i];
    if (
      circleCollidesWithRectangle({
        circle: { ...player, velocity: { x: -5, y: 0 } },
        rectangle: boundary,
      })
    ) {
      player.velocity.x = 0;
      break;
    } else {
      player.velocity.x = -5;
    }
  }
} else if (!keys.ArrowLeft.pressed && lastKey === "ArrowLeft") {
  player.velocity.x = 0;
} else if (keys.ArrowRight.pressed && lastKey === "ArrowRight") {
  for (let i = 0; i < boundaries.length; i++) {
    const boundary = boundaries[i];
    if (
      circleCollidesWithRectangle({
        circle: { ...player, velocity: { x: 5, y: 0 } },
        rectangle: boundary,
      })
    ) {
      player.velocity.x = 0;
      break;
    } else {
      player.velocity.x = 5;
    }
  }
} else if (!keys.ArrowRight.pressed && lastKey === "ArrowRight") {
  player.velocity.x = 0;
}