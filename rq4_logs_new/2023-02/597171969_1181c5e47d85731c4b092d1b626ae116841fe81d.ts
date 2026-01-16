import { Ghost } from "./ghost-class";
import type { SpriteGhostTypes } from "./types";

export function determineGhostState({
  blinking,
  colOrRow,
  eaten,
  ghost,
  position,
  sprites,
}: {
  blinking: boolean;
  colOrRow: "col" | "row";
  eaten: boolean;
  ghost: Ghost;
  position: "top" | "right" | "bottom" | "left";
  sprites: SpriteGhostTypes;
}) {
  if (blinking) {
    ghost.scaredAboutToExpireTimer--;
    if (ghost.scaredAboutToExpireTimer === 0) {
      ghost.scaredAboutToExpireTimer = ghost.scaredAboutToExpireTimerDefault;
      if (ghost.blinking) {
        ghost.blinking = false;
      } else {
        ghost.blinking = true;
      }
    }
  }
  const index = colOrRow === "col" ? 0 : 1;
  if (eaten) {
    return sprites.eaten[position][index];
  } else if (blinking) {
    return sprites.flash[position][index];
  } else {
    return sprites.scared[position][index];
  }
}