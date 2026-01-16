import { StoneInterface, stoneType } from "./Stone";
import { DocumentData } from "firebase/firestore";
import { empowerStone, handicapStone } from "../../../api/firestore";
import { chickenTurningToHenCoordinates } from "./MovementRules";

interface shouldChickenTurnIntoHenInterface {
  amIOpponent: boolean;
  stashed: boolean;
  type: stoneType;
  movingToLetter: string;
  movingToNumber: number;
}

export const shouldChickenTurnIntoHen = ({
  amIOpponent,
  stashed,
  type,
  movingToLetter,
  movingToNumber,
}: shouldChickenTurnIntoHenInterface) => {
  if (type !== "CHICKEN") {
    return false;
  }

  if (stashed) {
    return false;
  }

  if (
    amIOpponent &&
    chickenTurningToHenCoordinates.opponent.includes(
      `${movingToLetter}${movingToNumber}`
    )
  ) {
    return true;
  }

  return (
    !amIOpponent &&
    chickenTurningToHenCoordinates.creator.includes(
      `${movingToLetter}${movingToNumber}`
    )
  );
};

export function transformChickenToHen(
  gameData: DocumentData,
  placedStoneId: string
) {
  empowerStone({
    gameId: gameData.gameId,
    stoneId: placedStoneId,
    type: "HEN",
  });
}

export function transformHenToChicken(
  gameData: DocumentData,
  lyingStoneId: string
) {
  handicapStone({
    gameId: gameData.gameId,
    stoneId: lyingStoneId,
    type: "CHICKEN",
  });
}

export function isHenGettingTaken(lyingStone: StoneInterface) {
  return lyingStone.type === "HEN";
}

export function isChickenTakingOver(draggedStone: StoneInterface) {
  return draggedStone.type === "CHICKEN";
}