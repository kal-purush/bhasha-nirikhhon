import habitInfo from "../../../../../store/habitInfo";
import pillInfo from "../../../../../store/pillInfo";
import useHabitInfoStore from "../../../../../store/useHabitInfoStore";
import { usePillInfoStore } from "../../../../../store/usePillInfoStore";
import { isHabitChallenge, isPillChallenge } from "./determineChallenge";

export const getChallengeCopy = <T>(
  challengeType: string,
  challengeId: string
): Omit<T, "id"> => {
  let challengeCopy: any = null;
  const { getPillCopy } = usePillInfoStore();
  const { getHabitCopy } = useHabitInfoStore();

  if (isPillChallenge(challengeType)) {
    challengeCopy = getPillCopy(challengeId);
  }

  if (isHabitChallenge(challengeType)) {
    challengeCopy = getHabitCopy(challengeId);
  }

  return challengeCopy as Omit<T, "id">;
};