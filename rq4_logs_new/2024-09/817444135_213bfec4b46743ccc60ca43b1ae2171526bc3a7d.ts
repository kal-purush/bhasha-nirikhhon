import {
  firstSample,
  firstSampleFemale,
  secondSample,
  secondSampleFemale,
  zeroSample,
  zeroSampleFemale,
} from "../../utils/const/probas.ts"
import { Step, UserData } from "../../types/accordion.ts"


interface LoadUserDataParams {
  currentUserData: UserData;
}

export const loadUserData = ({ currentUserData }: LoadUserDataParams): Step[] => {
  if (!currentUserData) return []

  return [
    {
      title: "Нульова проба",
      data: currentUserData.sex === "MALE" ? zeroSample : zeroSampleFemale,
      checked: currentUserData.zeroProba,
      probaType: "zeroProba",
    },
    {
      title: "Перша проба",
      data: currentUserData.sex === "MALE" ? firstSample : firstSampleFemale,
      checked: currentUserData.firstProba,
      probaType: "firstProba",
    },
    {
      title: "Друга проба",
      data: currentUserData.sex === "MALE" ? secondSample : secondSampleFemale,
      checked: currentUserData.secondProba,
      probaType: "secondProba",
    },
  ]
}