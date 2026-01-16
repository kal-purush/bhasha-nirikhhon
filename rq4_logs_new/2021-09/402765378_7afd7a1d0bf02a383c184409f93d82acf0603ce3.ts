import { Part } from "../../types/Interface";

export const partPropsContstants = {
  ROLE: "role",
  MIN_MD: "minMd",
  MAX_MD: "maxMd",
  PREDICTED_MD: "predictedMd",
  PREDICTED_MD_FORMULA: "predictedMdFormula",
};

export default function updatePart(
  part: Part,
  partProps: string,
  updatedValue: string
) {
  const { ROLE, MIN_MD, MAX_MD, PREDICTED_MD, PREDICTED_MD_FORMULA } =
    partPropsContstants;

  const updatePart = {
    ...part,
    minMd: partProps === MIN_MD ? Number(updatedValue) : part.minMd,
    maxMd: partProps === MAX_MD ? Number(updatedValue) : part.maxMd,
    predictedMd:
      partProps === PREDICTED_MD ? Number(updatedValue) : part.predictedMd,
    predictedMdFormula:
      partProps === PREDICTED_MD_FORMULA
        ? (updatedValue as string)
        : part.predictedMdFormula,
    role: partProps === ROLE ? (updatedValue as string) : part.role,
  };
  return updatePart;
}