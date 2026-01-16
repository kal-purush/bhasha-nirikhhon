import { atom } from "recoil";

export const firstDataSortKeyAtom = atom({
  key: "firstSelector",
  default: "ROAS"
});

export const secondDataSortKeyAtom = atom({
  key: "secondSelector",
  default: "선택"
});