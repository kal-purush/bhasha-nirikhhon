import { atom } from "recoil";
import { DatabaseConfigType } from "utils";

export type DatabaseConfigState = DatabaseConfigType | undefined;

export const databaseConfigState = atom<DatabaseConfigState>({
  key: "databaseConfigDisplayColumnsState",
  default: undefined,
});