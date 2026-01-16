import { GameItemsDictionary } from "@/types/socket";
import { atom } from "jotai";

export const itemsAtom = atom<GameItemsDictionary>({});