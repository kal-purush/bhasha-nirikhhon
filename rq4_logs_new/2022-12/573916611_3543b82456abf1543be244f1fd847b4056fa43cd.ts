import { alphabet, Letter } from "./alphabet.ts";

export type Dictionary = Word[];

export type NoSay = { silent: string };

export type Word = {
  word: string,
  sounds: (Letter | NoSay)[],
  audio: string,
  picture: string,
}

function error<T>(message: string): T {
  throw new Error(message);
}

function s(silent: string): NoSay {
  return { silent };
}

function l(letter: string): Letter {
  return alphabet.find(a => a.letter == letter) || error("Not found")
}

export const dictionary: Dictionary = [
  {
    word: "cat",
    sounds: [l("c"), l("a"), l("t")],
    audio: "cat.m4a",
    picture: "cat.png",
  },
  {
    word: "cab",
    sounds: [l("c"), l("a"), l("b")],
    audio: "cab.m4a",
    picture: "cab.png",
  },
  // {
  //   word: "car",
  //   sounds: [l("c"), l("a"), l("r")],
  //   audio: "car.m4a",
  //   picture: "car.png",
  // },
];