import { WordCardProps } from "../../components/WordCard/WordCard";

export enum WORD_ACTION {
  PICK_WORD = "PICK_WORD",
  SET_WORDS = "SET_WORDS",
  UNPICK_WORDS = "UNPICK_WORDS",
  SHOW_TYPES = "SHOW_TYPE",
}

export interface StateWord extends WordCardProps {
  id: string;
  showType: boolean;
}

interface PickWordAction {
  type: WORD_ACTION.PICK_WORD;
  payload: {
    cardIndex: number;
  };
}

interface UnpickWordsAction {
  type: WORD_ACTION.UNPICK_WORDS;
}

interface SetWordsAction {
  type: WORD_ACTION.SET_WORDS;
  payload: {
    words: StateWord[];
  };
}

interface ShowTypesAction {
  type: WORD_ACTION.SHOW_TYPES;
}

export type WordActionsType = ShowTypesAction | SetWordsAction | UnpickWordsAction | PickWordAction;

function pickWord(cardIndex: number): WordActionsType {
  return {
    type: WORD_ACTION.PICK_WORD,
    payload: {
      cardIndex,
    },
  };
}

function unpickWords(): WordActionsType {
  return {
    type: WORD_ACTION.UNPICK_WORDS,
  };
}

function setWords(words: StateWord[]): WordActionsType {
  return {
    type: WORD_ACTION.SET_WORDS,
    payload: {
      words,
    },
  };
}

function showTypes(): WordActionsType {
  return {
    type: WORD_ACTION.SHOW_TYPES,
  };
}

export default {
  pickWord,
  unpickWords,
  setWords,
  showTypes,
};