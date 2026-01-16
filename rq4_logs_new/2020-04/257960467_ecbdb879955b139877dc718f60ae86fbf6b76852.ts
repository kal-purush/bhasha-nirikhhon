import * as shortid from "shortid";

import { TYPES } from "../constants";

export class Word {
  readonly value: string;
  readonly type: TYPES;
  readonly id: string;

  constructor(value: string, type: TYPES) {
    this.value = value;
    this.type = type;
    this.id = shortid.generate() as string;
  }
}

class WordCreator {
  createRedWord(value: string): Word {
    return new Word(value, TYPES.RED);
  }
  createBlueWord(value: string): Word {
    return new Word(value, TYPES.BLUE);
  }
  createBlackWord(value: string): Word {
    return new Word(value, TYPES.BLACK);
  }
  createNeutralWord(value: string): Word {
    return new Word(value, TYPES.NEUTRAL);
  }
}

export default WordCreator;