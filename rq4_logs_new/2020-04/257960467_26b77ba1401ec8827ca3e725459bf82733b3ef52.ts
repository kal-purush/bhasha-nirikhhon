import wordActions, { StateWord, WORD_ACTION } from "../words.actions";
import { TYPES } from "../../../api/constants";

describe("wordActions", function () {
  it("wordActions.pickWord should return proper action payload", () => {
    expect(wordActions.pickWord(0)).toEqual({
      type: WORD_ACTION.PICK_WORD,
      payload: {
        cardIndex: 0,
      },
    });
  });

  it("wordActions.showTypes should return proper action payload", () => {
    expect(wordActions.showTypes()).toEqual({
      type: WORD_ACTION.SHOW_TYPES,
    });
  });

  it("wordActions.unpickWords should return proper action payload", () => {
    expect(wordActions.unpickWords()).toEqual({
      type: WORD_ACTION.UNPICK_WORDS,
    });
  });

  it("wordActions.setWords should return proper action payload", () => {
    const words: StateWord[] = [
      {
        index: 0,
        id: "id",
        type: TYPES.RED,
        showType: false,
        isPicked: false,
        value: "test",
      },
    ];
    expect(wordActions.setWords(words)).toEqual({
      type: WORD_ACTION.SET_WORDS,
      payload: {
        words,
      },
    });
  });
});