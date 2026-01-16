import * as strategies from "../TextClickStrategies";
import * as _ from "lodash";
import * as actions from "../actions";
import { getType } from "typesafe-actions";

it("context boundaries properly set", () => {
  const wordCount = 10;
  const contextSelector = new strategies.ContextSelector(wordCount);

  const wordsToClick = _.range((wordCount - 1) / 2).concat(
    _.range((wordCount - 1) / 2, 0)
  );

  wordsToClick.forEach(i => {
    expect(contextSelector.onWordClick(i)).toEqual({
      type: getType(actions.setContextBoundaries),
      payload: {
        start: i,
        length: wordCount - i
      },
      meta: undefined
    });
  });
});