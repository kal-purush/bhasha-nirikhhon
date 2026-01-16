import { contextBoundaryReducer } from "./cardState";
import { setContextBoundaries } from "../actions";

it("context boundaries set properly", () => {
  const action = setContextBoundaries(2, 4);
  const updated = contextBoundaryReducer(void 0, action);

  expect(updated).toEqual({
    start: 2,
    length: 4
  });
});