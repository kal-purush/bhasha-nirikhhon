//
// `arrow-body-style`
//
// Is completely unchecked, there is no enforced style.
export const shortOneLiner = () => void 0;

export const littleLonger = () =>
  shortOneLiner() || void 0;

export const multipleLines = () =>
  littleLonger(
    shortOneLiner(
      void 0));

export const returnIsOk = () => {
  return littleLonger(
    shortOneLiner(
      void 0));
};