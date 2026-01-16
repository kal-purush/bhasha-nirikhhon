export const getDom = (dom = ".grow"): any => {
  return document.querySelector(dom).getBoundingClientRect();
};

export const setDomWidthHeight = (w, h, dom = ".main-content"): any => {
  let width = `calc(${w}px - 24px*2)`;
  let height = `calc(${h}px - 24px*2)`;
  if (dom !== ".main-content") {
    width = `${w}px`;
    height = `${h}px`;
  }
  document.querySelector(dom).style.cssText =
    `width: ${width};height: ${height}`;
};