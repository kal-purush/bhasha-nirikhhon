import { Point, Range, Editor, Transforms } from "slate";

const pointToStr = (point: Point | undefined) => {
  if (!point) {
    return "-";
  }
  let pointStr = "";
  pointStr += point.path.join(".");
  pointStr += " " + point.offset;
  return pointStr;
};

const rangeToStr = (range: Range) => {
  let rangeStr = "";
  rangeStr += pointToStr(range.anchor);
  rangeStr += " -> ";
  rangeStr += pointToStr(range.focus);
  return rangeStr;
};

export const wrapMentionBrackets = (editor: Editor, selection: Range) => {
  console.debug(rangeToStr(selection));

  const containingNode = Editor.node(editor, selection);
  console.debug(`containing node: ${JSON.stringify(containingNode)}`);

  const marksObj = Editor.marks(editor);
  const marks = marksObj ? Object.keys(marksObj) : [];

  if (!containingNode[0].text || marks.length > 0) {
    console.debug("cannot wrap a selection that is not simple text, ignoring");
    return;
  }

  const [leftPoint, rightPoint] = Range.edges(selection);

  Transforms.insertText(editor, "[", { at: leftPoint });
  const newRightPoint =
    Editor.after(editor, rightPoint, {
      distance: 1,
      unit: "character",
    }) || rightPoint;

  Transforms.insertText(editor, "]", { at: newRightPoint });
  const newLeftPoint =
    Editor.after(editor, leftPoint, {
      distance: 1,
      unit: "character",
    }) || leftPoint;

  const newAnchor = Range.isBackward(selection) ? newRightPoint : newLeftPoint;
  const newFocus = Range.isBackward(selection) ? newLeftPoint : newRightPoint;

  const newSelection = Editor.range(editor, newAnchor, newFocus);
  console.debug(`new selection: ${rangeToStr(newSelection)}`);

  Transforms.select(editor, newSelection);
};