export enum Display {
  FLEX = "flex",
  BLOCK = "block"
}

export enum FlexDirection {
  ROW = "row",
  ROW_REVERSE = "row-reverse",
  COLUMN = "column",
  COLUMN_REVERSE = "column-reverse"
}

export enum FlexWrap {
  NO_WRAP = "nowrap",
  WRAP = "wrap",
  WRAP_REVERSE = "wrap-reverse"
}

export enum JustifyContent {
  FLEX_START = "flex-start",
  FLEX_END = "flex-end",
  CENTER = "center",
  SPACE_BETWEEN = "space-between",
  SPACE_AROUND = "space-around",
  SPACE_EVENLY = "space-evenly"
}

export enum AlignItems {
  FLEX_START = "flex-start",
  FLEX_END = "flex-end",
  CENTER = "center",
  STRETCH = "stretch",
  BASELINE = "baseline"
}

export enum AlignSelf {
  AUTO = "auto",
  FLEX_START = "flex-start",
  FLEX_END = "flex-end",
  CENTER = "center",
  BASELINE = "baseline",
  STRETCH = "stretch"
}

export enum AlignContent {
  FLEX_START = "flex-start",
  FLEX_END = "flex-end",
  CENTER = "center",
  STRETCH = "stretch",
  SPACE_BETWEEN = "space-between",
  SPACE_AROUND = "space-around"
}

export interface MarginProps {
  margin: string | number;
  marginTop: string | number;
  marginBottom: string | number;
  marginLeft: string | number;
  marginRight: string | number;
  marginX: string | number;
  marginY: string | number;
}

export interface PaddingProps {
  padding: string | number;
  paddingTop: string | number;
  paddingBottom: string | number;
  paddingLeft: string | number;
  paddingRight: string | number;
  paddingX: string | number;
  paddingY: string | number;
}

export interface Size {
  width: number | string;
  height: number | string;
}

export interface FlexboxProps extends Size, PaddingProps, MarginProps {
  flex: boolean;
  direction: FlexDirection;
  wrap: FlexWrap;
  justify: JustifyContent;
  align: AlignItems;
  self: AlignSelf;
  content: AlignContent;
}

export enum Positions {
  TOP = 0,
  RIGHT = 1,
  BOTTOM = 2,
  LEFT = 3
}