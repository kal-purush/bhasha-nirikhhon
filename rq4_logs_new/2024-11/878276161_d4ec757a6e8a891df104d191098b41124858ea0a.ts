export const variantMapping = {
  h1: "h1",
  h2: "h2",
  h3: "h3",
  h4: "h4",
  h5: "h5",
  h6: "h6",
  "body-reg": "p",
  "caption-mid": "p",
  "caption-reg": "p",
};

export type TextVariant = keyof typeof variantMapping;

export type TextAlign =
  | "left"
  | "center"
  | "right"
  | "end"
  | "start"
  | "justify";

export type TextFamily = "gambetta" | "proxima-nova" | "playfair-display";

export type TextWeight =
  | "thin"
  | "extra-light"
  | "light"
  | "regular"
  | "medium"
  | "semi-bold"
  | "bold"
  | "extra-bold"
  | "black";

export type TextColor =
  | "af-green"
  | "af-yellow"
  | "af-lemon"
  | "af-light-lemon"
  | "af-dark-green"
  | "af-white"
  | "floral-white"
  | "af-light-grey"
  | "af-dark-grey";

export interface TextProps extends React.HTMLAttributes<HTMLOrSVGElement> {
  tag?: keyof JSX.IntrinsicElements;
  variant?: TextVariant;
  fontFamily?: TextFamily;
  align?: TextAlign;
  fontWeight?: TextWeight;
  color?: TextColor;
  customClassName?: string;
  children?: React.ReactNode;
}