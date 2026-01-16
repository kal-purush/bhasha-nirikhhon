import {
  RiAlignCenter,
  RiAlignJustify,
  RiAlignLeft,
  RiAlignRight,
  RiFontMono,
  RiFontSans,
  RiFontSansSerif,
  RiH1,
  RiH2,
  RiH3,
  RiH4,
} from "react-icons/ri";

export const colors = [
  "default",
  "gray",
  "brown",
  "orange",
  "yellow",
  "green",
  "blue",
  "purple",
  "pink",
  "red",
];

export const textAlignments = [
  {
    content: "Align left",
    icon: RiAlignLeft,
    value: "left",
  },
  {
    content: "Align center",
    icon: RiAlignCenter,
    value: "center",
  },
  {
    content: "Align right",
    icon: RiAlignRight,
    value: "right",
  },
  {
    content: "Align justify",
    icon: RiAlignJustify,
    value: "justify",
  },
];

export const fontFamilies = [
  {
    content: "Sans",
    icon: RiFontSans,
    value: "open-sans",
  },
  {
    content: "Sans serif",
    icon: RiFontSansSerif,
    value: "sans-serif",
  },
  {
    content: "Mono",
    icon: RiFontMono,
    value: "monospace",
  },
];

export const headings = [
  {
    content: "Heading 1",
    icon: RiH1,
    value: 1,
  },
  {
    content: "Heading 2",
    icon: RiH2,
    value: 2,
  },
  {
    content: "Heading 3",
    icon: RiH3,
    value: 3,
  },
  {
    content: "Heading 4",
    icon: RiH4,
    value: 4,
  },
];