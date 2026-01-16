import {
  Theme,
  DarkTheme as Dark,
  DefaultTheme as Default,
} from "@react-navigation/native";

const DefaultTheme = {
  ...Default,
  roundness: 2,
  colors: {
    ...Default.colors,
    primary: "red",
    background: "red",
    card: "red",
    text: "red",
  },
};

const DarkTheme = {
  ...Dark,
  roundness: 2,
  colors: {
    ...Dark.colors,
    primary: "#007AFF",
    accent: "#FFC107",
    background: "red",
    surface: "#121212",
    text: "#FFFFFF",
    disabled: "#A0A0A0",
    placeholder: "#B0B0B0",
    backdrop: "#FFFFFF",
  },
};

export { DefaultTheme, DarkTheme };