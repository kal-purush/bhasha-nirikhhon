import { colors } from "@/constants/Colors";
import { StyleSheet, TextStyle } from "react-native";

const GlobalStyles = StyleSheet.create({
  title: {
    fontSize: 28 as TextStyle["fontSize"], // Explicit type casting
    fontWeight: "900" as TextStyle["fontWeight"], // Explicit type casting
    color: colors.neutral.dark as TextStyle["color"], // Explicit type casting
  },
  h1: {
    fontSize: 24 as TextStyle["fontSize"],
    fontWeight: "700" as TextStyle["fontWeight"],
    color: colors.neutral.dark as TextStyle["color"],
  },
  h2: {
    fontSize: 20 as TextStyle["fontSize"],
    fontWeight: "500" as TextStyle["fontWeight"],
    color: colors.neutral.dark as TextStyle["color"],
  },
  h3: {
    fontSize: 16 as TextStyle["fontSize"],
    fontWeight: "500" as TextStyle["fontWeight"],
    color: colors.neutral.dark as TextStyle["color"],
  },
  body: {
    fontSize: 14 as TextStyle["fontSize"],
    fontWeight: "500" as TextStyle["fontWeight"],
    color: colors.neutral.dark as TextStyle["color"],
  },
  link: {
    fontSize: 14 as TextStyle["fontSize"],
    fontWeight: "500" as TextStyle["fontWeight"],
    color: colors.primary.light as TextStyle["color"],
  },
  alert: {
    fontSize: 12 as TextStyle["fontSize"],
    color: colors.alert.error as TextStyle["color"],
  },
});

export default GlobalStyles;