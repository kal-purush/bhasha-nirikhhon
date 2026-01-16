import { StyleSheet } from "react-native";
import { colors } from "@/constants/Colors";

export const accountStyles = StyleSheet.create({
  container: {
    flex: 1,
    display: "flex",
    flexDirection: "column",
    justifyContent: "space-between",
    alignItems: "flex-start",
  },
  accountContent: {
    display: "flex",
    flexDirection: "column",
    rowGap: 10,
    width: "100%",
    padding: 20,
  },
  userContent: {
    display: "flex",
    flexDirection: "column",
    width: "100%",
  },
  userInfo: {
    color: colors.neutral.medium,
  },
  profileContent: {
    display: "flex",
    flexDirection: "column",
    width: "100%",
  },
  profileInfo: {
    color: colors.neutral.medium,
  },
  buttonContainer: {
    display: "flex",
    flexDirection: "column",
    alignItems: "center",
    width: "100%",
    rowGap: 10,
  },
  input: {
    width: "100%",
    color: colors.neutral.dark,
  },
});