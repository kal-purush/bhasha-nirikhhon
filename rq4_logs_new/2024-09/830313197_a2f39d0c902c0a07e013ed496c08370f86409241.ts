import { colors } from "@/constants/Colors";
import { StyleSheet } from "react-native";

export const authStyles = StyleSheet.create({
  container: {
    display: "flex",
    flexDirection: "column",
    width: "100%",
    rowGap: 20,
  },
  title: {
    fontSize: 24,
    fontWeight: "bold",
    textAlign: "left",
  },
  form: {
    width: "100%",
    justifyContent: "center",
    alignItems: "center",
    rowGap: 10,
  },
  inputWrapper: {
    position: "relative",
    width: "100%",
  },
  formInput: {
    width: "100%",
    paddingHorizontal: 10,
    paddingVertical: 15,
    borderRadius: 5,
    borderWidth: 1,
    borderColor: colors.neutral.medium,
    color: colors.neutral.dark,
  },
  invalidInput: {
    borderColor: colors.alert.error,
  },
  errorTextContainer: {
    position: "absolute",
    right: 10,
    top: "50%",
    transform: [{ translateY: -8 }],
    flexDirection: "row",
    alignItems: "center",
    pointerEvents: "none",
  },
  errorText: {
    color: colors.alert.error,
    fontSize: 12,
    marginLeft: 5,
  },
  alert: {
    display: "flex",
    flexDirection: "row",
    gap: 5,
    width: "100%",
    justifyContent: "flex-start",
  },
  alertIcon: {
    color: colors.alert.error,
  },
  privacy: {
    textAlign: "left",
  },
  link: {
    color: colors.primary.light,
  },
  spinnerContainer: {
    width: "100%",
    justifyContent: "center",
    alignItems: "center",
  },
  authLink: {
    textAlign: "center",
  },
  socialMediaContainer: {
    display: "flex",
    flexDirection: "row",
    justifyContent: "space-evenly",
    alignItems: "center",
    width: "100%",
  },
});