import { colors } from "@src/theme/colors";
import { StyleSheet } from "react-native";

export const GlobalStyle = StyleSheet.create({
  appButtonContainer: {
    height: 50,
    backgroundColor: colors.light.actionButtonBgColor,
    borderRadius: 4,
    marginHorizontal: 24,
    justifyContent: 'center',
    alignItems: 'center',
  },
  appButtonText: {
    fontSize: 18,
    fontFamily: 'Neue Haas Grotesk Text Pro',
    fontStyle: 'normal',
    letterSpacing: 0.1,
    color: colors.light.actionButtonTitleColor,
    fontWeight: '700',
    alignSelf: 'center',
  },
})