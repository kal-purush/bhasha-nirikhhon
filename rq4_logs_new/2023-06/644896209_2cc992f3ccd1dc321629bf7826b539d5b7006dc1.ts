import { StyleSheet } from 'react-native';
import { otpInput } from '@src/theme/Dimensions';

export const OtpInputStyle = (themeColors: any) =>
  StyleSheet.create({
    container: {
      marginTop: otpInput.marginTop,
      flexDirection: 'row',
      justifyContent: 'space-between',
      alignItems: 'center',
    },
    input: {
      borderWidth: otpInput.borderWidth,
      backgroundColor: themeColors.otpInput.backgroundColor,
      borderColor: themeColors.otpInput.borderColor,
      color: themeColors.otpInput.color,
      padding: otpInput.padding,
      marginHorizontal: otpInput.marginHorizontal,
      borderRadius: otpInput.borderRadius,
      width: otpInput.width,
      height: otpInput.height,
      textAlign: 'center',
      fontSize: otpInput.fontSize,
      fontFamily: 'Space Grotesk',
      fontStyle: 'normal',
    },
    activeInput: {
      borderColor: themeColors.otpInput.activeInput.borderColor,
    },
    enteredInput: {
      borderColor: themeColors.otpInput.enteredInput.borderColor,
    },
    errorText: {
      color: themeColors.otpInput.errorText.color,
      borderColor: themeColors.otpInput.errorText.borderColor,
    },
  });