import { StyleSheet } from 'react-native';
import { moderateScale, verticalScale } from '@Theme/Metrics';

export const CurrentProviderStyle = (themeColors: any) =>
  StyleSheet.create({
    headerText: { alignSelf: 'center' },
    linkText: {
      alignSelf: 'center',
      color: themeColors.currentProvider.link,
      fontSize: moderateScale(18),
    },
    linkTextDetail: {
      alignSelf: 'center',
      color: themeColors.currentProvider.link,
      fontSize: moderateScale(12),
    },
    detailText: {
      alignSelf: 'center',
      textAlign: 'left',
      lineHeight: moderateScale(15.2),
      letterSpacing: moderateScale(0.5),
      fontSize: moderateScale(12),
    },
    selectionView: {
      flexDirection: 'row',
      marginTop: moderateScale(7),
      height: verticalScale(130),
      justifyContent: 'space-between',
    },
    createAccountDefault: {
      flex: 0.48,
      alignItems: 'center',
      borderWidth: moderateScale(1.5),
      borderRadius: moderateScale(4),
      borderColor: themeColors.currentProvider.borderColor,
      backgroundColor: themeColors.currentProvider.defaultBgColor,
      padding: moderateScale(10),
      justifyContent: 'space-evenly',
    },
    createAccountTitle: {
      alignSelf: 'center',
      lineHeight: moderateScale(15.2),
      letterSpacing: moderateScale(0.5),
      textAlign: 'center',
      fontFamily: 'SpaceGrotesk-Bold',
      color: themeColors.currentProvider.titleColor,
    },
    providerInfo: {
      justifyContent: 'center',
      alignItems: 'center',
      marginTop: moderateScale(5),
    },
    sendText: { color: themeColors.otpVerification.sendTextColor },
    textLabel: { marginTop: moderateScale(13) },
    infoText: {
      color: themeColors.currentProvider.titleUnSelected,
      fontSize: moderateScale(11),
    },
  });