import { colors } from './colors';
export const fontFamily = {
  primary: {
    primary: "Poppins",
    weight: "54px",
    size: 700,
    height: "80px"

  },
};

export const fontLineHeight = {
  small: "100px",
  medium: "120px",
  large: "150px"
};

export const fontSize = {
  xxxxLarge: "40px",
  xxxLarge: "36px",
  xxLarge: "28px",
  xLarge: "18px",
  large: "16px",
  medium: "14px",
  small: "12px"
};

export const fontWeight = {
  extraBold: 800,
  bold: 700,
  semibold: 600,
  medium: 500,
  regular: 400

};

export const display = {
  fontFamily: fontFamily.primary,
  size: fontSize.xxxxLarge,
  weight: fontWeight.bold,
  lineHeight: fontLineHeight.large,
  color: colors.brand.primary.medium,
};

export const h1 = {
  fontFamily: fontFamily.primary,
  size: fontSize.xxxxLarge,
  weight: fontWeight.semibold,
  lineHeight: fontLineHeight.large,
  color: colors.brand.primary.medium,
};

export const h2 = {
  fontFamily: fontFamily.primary,
  size: fontSize.xxLarge,
  weight: fontWeight.regular,
  lineHeight: fontLineHeight.large,
  color: colors.brand.primary.medium,

};

export const h3 = {
  fontFamily: fontFamily.primary,
  size: fontSize.xLarge,
  weight: fontWeight.semibold,
  lineHeight: fontLineHeight.large,
  color: colors.neutral.neutralXDark,
};

export const h4 = {
  fontFamily: fontFamily.primary,
  size: fontSize.large,
  weight: fontWeight.extraBold,
  lineHeight: fontLineHeight.large,
  color: colors.brand.primary.medium,
};

export const body = {
  size: fontSize.xLarge,
  weight: fontWeight.regular,
  lineHeight: fontLineHeight.large,
  color: colors.neutral.neutralXDark,
};

export const body2 = {
  size: fontSize.large,
  weigth1: fontWeight.regular,
  color: colors.neutral.neutralXDark,
};

export const body3 = {
  size2: fontSize.large,

  weigth2: fontWeight.bold,
  color: colors.neutral.neutralXDark,
};

export const body4 = {
  size3: fontSize.large,
  weigth3: fontWeight.regular,
  color: colors.neutral.neutralXDark
};

export const definitionOfTheTerm = {
  size: fontSize.medium,
  fontWeight: {
    weight1: fontWeight.regular,
    weight2: fontWeight.bold,
  },

  lineHeight: fontLineHeight.large,
  color: colors.neutral.neutralXDark,
};

export const customStyles = {
  labelLargeBold: {
    size: fontSize.large,
    weight: fontWeight.bold,
    lineHeight: fontLineHeight.large,
    color: colors.brand.primary.dark,
  },

  labelLargeSemibold: {
    size: fontSize.large,
    weight: fontWeight.semibold,
    lineHeight: fontLineHeight.large,
    color: colors.brand.primary.dark,
  },

  labelLargeRegular: {
    size: fontSize.large,
    weight: fontWeight.regular,
    lineHeight: fontLineHeight.large,
    color: colors.brand.primary.dark,
  },

  largeLabel: {
    size: fontSize.medium,
    weight: fontWeight.regular,
    lineHeight: fontLineHeight.large,
    color: colors.neutral.white,
  },
};

export const capition = {
  size: fontSize.small,
  weight: fontWeight.regular,
  lineHeight: fontLineHeight.large,
  color: colors.neutral.neutralXDark,
};

export const price = {
  bigPrice: {
  size: fontSize.xxxLarge,
  weight: fontWeight.bold,
  lineHeight: fontLineHeight.large,
  color: colors.neutral.neutralXDark,
  },

  mediumPrice: {
    size: fontSize.xLarge,
    weight: fontWeight.bold,
    lineHeight: fontLineHeight.large,
    color: colors.neutral.neutralXDark,
  },

  smallPrice: {
    size: fontSize.medium,
    weight: fontWeight.regular,
    lineHeight: fontLineHeight.large,
    color: colors.neutral.neutralXDark,
  },
};