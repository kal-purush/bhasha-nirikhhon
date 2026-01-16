import { colors } from '../colors';

export const components = {
  family: {
    primary: "Poppins",
  },

  lineHeight: {
    small: "40px",
    medium: "48px",
    large: "60px"
  },

  fontSize: {
    xxxxLarge: "40px",
    xxxLarge: "36px",
    xxLarge: "28px",
    xLarge: "18px",
    large: "16px",
    medium: "14px",
    small: "12px"
  },

  fontWeight: {
    extraBold: 800,
    bold: 700,
    semibold: 600,
    medium: 500,
    regular: 400
  },

};
export const display ={
  fontFamily:components.family.primary,
  size: components.fontSize.xxxxLarge,
  weight: components.fontWeight.bold,
  lineHeight: components.lineHeight.large,
  color: colors.brand.primary.medium,
}

