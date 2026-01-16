export const fontWeight = {
  light: 300,
  regular: 400,
  medium: 500,
  bold: 700,
} as const;

export const fontFamily = {
  interLight: 'Inter_300Light',
  interRegular: 'Inter_400Regular',
  interBold: 'Inter_700Bold',
  interMedium: 'Inter_500Medium',
} as const;

export const fontSizes = {
  titleXLarge: 36,
  titleLarge: 32,
  titleMedium: 28,
  titleSmall: 24,
  paragraphXXLarge: 22,
  paragraphXLarge: 20,
  paragraphLarge: 18,
  paragraphMedium: 16,
  paragraphSmall: 14,
  paragraphXSmall: 12,
} as const;

export const lineHeights = {
  titleXLarge: 36,
  titleLarge: 32,
  titleMedium: 28,
  titleSmall: 24,
  paragraphXXLarge: 22,
  paragraphXLarge: 20,
  paragraphLarge: 18,
  paragraphMedium: 21.6,
  paragraphSmall: 14,
  paragraphXSmall: 12,
} as const;

export default {
  fontWeight,
  fontSizes,
  fontFamily,
  lineHeights,
};