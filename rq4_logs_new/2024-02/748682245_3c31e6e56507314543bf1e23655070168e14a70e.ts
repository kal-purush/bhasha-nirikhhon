// purpose: add custom type for theme

import 'styled-components'

declare module 'styled-components' {
  export interface DefaultTheme {
    colors: ThemeColors
    fontSizes: ThemeFonts
    spacings: ThemeSpaces
  }
}

type ThemeColors = {
  primary: string
  primaryDark: string
  background: string
  background_transparent: string
  text: string
}

type ThemeFonts = {
  xs: string
  s: string
  m: string
  l: string
  xl: string
  xxl: string
  xxxl: string
}

type ThemeSpaces = {
  s: string
  m: string
  l: string
  xl: string
}