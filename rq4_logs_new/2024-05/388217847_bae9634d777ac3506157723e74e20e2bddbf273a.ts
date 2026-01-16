import colors from '@/tokens/colors'

const { infoLight, white, grey900, info } = colors

export enum Themes {
  DARK = 'dark',
  LIGHT = 'light',
  INFO = 'info',
  PURPLE = 'purple'
}

export const colorsArrow = {
  [Themes.DARK]: grey900,
  [Themes.LIGHT]: white,
  [Themes.INFO]: infoLight,
  [Themes.PURPLE]: info
}

export type TooltipThemes = `${Themes}`

export type Strategies = 'absolute' | 'fixed'