import React from 'react'
import { Theme } from '../../theme/'
import { Spaces, Dimensions } from '../../styled-system/'

type PaletteKeys = keyof Theme['palette']

export interface StyledTextProps extends Spaces, Dimensions {
  /** Размер текста */
  fontSize?: 's' | 'm' | 'l' | 'xl' | 'xxl' | 'xxxl' | 'xxxxl' | 'xxxxxl',
  /** Коэффициент для высоты строки */
  lineHeight?: 's' | 'm' | 'l',
  /** Расстояние между буквами */
  letterSpacing?: string,
  /** Текстовое содержание */
  children?: React.ReactNode,
  /** Текстовое содержание */
  weight?: 'light' | 'normal' | 'bold',
  /** Курсивное начертание */
  italic?: boolean,
  /** Текст в верхнем регистре */
  uppercase?: boolean,
  /** Подчеркнутый текст */
  underline?: 'dashed' | 'dotted' | boolean,
  /** Смещение подчеркнутой линии */
  underlineOffset?: string,
  /** Обрезка высоты строки у текста сверху и снизу */
  crop?: boolean,
  /** Текстовая нода */
  as?: 'string',
  /** Цвет текста в обычном состояни */
  color?: PaletteKeys,
  /** Цвет текста в состояни ховера */
  colorHover?: PaletteKeys,
  /** Цвет текста в нажатом состояни */
  colorActive?: PaletteKeys,
  /** Цвет текста в активном состояни */
  colorChecked?: PaletteKeys,
  /** Цвет текста в состояни фокуса */
  colorFocus?: PaletteKeys,
  /** Цвет текста в неактивном состояни */
  colorDisabled?: PaletteKeys,
  /** Горизонтальное выравнивание текста */
  align?: 'start' | 'end' | 'center' | 'justify',
  /** Вертикальное выравнивание текста */
  valign?: 'top' | 'middle' | 'bottom' | 'baseline',
  /** Текст без переносов */
  noWrap?: boolean,
  /** Обрезать текст с помощью многоточия */
  truncate?: boolean,
  /** @ignore */
  theme?: Theme,
}