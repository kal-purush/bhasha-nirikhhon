import { FontWeight } from '@constants/styles';

export interface ThemeSizes {
  sm?: string;
  md: string;
  lg?: string;
  xl?: string;
}

interface CarouselParams {
  maxHeight: string;
  length: number;
  animationDuration: number;
}

interface ButtonParams {
  paddingX: string;
  paddingY: ThemeSizes;
}

interface SliderParams {
  height: string;
  thumbHeight: string;
  thumbWidth: string;
  step: number;
}

interface ZIndexValues {
  headerMenu: number;
  filterMenu: number;
}

export interface ThemeConstants {
  productCardWidth: ThemeSizes;
  iconSize: ThemeSizes;
  gap: ThemeSizes;
  transitionTime: ThemeSizes;
  carousel: CarouselParams;
  boxShadow: string;
  borderRadius: ThemeSizes;
  button: ButtonParams;
  slider: SliderParams;
  zIndexes: ZIndexValues;
  fontWeight: typeof FontWeight;
}