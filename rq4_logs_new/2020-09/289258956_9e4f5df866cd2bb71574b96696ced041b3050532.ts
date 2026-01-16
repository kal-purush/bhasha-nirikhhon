import { ThemeTextColors } from 'src/@types/StyledComponentsTheme';

export enum ButtonType {
  Contained = 'contained',
  Outlined = 'outlined',
}

export type ButtonComponentProps = {
  /** @default Contained */
  buttonType?: ButtonType;
  /**
   * Determines button's color mode.
   * @default Dark
   */
  color?: keyof ThemeTextColors;
  /** Background color used for contained mode */
  background?: string;
  /** Border color used for outlined mode */
  strokeColor?: string;
  className?: string;
  onClick?: (event: React.MouseEvent<HTMLButtonElement, MouseEvent>) => void;
};