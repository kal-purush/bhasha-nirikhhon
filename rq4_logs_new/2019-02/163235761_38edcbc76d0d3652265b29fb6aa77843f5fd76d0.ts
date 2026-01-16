import { css } from 'styled-components';

import {
  ButtonSize,
  ButtonVariant,
  ButtonColor,
  WrapperProps,
} from './component';

export function getButtonSizeStyles(
  size?: ButtonSize,
): ReturnType<typeof css> | null {
  if (size === 'small') {
    return css`
      font-size: var(--xs-font-size);
      padding: 0 var(--sm-padding);
      height: 1.4rem;
    `;
  }

  if (size === 'medium') {
    return css`
      font-size: var(--sm-font-size);
      padding: 0 var(--md-padding);
      height: 1.5rem;
    `;
  }

  if (size === 'large') {
    return css`
      font-size: var(--md-font-size);
      padding: 0 var(--xl-padding);
      height: 1.8rem;
    `;
  }

  return null;
}

export type GetButtonVariantStylesArgs = Readonly<{
  variant: ButtonVariant;
  color: ButtonColor;
  isPointerOver: boolean;
  isPointerDown: boolean;
}>;

export function getButtonVariantStyles(args: GetButtonVariantStylesArgs): any {
  if (args.variant === 'contained') {
    let backgroundColor: string;
    let backgroundColorWhenPointerOver: string;

    if (args.color === 'primary') {
      backgroundColor = 'var(--primary-color-500)';
      backgroundColorWhenPointerOver = 'var(--primary-color-600)';
    } else if (args.color === 'accent') {
      backgroundColor = 'var(--accent-color-500)';
      backgroundColorWhenPointerOver = 'var(--accent-color-600)';
    } else {
      backgroundColor = 'var(--danger-color-500)';
      backgroundColorWhenPointerOver = 'var(--danger-color-600)';
    }

    return css<WrapperProps>`
      border-style: none;

      color: white;
      background-color: ${backgroundColor};

      ${props =>
        props.isPointerOver &&
        css`
          background-color: ${backgroundColorWhenPointerOver};
        `}

      ${props =>
        props.isPointerDown &&
        css`
          box-shadow: none;
          transform: scale(0.95);
        `}
    `;
  }

  return null;
}