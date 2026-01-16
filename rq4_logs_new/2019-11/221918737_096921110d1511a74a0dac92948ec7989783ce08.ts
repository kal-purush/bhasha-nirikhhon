import styled, { css } from 'styled-components';

interface Props {
  readonly variant: string;
  disabled?: boolean;
  children?: React.ReactNode;
}

export const TableActionButton = styled.button<Props>`
  height: 70px;
  width: 56px;
  color: white;
  ${p =>
    p.variant === 'primary' &&
    css`
      border: 2px solid ${p.theme.colors.skyBlue};
      background-color: ${p.theme.colors.skyBlue};
      outline: none;
      &:hover {
        background-color: ${p.theme.colors.lightSkyBlue};
        border: 2px solid ${p.theme.colors.lightSkyBlue};
      }
      &:focus {
        background-color: ${p.theme.colors.lightSkyBlue};
        border: 2px solid ${p.theme.colors.darkSkyBlue};
      }
      &:active {
        background-color: ${p.theme.colors.darkSkyBlue};
      }
      &:disabled {
        background-color: ${p.theme.colors.gray3};
        border: 2px solid ${p.theme.colors.gray3};
      }
    `};
` as React.FunctionComponent<Props>;