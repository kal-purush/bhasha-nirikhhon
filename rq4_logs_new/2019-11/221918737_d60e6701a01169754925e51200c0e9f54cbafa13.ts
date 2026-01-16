import styled, { css } from 'styled-components';

interface OptionProps {
  variant?: string;
  disabled?: boolean;
}

export const ListOption = styled.div<OptionProps>`
  text-align: center;
  font-weight: bold;
  height: 36px;
  font-size: 12px;
  line-height: 16px;
  display: flex;
  justify-content: center;
  align-items: center;
  padding: 5px 10px;

  &:first-child {
    border-radius: ${p => p.theme.borderRadius.small} ${p => p.theme.borderRadius.small} 0 0;
  }

  &:last-child {
    border-radius: 0 0 ${p => p.theme.borderRadius.small} ${p => p.theme.borderRadius.small};
  }

  &:only-child {
    border-radius: ${p => p.theme.borderRadius.small};
  }

  &:hover {
    background-color: ${p => p.theme.colors.aliases.primaryLight};
    color: ${p => p.theme.colors.white};
  }

  &:active {
    background-color: ${p => p.theme.colors.aliases.primaryDark};
  }

  &:not(:last-child) {
    border-bottom: 1px solid ${p => p.theme.colors.gray3};
  }

  ${p =>
    p.disabled &&
    css`
      &:hover {
        background-color: ${p.theme.colors.gray3};
      }
    `};
`;

export const PopoverList = styled.div`
  box-shadow: ${p => p.theme.boxShadow.standard};
  background-color: ${p => p.theme.colors.white};
  position: relative;
  display: inline-block;
  border-radius: 5px;
  &:before,
  &:after {
    content: '';
    border-left: 8px solid transparent;
    border-right: 8px solid transparent;
    position: absolute;
    right: 13px;
    top: 0;
  }
  &:after {
    border-bottom: 8px solid ${p => p.theme.colors.white};
    top: -8px;
    bottom: auto;
  }
`;