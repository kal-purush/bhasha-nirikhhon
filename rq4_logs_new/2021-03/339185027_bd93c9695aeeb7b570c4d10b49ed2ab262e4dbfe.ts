import { styled, theme } from '../../../../../theme';
import {
  MovieHoverPanelProps,
  MovieHoverItemsProps,
} from './MovieHover.interface';

export const MovieHoverPanel = styled.div<MovieHoverPanelProps>`
  position: absolute;
  top: 20px;
  right: 20px;
  z-index: 1;
  display: ${({ showIcon }) => (showIcon ? 'flex' : 'none')};
  align-items: center;
  justify-content: center;
  height: 30px;
  width: 30px;
  background-color: ${theme.colors.black};
  border-radius: 50%;

  &:hover {
    cursor: pointer;
  }

  & > svg {
    fill: ${theme.colors.white};
    height: 60%;
  }
`;

export const MovieHoverItems = styled.ul<MovieHoverItemsProps>`
  position: absolute;
  width: 190px;
  background-color: ${theme.colors.black};
  list-style: none;
  padding: 0;
  right: 10px;
  z-index: 1;
`;

export const MovieHoverItem = styled.li`
  padding: 15px;
  color: ${theme.colors.white};
  font-size: 14px;

  &:hover {
    cursor: pointer;
    background-color: ${theme.colors.pink};
  }
`;

export const CrossButton = styled.button`
  position: relative;
  padding: 7px;
  left: 152px;
  top: 3px;
  background-color: transparent;
  border: 0;
  outline: 0;

  &:hover {
    cursor: pointer;

    & > div > svg {
      fill: ${theme.colors.pink};
    }
  }

  & > div {
    & > svg {
      width: 12px;
      top: 10px;
    }
  }
`;