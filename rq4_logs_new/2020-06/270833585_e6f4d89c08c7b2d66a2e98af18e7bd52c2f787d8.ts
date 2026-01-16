import styled, { css } from 'styled-components';

import { colors } from '../../styles';
import Tooltip from '../Tooltip';

interface ContainerProps {
  isFocused: boolean;
  isFilled: boolean;
  isErrored: boolean;
}

export const Container = styled.div<ContainerProps>`
  background: #232129;
  border-radius: 10px;
  border: 2px solid #232129;
  padding: 16px;
  width: 100%;
  color: #666360;

  display: flex;
  align-items: center;

  & + div {
    margin-top: 8px;
  }

  ${(props) =>
    props.isErrored &&
    css`
      border-color: ${colors.fourth};
    `}

  ${(props) =>
    props.isFocused &&
    css`
      color: ${colors.secundary};
      border-color: ${colors.secundary};
    `}

  ${(props) =>
    props.isFilled &&
    css`
      color: ${colors.secundary};
    `}


  input {
    flex: 1;
    border: 0;
    color: ${colors.primary};
    -webkit-text-fill-color: ${colors.primary};

    transition: background-color 5000s ease-in-out 0s;

    background: transparent;
    &::placeholder {
      color: ${colors.third};
    }
  }

  svg {
    margin-right: 16px;
  }
`;

export const Error = styled(Tooltip)`
  height: 20px;
  margin-left: 16px;

  svg {
    margin: 0;
  }

  span {
    background: ${colors.fourth};
    color: ${colors.sixth};
    &::before {
      border-color: ${colors.fourth} transparent;
    }
  }
`;