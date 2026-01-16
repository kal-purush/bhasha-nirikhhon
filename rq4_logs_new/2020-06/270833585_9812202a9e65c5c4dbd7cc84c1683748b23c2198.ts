import { animated } from 'react-spring';

import styled, { keyframes, css } from 'styled-components';

interface ContainerProps {
  hasDescription: boolean;
  load: boolean;
}

const loadingTypeVariantions = {
  info: css`
    background: rgba(0, 0, 0, 0.2);
    color: #fff;
  `,
};

const rotate360 = keyframes`

to{
  transform: rotate(1turn);
}
`;

export const Container = styled(animated.div)<ContainerProps>`
  width: 360px;
  position: relative;
  padding: 16px 30px 16px 16px;
  border-radius: 10px;
  box-shadow: 2px 2px 8px rgba(0, 0, 0, 0.2);

  display: flex;

  ${() => loadingTypeVariantions.info}

  div {
    flex: 1;
    > article {
      margin: 4px 12px 0 30px;
      transform: scale(5);
      width: 20%;
      min-height: 20vh;

      flex-direction: column;
      align-items: center;
      display: flex;
      justify-content: center;
      height: auto;

      span {
        animation: ${rotate360} 1s linear infinite;
        border: 2.2px solid #e5e5e5;
        border-radius: 50%;
        border-top-color: #0066f9;
        height: 20px;
        width: 20px;
      }
    }
    p {
      margin-top: 4px;
      font-size: 14px;
      opacity: 0.8;
      line-height: 20px;

      margin: 4px 12px 20px 30px;
    }
  }

  ${(props) =>
    !props.hasDescription &&
    css`
      align-items: center;
      svg {
        margin-top: 0;
      }
    `}
`;