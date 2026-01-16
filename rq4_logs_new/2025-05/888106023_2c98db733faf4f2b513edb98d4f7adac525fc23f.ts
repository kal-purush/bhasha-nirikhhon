import { inube } from "@inubekit/inubekit";
import styled from "styled-components";

const StyledFloatingButton = styled.div`
  position: fixed;
  bottom: 130px;
  right: 36px;
  z-index: 0.5;
  cursor: pointer;
`;

const StyledCircleWithCross = styled.div`
  width: 50px;
  height: 50px;
  background: ${({ theme }) =>
    theme?.palette.blue.B400 ?? inube.palette.blue.B400};
  border: ${({ theme }) =>
    `1px solid ${theme?.palette.blue.B400 ?? inube.palette.blue.B400}`};
  border-radius: 50%;
  position: relative;
  display: flex;
  align-items: center;
  justify-content: center;

  &::before,
  &::after {
    content: "";
    position: absolute;
    width: 20px;
    height: 3px;
    background: ${({ theme }) =>
      theme?.palette.neutral.N0 ?? inube.palette.neutral.N0};
  }

  &::before {
    transform: rotate(90deg);
  }

  &::after {
    transform: rotate(180deg);
  }
`;

export { StyledFloatingButton, StyledCircleWithCross };