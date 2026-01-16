import { inube } from "@inubekit/inubekit";
import { tokens } from "@src/design/tokens";
import styled from "styled-components";
interface IStyledContent {
  $options?: number;
}

const StyledContent = styled.div<IStyledContent>`
  display: grid;
  grid-template-columns: 1fr auto;
  width: 190px;
  gap: ${tokens.spacing.s075};
  background-color: ${({ theme }) =>
    theme?.palette?.neutral.N0 ?? inube.palette.neutral.N0};
  box-sizing: border-box;
  border: 1px solid ${tokens.spacing.s100};
  border-radius: ${tokens.spacing.s100};
  box-shadow: 0px 2px 6px 2px rgba(0, 0, 0, 0.1);
  top: 25px;
  right: 10px;
  z-index: 1;
  position: absolute;
`;

export { StyledContent };