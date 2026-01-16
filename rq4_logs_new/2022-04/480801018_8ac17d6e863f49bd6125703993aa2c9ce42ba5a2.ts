import styled, { css } from "styled-components";
import { spacing } from "../constants/spacingSize";

export const StyledAccordion = styled.div<{ isMdxMode?: boolean }>`
  padding: ${({ isMdxMode }) => (isMdxMode ? 0 : `${spacing.s40}px 0`)};
  border-bottom: 1px solid #3d4168;
  color: #ffffff;
  font-family: inherit;
`;

export const AccordionBodyWrapper = styled.div<{
  height: string;
  overflow: string;
}>`
  height: ${({ height }) => `${height}px`};
  overflow-y: ${({ overflow }) => `${overflow}`};
  transition: height 1s ease;
`;

export const AccordionTitleWrapper = styled.div`
  display: flex;
  align-items: center;
  gap: 16px;
`;

export const InnerContentWrapper = styled.div<{ isMdxMode?: boolean }>`
  ${({ isMdxMode = true }) =>
    isMdxMode
      ? css`
          padding-bottom: ${spacing.s50}px;
          > *:last-child {
            margin-bottom: 0;
          }
        `
      : ""}
`;