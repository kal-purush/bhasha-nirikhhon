import React from "react";
import styled from "@emotion/styled";
import { Section } from "components/common";
import { wrapContext } from "components/common";

export const Logo = wrapContext(styled.img`
  grid-row: 1/6;
  grid-column: 1/2;
  height: calc(4vw + 30px);
  padding: ${({ isMobile }) =>
    isMobile ? "10px 0 10px 0" : " 10px 0 10px 20%"};
  justify-self: ${({ isMobile }) => (isMobile ? "center" : "start")};
`);

export const HeaderSection = styled(Section)`
  position: relative;
  background: white;
`;