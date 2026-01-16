import styled from "styled-components";

export const HeadingLarge = styled.h1`
  color: ${(props) => props.theme.colors.veryLightGrey};
  font-size: clamp(12rem, 15vw, 20rem);
  font-weight: 700;
  line-height: 20rem;
  letter-spacing: -5px;
`;

export const HeadingSmall = styled.h1`
  font-size: clamp(4.8rem, 11vw, 8rem);
  font-weight: 700;
  line-height: 8rem;
  letter-spacing: -2px;
`;

export const H2 = styled.h2`
  font-size: clamp(4rem, 7.5vw, 5.6rem);
  font-weight: 700;
  line-height: clamp(4.8rem, 7.5vw, 5.6rem);
  letter-spacing: -1.43px;
  @media ${(props) => props.theme.breakpoints.xs} {
    letter-spacing: -2px;
  }
`;

export const H3 = styled.h3`
  font-size: 1.8rem;
  font-weight: 700;
  line-height: 2.5rem;
  letter-spacing: 0px;
`;