import styled from "styled-components";

const Section = styled.section`
  display: flex;
  flex-direction: column;
  align-items:start;
`

const THome = styled.h1`
  display: flex;
  flex-direction: column;
`
const SubTitle = styled.span`
  font: var(--font-default3);
  letter-spacing: 2px;
  text-transform: uppercase;
  color: var(--light);
  `
const SpaceSpan = styled.span`
  font: var(--font-strong1);
  text-transform: uppercase;
  letter-spacing: 4px;
`

const P = styled.p`
  font: var(--font-strong2);
  color: var(--light);
  letter-spacing: .5px;
  max-width: 45ch;
`




export {
  Section,
  THome,
  SpaceSpan,
  SubTitle,
  P,

}