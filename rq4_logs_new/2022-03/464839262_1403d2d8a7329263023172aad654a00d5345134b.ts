import styled from 'styled-components';

export const OverviewCardsContainer = styled.div`
  position: relative;
  width: 100%;
  margin-top: 2.5rem;
`;

export const CardContainer = styled.div`
  position: relative;
  width: 100%;
  margin-top: 1.25rem;

  display: grid;
  grid-template-columns: repeat(4, 1fr);
  gap: 1.75rem;

  @media (max-width: ${(props) => props.theme.breakpoints.smallDesktop}px) {
    grid-template-columns: repeat(2, 1fr);
  }

  @media (max-width: ${(props) => props.theme.breakpoints.mobile}px) {
    grid-template-columns: 1fr;
  }
`;