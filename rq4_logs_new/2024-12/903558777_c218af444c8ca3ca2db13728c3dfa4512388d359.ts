import styled from "styled-components";

export const SummaryContainer = styled.section`
  display: grid;
  grid-template-columns: repeat(3, 1fr);
  gap: 2rem;
  margin: 0 auto;
  width: 100%;
  max-width: 1120px;
  margin-top: -5rem;
  padding: 0 1.5rem;
`

export const SummaryContentContainer = styled.div`
  display: flex;
  flex-direction: column;
  gap: 0.75rem;
  padding: 1.5rem;
  padding-left: 2rem;
  border-radius: 6px;
  background-color: ${props => props.theme['gray-600']};
`

export const GreenSummaryContentContainer = styled(SummaryContentContainer)`
  background-color: ${props => props.theme['green-500']};
`

export const SummaryTitleAndIconContainer = styled.header`
  display: flex;
  justify-content: space-between;

  span {
    font: 400 1rem Roboto, sans-serif;
    color: ${props => props.theme['gray-300']};
    line-height: 1.6;
  }
`

export const SummaryContentValue = styled.strong`
  font: 700 2rem Roboto, sans-serif;
  color: ${props => props.theme['gray-100']};
  line-height: 1.4;
`