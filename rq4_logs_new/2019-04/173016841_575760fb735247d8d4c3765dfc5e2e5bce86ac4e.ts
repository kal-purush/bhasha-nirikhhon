import styled from '../../custom/styled-components';
import TextArea from '../../components/TextArea';
import { mobileBreakpoint } from '../../styles/variables';

export const Body = styled.div`
  display: flex;
  align-items: center;
  flex-direction: column;

  & > * + * {
    margin-top: 0.75em;
  }
`;

export const ThingTextArea = styled(TextArea)`
  font-size: 1rem;
  width: 80%;

  @media screen and (min-width: ${mobileBreakpoint}) {
    font-size: 1.25rem;
  }
`;

export const Danger = styled.span`
  font-weight: bold;
  color: ${props => props.theme.danger};
`;