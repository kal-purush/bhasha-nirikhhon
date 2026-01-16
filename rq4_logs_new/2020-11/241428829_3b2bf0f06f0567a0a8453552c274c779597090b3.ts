import styled from 'styled-components'

export const LinkWithIcon = styled.button`
  height: 5.6rem;
  padding: 0 1.6rem;
  display: flex;
  align-items: center;
  justify-content: center;
  border: 0;
  font: 700 1.6rem Montserrat;
  color: var(--color-primary);
  cursor: pointer;
  transition: opacity var(--time-transition-button);

  &:hover {
    opacity: var(--opacity-800);
  }

  &:disabled {
    cursor: not-allowed;
    opacity: var(--opacity-400);
  }

  & svg {
    margin-right: .8rem;
  }
`