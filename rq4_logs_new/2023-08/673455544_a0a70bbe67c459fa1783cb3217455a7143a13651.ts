import { styled } from 'styled-components'

export const FooterContainer = styled.footer`
  display: flex;
  flex-direction: column;
  align-items: center;
  justify-content: center;
  gap: .7rem;

  padding: 2rem;

  background: var(--black);

  > h3, p {
    color: var(--white);
  }

  > h3 {
    font-size: 1rem;
  }

  > p {
    font-size: .7rem;
  }

  @media (min-width: 768px) {
    gap: 2rem;

    > h3 {
      font-size: 1.4rem;
    }

    > p {
      font-size: .8rem;
    }
  }
`

export const ButtonToBeginning = styled.a`
  color: var(--white);
  
  font-size: 1.7rem;
`