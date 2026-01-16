import { styled } from "styled-components";

export const SectionProfileInfoComponent = styled.section`
  display: flex;
  flex-direction: column;
  gap: 1rem;

  padding: 1rem;

  box-shadow: rgba(0, 0, 0, 0.1) 0px 1px 3px 0px, rgba(0, 0, 0, 0.06) 0px 1px 2px 0px;

  border-radius: 8px;

  > p {
    color: var(--gray);
    font-size: .7rem;
  }

  @media (min-width: 768px) {
    margin-top: 5rem;
    padding: 2rem;

    > p {
      font-size: .8rem;
    }
  }
`

export const UserDiv = styled.div`
  display: flex;
  gap: .4rem;

  > div {
    display: flex;
    flex-direction: column;
    gap: .4rem;

    > span:nth-child(1) {
      color: var(--black);
      font-size: .9rem;
      font-weight: 600;
    }

    > span:nth-child(2) {
      background: #EDEAFD;
      color: var(--primary-color);
      font-size: .7rem;
      padding: .4rem;
      border-radius: 6px;
    }
  }
`

export const SiglaUser = styled.span`
  display: flex;
  align-items: center;

  border-radius: 50%;

  padding: .4rem 1rem;

  background: var(--primary-color);

  color: var(--white);
  font-weight: 600;
`

export const ButtonCreate = styled.button`
  color: var(--primary-color);
  border: 1px solid var(--primary-color);

  width: fit-content;

  padding: .6rem 1rem;

  transition: .2s ease;

  &:hover {
    background: var(--black);
    border: 1px solid var(--black);

    color: var(--white);
  }

  @media (min-width: 768px) {
    font-size: 1rem;

    padding: .8rem 1.2rem;
  }
`