import { styled } from "styled-components"

export const CardContainer = styled.li`
  display: flex;
  flex-direction: column;

  width: 250px;

  box-shadow: rgba(100, 100, 111, 0.2) 0px 7px 29px 0px;

  background: var(--white);

  border-radius: 12px;

  flex-shrink: 0;

  @media (min-width: 768px) {
    width: 285px;
  }
`

export const FigureContainer = styled.figure`
  > img {
    width: 100%;
    border-radius: 12px 12px 0 0;
  }
`

export const ContainerInfo = styled.div`
  display: flex;
  flex-direction: column;
  gap: .4rem;

  padding: 1rem;

  > h3 {
    font-size: .8rem;
    color: var(--black);
  }

  > p {
    font-size: .7rem;
    color: var(--gray);
  }

  @media (min-width: 768px) {
    gap: .7rem;

    > h3 {
      font-size: 1rem;
    }

    > p {
      font-size: .8rem;
    }
  }
`

export const ContactUserContainer = styled.div`
  display: flex;
  align-items: center;
  gap: .4rem;

  :nth-child(1) {
    border-radius: 50%;

    background-color: var(--blue);

    padding: .4rem .7rem;

    font-size: .7rem;

    color: var(--white);
  }

  :nth-child(2) {
    font-size: .7rem;
    color: var(--gray);
  }

  @media (min-width: 768px) {
    :nth-child(1), :nth-child(2) {
      font-size: .83rem;
    }
  }
`

export const ContainerInfoCar = styled.div`
  display: flex;
  justify-content: space-between;
  align-items: center;

  > div {
    display: flex;
    gap: .4rem;

    > span {
      background: var(--primary-color);

      border-radius: 6px;

      opacity: .7;
      
      padding: .4rem;
      font-size: .7rem;
      color: var(--white);

      @media (min-width: 768px) {
        font-size: .8rem;
      }
    }
  }

  > span {
    font-size: .9rem;
    font-weight: 700;

    color: var(--gray);
  }
`