import { styled } from "styled-components";

export const CardContainer = styled.li`
  display: flex;
  flex-direction: column;

  width: 250px;

  box-shadow: rgba(100, 100, 111, 0.2) 0px 7px 29px 0px;

  background: var(--white);

  border-radius: 12px;

  flex-shrink: 0;

  position: relative;

  @media (min-width: 768px) {
    width: 282px;
    height: fit-content;
    min-height: 390px;
    max-height: fit-content;
  }
`;

export const FigureContainer = styled.figure`
  > img {
    width: 100%;
    height: 152px;

    object-fit: cover;

    border-radius: 12px 12px 0 0;
  }
`;

export const ContainerInfo = styled.div`
  display: flex;
  flex-direction: column;
  gap: 0.4rem;

  padding: 1rem;

  > h3 {
    font-size: 0.8rem;
    color: var(--black);
  }

  > p {
    font-size: 0.7rem;
    color: var(--gray);
    height: 90px;
    line-height: 150%;
  }

  @media (min-width: 768px) {
    gap: 0.7rem;

    > h3 {
      font-size: 1rem;
    }

    > p {
      font-size: 0.8rem;
    }
  }
`;

export const ContactUserContainer = styled.div`
  display: flex;
  align-items: center;
  gap: 0.4rem;

  :nth-child(1) {
    border-radius: 50%;

    background-color: var(--blue);

    padding: 0.4rem 0.7rem;

    font-size: 0.7rem;

    color: var(--white);
  }

  :nth-child(2) {
    font-size: 0.7rem;
    color: var(--gray);
  }

  @media (min-width: 768px) {
    :nth-child(1),
    :nth-child(2) {
      font-size: 0.83rem;
    }
  }
`;

export const ContainerInfoCar = styled.div`
  display: flex;
  justify-content: space-between;
  align-items: center;

  > div {
    display: flex;
    gap: 0.4rem;

    > span {
      background: var(--primary-color);

      border-radius: 6px;

      opacity: 0.7;

      padding: 0.4rem;
      font-size: 0.7rem;
      color: var(--white);
      display: flex;
      align-items: center;

      @media (min-width: 768px) {
        font-size: 0.8rem;
      }
    }
  }

  > span {
    font-size: 0.9rem;
    font-weight: 700;

    color: var(--gray);
  }
`;

export const FlagGoodDeal = styled.div`
  display: flex;
  align-items: center;

  position: absolute;
  top: -10px;
  right: -10px;

  border-radius: 50%;

  box-shadow: rgba(0, 0, 0, 0.05) 0px 0px 0px 1px;

  padding: .4rem;

  background: var(--alert-success);

  color: var(--white);
  font-size: 1.2rem;
`

export const ButtonContainer = styled.div`
  display: flex;
  justify-content: space-between;
  gap: 1rem;

  margin-top: .7rem;

  > button {
    background: var(--white);
    
    padding: .4rem;

    border: 1px solid var(--light-gray);
    border-radius: 6px;

    transition: .2s ease;

    font-weight: 600;
    color: var(--gray);

    &:hover {
      background: var(--black);
      color: var(--white);
      border: 1px solid var(--black);
    }
  }
`