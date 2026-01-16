import styled from "styled-components";

export const CarStatusField = styled.div`
  display: flex;
  flex-direction: row;
  align-items: center;
  justify-content: space-around;
  gap: 0.5rem;

  > input {
    display: none;
  }

  > label {
    background: var(--primary-color);
    padding: 0.8rem;
    width: 100%;
    color: var(--white);
    font-size: 0.9rem;
    font-weight: 600;
    transition: 0.2s ease;
    border-radius: 8px;
    cursor: pointer;

    &:hover {
      background: var(--gray);
      color: var(--white);
    }
  }
`

export const UpdateButtonsContainer = styled.div`
    display: flex;
    flex-direction: row;
    align-items: center;
    justify-content: center;
    gap: .3rem;

    > button {
        background: var(--primary-color);
        padding: .8rem;
        width: 55%;
        color: var(--white);
        font-size: .9rem;
        font-weight: 600;
        transition: .2s ease;

        &:hover {
            background: var(--black);
            color: var(--white);
        }
    }

    > .cancel {
        background-color: var(--light-gray);
        width: 100%;
        height: 100%;

        &:hover{
            background-color: var(--gray);
        }
    }
`