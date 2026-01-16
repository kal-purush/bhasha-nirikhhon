import { styled } from "styled-components"

export const BackgroundModalEditProfile = styled.main`
  display: flex;
  justify-content: center;
  align-items: center;
  width: 100vw;
  height: 100vh;
  
  position: fixed;
  top: 0;
  right: 0;
  
  background-color: rgba(0, 0, 0, 0.7);

  z-index: 500;

  backdrop-filter: blur(2px);
  `

export const FormContainer = styled.form`
  display: flex;
  flex-direction: column;
  gap: .7rem;

  background: var(--white);

  padding: 1rem 2rem;
  height: 570px;

  border-radius: 8px;

  overflow-y: auto;

  @media (min-width: 768px) {
    width: 520px;
  }
`

export const TitleContainer = styled.div`
  display: flex;
  justify-content: space-between;

  > button {
    cursor: pointer;
  }
`

export const FieldsetContainer = styled.fieldset`
  display: flex;
  flex-direction: column;
  gap: .4rem;

  border: none;

  > label {
    color: var(--gray);
    font-size: .8rem;
  }

    /* > textarea {
        padding: .7rem 1rem;
        border-radius: 8px;
        border: 2px solid var(--white);
        outline: none;
        color: var(--gray);
        transition: .2s ease;

        &:focus {
            border: 2px solid var(--primary-color);
        }

        &::placeholder {
            color: var(--light-gray);
        }
    } */

  > input {
      padding: .7rem 1rem;
      border-radius: 8px;
      border: 2px solid var(--white);
      outline: none;
      color: var(--gray);
      transition: .2s ease;

    &:focus {
        border: 2px solid var(--primary-color);
    }

    &::placeholder {
        color: var(--light-gray);
    }
  }
`

export const ButtonContainer = styled.div`
  display: flex;
  justify-content: space-between;

  > button {
    padding: .7rem 1.7rem;
    transition: .1s ease;
  }

  :nth-child(1) {
    background: var(--light-gray);
    color: var(--gray);

    &:hover {
      background: var(--gray);
      color: var(--light-gray);
    }
  }

  :nth-child(2) {
    background: var(--red);
    color: var(--alert-negative);

    &:hover {
      background: var(--alert-negative);
      color: var(--red);
    }
  }

  :nth-child(3) {
    background: var(--light-purple);
    color: var(--primary-color);

    &:hover {
      background: var(--primary-color);
      color: var(--light-purple);
    }
  }
`