import { Link } from "react-router-dom";
import { styled } from "styled-components";

export const MainContainerLogin = styled.main`
  display: flex;
  flex-direction: column;
  padding: 1rem;

  @media (min-width: 768px) {
    margin-top: 7rem;
    align-items: center;

    padding-bottom: 4rem;
  }
`

export const FormLoginContainer = styled.form`
  display: flex;
  flex-direction: column;
  gap: 1rem;

  border-radius: 8px;

  padding: 1.4rem;

  box-shadow: rgba(0, 0, 0, 0.16) 0px 1px 4px;
  background: var(--white);

  @media (min-width: 768px) {
    width: 412px;
  }
`

export const TitleLogin = styled.div`
  > h3 {
    color: var(--gray);
  }
`

export const FieldsetLogin = styled.fieldset`
  display: flex;
  flex-direction: column;
  gap: .4rem;

  border: none;

  > label {
    color: var(--gray);
    font-size: .8rem;
  }

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

export const ForgotMyPassword = styled.div`
  display: flex;
  align-items: flex-end;

  > span {
    font-size: .7rem;
    font-weight: 600;
    color: var(--gray);

    align-self: flex-end;

    cursor: pointer;
  }
`

export const ButtonContainer = styled.div`
  display: flex;
  flex-direction: column;
  align-items: center;
  justify-content: center;
  gap: 1rem;

  > button {
    background: var(--primary-color);

    padding: .8rem;

    width: 100%;

    color: var(--white);
    font-size: .9rem;
    font-weight: 600;

    &:hover {
      background: var(--black);
      color: var(--white);
    }
  }
`

export const ButtonToRegister = styled(Link)`
  color: var(--gray);
  font-size: .8rem;
  font-weight: 600;

  cursor: pointer;
`