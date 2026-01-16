import styled from 'styled-components'

export const Container = styled.div`
  width: 100vw;
  height: 100vh;
  overflow: hidden;
`

export const Main = styled.div`
  width: 100%;
  display: grid;
  grid-template-columns: 55% 45%;
  grid-template-areas: 'background login';
`

export const BackgroundContainer = styled.div`
  background-color: var(--color-primary);
  grid-area: 'background';
  display: flex;
  align-items: center;
  justify-content: center;
  height: 100vh;
`

export const BackgroundImage = styled.img`
  width: 90%;
`

export const LoginContainer = styled.div`
  grid-area: 'login';
  display: grid;

  align-content: center;
  justify-items: center;
  height: 100vh;
`

export const SignIn = styled.div`
  width: 60%;
`

export const Legend = styled.div`
  font-family: 'Poppins';
  font-size: 3.2rem;
  margin-bottom: 3.2rem;
`

export const ButtonContainer = styled.div`
  button {
    width: 100%;
    height: 5.6rem;
    background: var(--color-secondary);
    color: var(--color-button-text);
    border: none;
    border-radius: 0.8rem;
    cursor: pointer;
    font: 700 1.6rem 'Archivo';
    display: flex;
    align-items: center;
    justify-content: center;
    text-decoration: none;
    transition: background-color 0.2s;
    margin-top: 3.2rem;

    &:hover {
      background-color: var(--color-secondary-dark);
    }

    &:focus {
      outline: 0;
    }
  }
`
export const AccountContainer = styled.div`
  display: flex;
  align-items: center;
  justify-content: space-between;
  width: 100%;
  margin: 2.8rem 0 4.4rem;

  a {
    font-family: 'Poppins';
    color: var(--color-text-complement);
    text-decoration: none;

    &:hover {
      color: var(--color-primary);
      text-decoration: underline var(--color-primary);
    }
  }
`

export const RememberAccount = styled.input`
  cursor: pointer;
  margin-right: 1.2rem;
`

export const Footer = styled.div`
  margin-top: 6.4rem;
  display: flex;
  align-items: center;
  justify-content: center;
  flex-direction: column;

  p {
    font-family: 'Poppins';
    color: var(--color-text-base);
    font-size: 1.6rem;
  }

  a {
    margin-top: 0.4rem;
    font-family: 'Poppins';
    color: var(--color-primary);
    text-decoration: none;
    font-weight: 600;

    &:hover {
      text-decoration: underline var(--color-primary);
    }
  }
`