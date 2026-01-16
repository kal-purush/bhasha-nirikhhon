import styled from 'styled-components'
import { Description } from '../../components/Description/styles'

export const Container = styled.div`
  width: 90vw;
  max-width: 856px;
  min-height: 100vh;
  margin: auto;
`

export const SignUpForm = styled.form`
  width: 100%;
  padding: 2.4rem;
  margin: .8rem 0 2.4rem 0;

  display: flex;
  flex-direction: column;

  border: 0;
  border-radius: 1.6rem;
  background-color: var(--color-in-background);

  & > button {
    margin-top: 4.8rem;
  }

  @media (min-width: 769px) {
    & > button {
      align-self: flex-end;
    }
  }
`

export const InputGroup3x2 = styled.div`
  @media (min-width: 769px) {
    display: grid;
    grid-template-columns: 3fr 2fr;
    gap: .8rem;
  }
`

export const InputGroup2x2 = styled.div`
  & div:last-child {
    @media (max-width: 769px) {
      margin: .8rem 0;
    }
  }

  @media (min-width: 769px) {
    display: grid;
    grid-template-columns: repeat(2, 2fr);
    gap: .8rem;
  }
`

export const InputGroup2x3x2 = styled.div`
  margin: .8rem 0 0 0;

  @media (min-width: 1280px) {
    display: grid;
    grid-template-columns: 2fr 3fr 2fr;
    gap: .8rem;
    align-items: flex-start;
  }
`

export const MapContainer = styled.div`
  position: relative;
  width: 100%;
  margin: .8rem 0 1.6rem 0;
  border: .4rem solid var(--color-black-200);
  border-radius: .8rem;
  overflow: hidden;
  transition: all var(--time-transition-input);

  &:focus-within {
    border-color: var(--color-primary);
  }
`

export const MapDescription = styled.span`
  position: absolute;
  bottom: 0;
  z-index: 5;
  width: 100%;
  padding: 1.6rem .8rem;
  display: flex;
  align-items: center;
  justify-content: center;
  background-color: rgba(255, 255, 255, 0.6);
`

export const MapDescriptionText = styled(Description)`
  text-align: center;
  font-weight: 500;
  color: var(--color-primary);
`