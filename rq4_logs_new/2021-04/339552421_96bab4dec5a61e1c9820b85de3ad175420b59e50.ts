import styled from 'styled-components'
import { border, breakPoints } from '../constants'
import device from '../constants/breakPoints'

export const Container = styled.div`
  padding: 3.6rem 1.6rem;

  @media ${breakPoints.tablet} {
    padding: 5.6rem 1.2rem;
  }

  overflow-x: hidden;
`

export const Title = styled.div`
  display: flex;
  align-items: center;
  margin-bottom: 2.4rem;
  max-width: 120rem;
  margin: 0 auto 2.4rem;

  div {
    content: '';
    width: 1.6rem;
    height: 1.6rem;

    border: 4px solid ${({ theme }) => theme.colors.orangePrimary};
    border-radius: 100vmax;

    margin-right: 1.6rem;
  }

  h1 {
    font-size: 2.4rem;
  }

  @media ${breakPoints.tablet} {
    div {
      width: 2.4rem;
      height: 2.4rem;

      border: 8px solid ${({ theme }) => theme.colors.orangePrimary};
    }

    h1 {
      font-size: 4rem;
    }
  }
`

export const CheckoutTable = styled.div`
  max-width: 120rem;
  margin: 0 auto;
  display: flex;
  flex-wrap: wrap;
  flex-direction: column;
  overflow-x: auto;
  -webkit-overflow-scrolling: touch;
  &::-webkit-scrollbar {
    display: none;
  }
`

export const CheckoutDetails = styled.div`
  height: 100%;

  display: flex;
  align-items: center;

  border-bottom: 1px solid ${({ theme }) => theme.colors.orangePrimary};

  aside {
    min-width: 53.2rem;
    display: flex;
    align-items: center;

    img {
      max-height: 8.8rem;
      max-width: 15.4rem;
      width: 100%;
    }

    h4 {
      white-space: nowrap;
      margin-left: 2.4rem;
      font-weight: lighter;
      font-size: 1.6rem;
    }
  }

  a {
    font-weight: lighter;
    font-size: 1.6rem;
    color: ${({ theme }) => theme.colors.error};
  }

  h3 {
    font-weight: 400;
    font-size: 2.4rem;
    color: ${({ theme }) => theme.colors.gray};
  }

  h3,
  a,
  section {
    display: flex;
    justify-content: center;
    margin: 0 auto;
  }

  @media ${breakPoints.tablet} {
    h3,
    a,
    section {
      width: 100%;
    }
  }
`

export const CheckoutColumns = styled.section`
  display: flex;
  align-items: center;

  background-color: ${({ theme }) => theme.colors.white};

  strong {
    text-align: center;
    font-size: 1.6rem;
    font-weight: bold;
    width: 100%;

    padding: 0.8rem 5.6rem;

    background-color: ${({ theme }) => theme.colors.orangePrimary};
    color: ${({ theme }) => theme.colors.white};
  }

  strong + strong {
    border-left: 1px solid ${({ theme }) => theme.colors.yellowPrimary};
  }

  strong:first-child {
    min-width: 53.2rem;
  }
`
export const SummaryOrder = styled.div`
  width: 100%;
  max-width: 120rem;
  margin: 5.6rem auto;

  display: flex;
  flex-direction: column;
`

export const SummaryTitle = styled.div`
  width: 100%;
  text-align: center;
  height: 3.2rem;
  background: ${({ theme }) => theme.colors.orangePrimary};

  h2 {
    color: ${({ theme }) => theme.colors.white};
    font-size: 1.6rem;
  }
  display: flex;
  justify-content: center;
  align-items: center;
`

export const SummaryContent = styled.div`
  display: flex;
  flex-direction: column;

  @media ${device.tablet} {
    flex-direction: row;
    justify-content: space-between;
  }
`

export const SummaryDelivery = styled.div`
  margin-top: 3.2rem;
  display: flex;
  flex-direction: column;

  section {
    display: flex;

    justify-content: space-between;
    align-items: center;

    strong {
      color: ${({ theme }) => theme.colors.black};
      font-size: 2.4rem;
      display: flex;
      align-items: center;
      svg {
        margin-right: 1.6rem;
        font-size: 2.4rem;
        color: ${({ theme }) => theme.colors.gray};
      }
    }

    p {
      font-size: 1.6rem;
      color: ${({ theme }) => theme.colors.gray};
    }
    span {
      font-size: 2.4rem;
      color: ${({ theme }) => theme.colors.black};

      display: flex;
      flex-direction: column;

      button {
        border: 0;
        background: transparent;
        cursor: pointer;
        color: ${({ theme }) => theme.colors.redPrimary};
        margin-top: 0.4rem;
      }
    }

    & + section {
      margin-top: 3.2rem;
    }
  }

  @media ${device.tablet} {
    width: 28%;
  }
`

export const SummaryInfo = styled.div`
  margin-top: 3.2rem;
  display: flex;
  flex-direction: column;
  /* align-items: center; */

  section {
    display: flex;
    justify-content: space-between;

    strong {
      color: ${({ theme }) => theme.colors.black};
      font-size: 2.4rem;
      display: flex;
      align-items: center;
    }

    span {
      color: ${({ theme }) => theme.colors.black};
      font-size: 2.4rem;
      display: flex;
      align-items: center;
    }

    & + section {
      margin-top: 3.2rem;
    }
  }
  @media ${device.tablet} {
    width: 28%;
  }
`

export const SummaryButtons = styled.div`
  margin-top: 3.2rem;
  display: flex;
  flex-direction: column;
  justify-content: space-between;
  align-items: center;

  button {
    width: 100%;
    display: flex;
    justify-content: space-between;
    align-items: center;

    height: 4.8rem;
    padding: 1.6rem;

    color: ${({ theme }) => theme.colors.white};
    background: ${({ theme }) => theme.colors.orangePrimary};

    border: 0;
    border-radius: ${border.borderRadius};

    font-size: 1.6rem;
    & + button {
      background: ${({ theme }) => theme.colors.greenPrimary};
      margin-top: 1.6rem;
    }
  }

  @media ${device.tablet} {
    width: 28%;
  }
`