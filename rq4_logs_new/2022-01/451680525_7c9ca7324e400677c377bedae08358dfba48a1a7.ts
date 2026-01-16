import styled from 'styled-components'

export const Container = styled.div`
  background-color: #000;
  color: #fff;
  font-family: 'Rock Salt', cursive;
  padding: 10px;
  box-sizing: border-box;
`

export const Title = styled.div`
  text-align: center;
  font-size: 30px;
  width: 100vw;
  margin: 3.3rem 0px;
`

export const HeaderContainer = styled.div`
  display: flex;
  justify-content: space-between;
  align-items: center;
`

export const MyOrder = styled.div`
  display: flex;
  flex-direction: column;
  align-items: flex-end;
  a {
    font-family: Roboto;
    font-style: normal;
    font-weight: normal;
    font-size: 14px;
    line-height: 16px;
    margin-top: 10px;
    text-decoration: none;
    color: #fff;
  }
  a:hover {
    color: red;
  }

  span {
    font-family: Roboto;
    margin-right: 1rem;
  }
`

export const UserContainer = styled.div`
  svg {
    margin: 0px 10px;
    width: 30px;
    height: 30px;
    color: #fff;
  }
  display: flex;
  flex-wrap: wrap;
  justify-content: space-between;
`

export const IconsH = styled.div`
  display: flex;
  align-items: center;
`

export const ProductDetails = styled.div`
  text-align: center;
  display: flex;
  flex-direction: column;
  @media (min-width: 768px) {
    margin-left: 1rem;
  }
}`

export const Quadrado = styled.div`
  text-align: center;
  width: 200px;
  height: 250px;
  background: white;
  margin: 2rem 0;
  align-self: center;
  background-color: #13141E;
  border-radius: 20px;
}`

export const SubTitle = styled.span`
}`

export const OrderPrice = styled.span`
  font-family: Ubuntu;
  font-size: 14px;
  margin-top: 1.3rem;
}`

export const ReviewDetails = styled.div`
  display: flex;
  flex-direction: column;
  font-family: Ubuntu;
}`

export const FormBox = styled.div`
  display: flex;
  width: 90vw;
  justify-content: center;

  button {
    background: #4cde18;
  }

  @media (min-width: 768px) {
    order: 1;
    display: flex;
    width: 800px;
    justify-content: center;
    margin-bottom: 40px;
  }

  @media (min-width: 1100px) {
    button {
    }
  }
`

export const Form = styled.form`
  display: flex;
  flex-direction: column;
  padding: 20px 19px 26px 19px;
  box-sizing: border-box;
  margin-top: 20px;
  width: 90%;
  background: #000000;
  @media (min-width: 768px) {
  }
`

export const ButtonReview = styled.div`
  display: flex;
  width: 100%;
  justify-content: flex-end;
  @media (min-width: 768px) {
    margin-top: 10px;
    display: flex;
    width: 100%;
  }
`

export const ButtonReviewSize = styled.div`
  height: 2rem;
  width: 100%;
  margin-top: 1rem;
  @media (min-width: 768px) {
    flex-flow: row nowrap;
    width: 40%;
  }
`

export const Page = styled.div`
  display: flex;
  flex-flow: column nowrap;
  @media (min-width: 768px) {
    flex-flow: row nowrap;
  }
`

export const Confirm = styled.span`
  margin-left: 2rem;
  @media (min-width: 768px) {
    margin-left: 4rem;
    margin-bottom: -1.3rem;
  }
`

export const BottomReview = styled.div`
  display: flex;
  flex-flow: column nowrap;
  align-items: center;
  @media (min-width: 768px) {
    flex-flow: row nowrap;
  }
`
export const RatingDiv = styled.div`
  margin: 1rem 0;
`