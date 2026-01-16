import styled from "styled-components";


export const Div = styled.div`
  display: flex;
  align-items: center;
  flex-direction: column;
  padding: 30px 30px;
  box-sizing: border-box;
  @media (min-width: 800px) {
    justify-content: center;
    margin: 0px;
  }
`;


export const DivA = styled.div`
  position: fixed;
  background-color: rgba(51, 51, 51, 0.5);
  width: 100%;
  height: 100%;
  padding: 10px;
  box-sizing: border-box;
  display: flex;
  justify-content: center;
  align-items: center;
  top: 0px;
  left: 0px;
  z-index: 10;
  button {
    margin-top: 15px;
  }
  label {
    display: flex;
    text-align: start;
    width: 100%;
    justify-content: flex-start;
  }
  svg {
    align-self: flex-end;
    width: 35px;
    height: 35px;
  }
`;

export const DivContainer = styled.div`
  display: flex;
  justify-content: initial;
  flex-direction: column;
  background-color: #ffffff;
  border-radius: 30px;
  max-width: 500px;
  min-width: 200px;
  min-height: 300px;
  span {
    text-align: right;
    font-size: 20px;
  }
`;


export const Content = styled.div`
`

export const InfoUser = styled.div`
    display: flex;
    flex-direction: column;
    align-items: flex-start;
    span{
      display: flex;
      p:first-child{
        font-weight: bold;
        width: 18ch;
        text-align: left;
        margin-right: 0px;
      }
    }
`

export const Header = styled.header`
    display: flex;
    width: 100%;
    justify-content: space-between;
    margin-bottom: 20px;
`

export const Profile = styled.div`
    display: flex;
    align-items: center;
    margin-right: 15px;
    h1 {
      font-size: 23px;
      margin-left: 15px;
    }
`