import styled from "styled-components";

export const Div = styled.div`
  display: flex;
  align-items: center;
  flex-direction: column;
  padding: 30px;
  box-sizing: border-box;
  @media (min-width: 800px) {
    justify-content: center;
    margin: 0px;
  }
`;

export const Form = styled.form`
  display: flex;
  align-items: center;
  flex-direction: column;
  width: 100%;
  h3 {
    color: var(--dark-grey);
    font-family: "Sansita", sans-serif;
    font-weight: 700;
    margin-bottom: 20px;
    font-size: 30px;
  }
`;

export const Error = styled.ul`
  margin: 0px;
  text-align: start;
  margin-bottom: 15px;
  font-size: 10px;
  color: rgb(240, 42, 42);
  padding: 0px 0px 0px 5px;
  li {
    list-style: inside;
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
  z-index:10;
  color: white;
  button {
    margin-top: 15px;
    background: #4CDE18;
    font-weight: bold;
    height: 35px;
  }
  label:not(label:first-child) {
    display: flex;
    text-align: start;
    width: 100%;
    justify-content: flex-start;
    margin-top: 10px;
  }
  option {
    border: 0px;
  }
  svg {
    align-self: flex-end;
    width: 35px;
    height: 35px;
  }
`;

export const DivContainer = styled.div`
  display: flex;
  justify-content: center;
  flex-direction: column;
  background-color: #fff;
  border-radius: 30px;
  width: 400px;
  min-width: 200px;
  color: black;
  span {
    text-align: right;
    font-size: 20px;
  }
`;