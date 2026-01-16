import styled from "styled-components";

export const Header = styled.div`
  width: 412px;
  height: 44px;
  align-items: center;
  display: flex;
  flex-direction: row;
  justify-content: space-between;
  background-color: #ffffff;
  color: black;
  border-bottom: 1px solid #e2e2e2;
  font-size: 15px;
`;

export const Container = styled.section`
  width: 100%;
  min-height: 100vh;
  background-color: white;
  color: black;
  overflow-y: scroll;
  display: flex;
  flex-direction: column;
`;

export const Category = styled.div`
  width: 412px;
  height: 44px;
  align-items: center;
  display: flex;
  flex-direction: row;
  justify-content: space-between;
  background-color: gainsboro;
  color: black;
  border-bottom: 1px solid #e2e2e2;
  font-size: 15px;
  padding-left: 10px;
`;

export const Lists = styled.ul`
  width: 100%;
`

export const List = styled.li`
  width: 100%;
  height: 44px;
  font-size: 15px;
  display: flex;
  align-items: center;
  border-bottom: 0.5px solid gainsboro;
  justify-content: space-between;
  padding: 0 10px;
`

export const Button = styled.button`
  background-color: transparent;
  border: none;
  color: black;
`

export const Logout = styled.div`
  width: 412px;
  height: 44px;
  align-items: center;
  display: flex;
  flex-direction: row;
  justify-content: center;
  background-color: gainsboro;
  color: red;
  border-bottom: 1px solid #e2e2e2;
  font-size: 15px;
  padding-left: 10px;
  position: absolute;
  bottom: 5%;
`;