import styled, { keyframes } from "styled-components";

const apear = keyframes`
from{
opacity: 0;
}
to{
opacity: 1;
}
`;

export const Container = styled.div`
  position: absolute;
  display: flex;
  justify-content: space-evenly;
  flex-direction: column;
  align-items: center;
  width: 100vw;
  height: 100vh;
  font-family: "Rock Salt";
  background-color: rgba(0, 0, 0, 80%);
  font-size: 24px;
  a {
    color: white;
    text-decoration: none;
  }
  animation: ${apear} 3s;

  @media (min-width: 1000px) {
    display: none;
  }
`;