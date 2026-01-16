import styled from "@emotion/styled";

export const Seatbox = styled.div`
  border: 1px solid black;
  border-radius: 20px;
  width: 80%;
  /* position: fixed; */
  bottom: 20%;
`;

export const Seatst = styled.div`
  display: flex;
  justify-content: center;
`;

export const Img = styled.div`
  background-color: lightgray;
  width: 50vh;
  margin: 5px;
  height: 50vh;
`;

export const Button = styled.div`
  width: 10vh;
  height: 5vh;
  border-radius: 20px;
  background-color: blue;
  color: white;
  display: flex;
  justify-content: center;
  margin: 5%;
  align-items: center;

  &:hover {
    background-color: white;
    color: blue;
  }
`;