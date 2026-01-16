import styled from "styled-components";
import bg from "../../assets/home_sushi.jpg";

export const Container = styled.div`
  display: flex;
  background-image: url(${bg});
  background-position: center center;
  background-repeat: no-repeat;
  background-size: 100vw 100vh;
  width: 100vw;
  height: 100vh;
  font-family: "Rock Salt";
  color: white;
  header {
    margin-top: 30px;
    display: flex;
    align-self: flex-start;
    justify-content: space-between;
    width: 100vw;
    margin-right: 10%;
  }
  .logo {
    display: flex;
    align-items: center;
    font-size: 24px;
  }
  a {
    background: none;
    border: none;
    color: white;
    font-size: 20px;
    text-decoration: none;
    :hover {
      color: red;
    }
  }
`;

export const Info = styled.div`
  width: 20vw;
  height: 100vh;
  background-color: rgba(0, 0, 0, 70%);
  display: flex;
  flex-direction: column;
  justify-content: space-around;
  font-size: 26px;
  padding: 0 15px;
  .redLetter {
    color: red;
  }
  .right {
    align-self: flex-end;
  }
  .contact {
    font-size: 15px;
  }

  h3 {
    font-size: 30px;
    margin-bottom: 10px;
  }
  strong {
    font-weight: 700;
  }
`;