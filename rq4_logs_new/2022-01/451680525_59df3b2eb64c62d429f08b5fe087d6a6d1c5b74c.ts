import styled from "styled-components";

export const Container = styled.div`
  background: #000;
  width: 100vw;
  height: 10vh;
`;
export const BoxLogo = styled.div`
  top: 2px;
  padding-left: 30px;
  display: flex;
  flex-direction: row;
  align-items: center
  `;

export const ImgLogo = styled.img`
  margin: 2px;
  width: 20%;
  @media (min-width: 1000px) {
  width: 10%;
    
  }
`;

export const TitleLogo = styled.div`
  display: none;
  @media (min-width: 1000px) {
    display: flex;
    font-size: 25px;
    font-family: "Rock Salt", cursive;
    font-weight: bold;
    color: #fff;
    margin-right: 7px;
  }
`;