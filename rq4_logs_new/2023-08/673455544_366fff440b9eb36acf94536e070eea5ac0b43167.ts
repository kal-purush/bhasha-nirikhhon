import { styled } from "styled-components";
import img28 from "../../../public/Group28.png";

export const DivHeader = styled.div`
  display: flex;
  justify-content: space-between;
  align-items: center;
  padding: 0 5%;
  border-bottom: 2px solid #f6f6f6;
`;

export const Nav = styled.nav`
  display: flex;
  gap: 10px;
`;

export const ButtonHeader = styled.button`
  background-color: white;
  border: 1px solid;
  border-radius: 6px;
  cursor: pointer;
  padding: 10px 20px;
  font-size: 16px;
`;

export const MobileNav = styled.nav`
  display: flex;
  flex-direction: column;
  gap: 10px;
  align-items: flex-end;
  padding: 10px 20px;
`;

export const DivImg = styled.div`
  background-image: url(${img28});
  background-position: center;
  background-repeat: no-repeat;
  background-size: cover;
  min-height: 544px;
  display: flex;
  justify-content: center;
  align-items: center;
`;

export const DivIntImg = styled.div`
  display: flex;
  align-items: center;
  justify-content: center;
  flex-direction: column;
`;

export const H1Styled = styled.h1`
  font-size: 50px;
  color: white;
  font-family: "Lexend";
`;

export const PStyled = styled.p`
  font-size: 36px;
  color: white;
  font-family: "Lexend";
  padding: 0 0 0 10px;
`;