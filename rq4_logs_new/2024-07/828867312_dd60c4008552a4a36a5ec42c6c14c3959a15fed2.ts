import styled, { css } from "styled-components";
import { Link } from "react-router-dom";

export const HeaderSec = styled("div")`
  display: flex;
  justify-content: space-between;
  padding: 30px 30px;
  border-bottom: #f0f0f0 solid 1px;
  margin-inline: auto;
`;

const button = css`
  display: flex;
  gap: 20px;
  border: transparent solid 1px;
  background-color: #fff;
  border-radius: 10px;
  box-shadow: 5px 5px 20px 0 #0000001a;
  text-decoration: none;
  cursor: pointer;
  transition: all 0.3s;

  &:hover {
    background-color: #f0f0f0;
    border: #e7e7e7 solid 1px;
  }
`;

export const HeaderButton = styled("button")`
  color: #1c1c27;
  margin-top: 15px;
  padding: 15px 20px;
  align-items: center;
  ${button};

  @media screen and (max-width: 576px) {
    padding: 5px;
    gap: 5px;
  }
`;

export const HeaderLink = styled(Link)`
  color: #1c1c27;
  margin-top: 15px;
  padding: 15px 20px;
  align-items: center;
  ${button};

  @media screen and (max-width: 576px) {
    padding: 10px;
  }
`;

export const UpdateButton = styled("button")`
  justify-self: flex-end;
  max-width: max-content;
  padding: 10px;
  color: #1c1c27;
  ${button}
`;

export const MainSec = styled("section")`
  display: grid;
  margin: 10px 28px;
`;

export const GridSec = styled("div")`
  display: grid;
  grid-template-columns: repeat(3, 1fr);
  gap: 35px;

  @media screen and (max-width: 990px) {
    grid-template-columns: repeat(2, 1fr);
    gap: 10px 25px;
  }

  @media screen and (max-width: 576px) {
    grid-template-columns: repeat(1, 1fr);
    gap: 10px;
  }
`;

export const Box = styled("div")`
  display: flex;
  gap: 30px;
  justify-content: space-between;
  text-align: center;
`;

export const NewsItemLink = styled(Link)`
  flex-direction: column;
  justify-content: space-between;
  gap: 30px;
  margin-top: 15px;
  padding: 15px 20px;

  ${button}
  &:hover {
    transform: scale(1.05);
  }
`;

export const FirstText = styled("p")`
  text-decoration: none;
  font-size: 22px;
  font-weight: 700;
  color: #1c1c27;
  margin: 0;
`;

export const SecondText = styled("p")`
  font-size: 14px;
  font-weight: 400;
  color: #adadad;
  margin: 0;
`;

export const UserText = styled("p")`
  font-size: 16px;
  font-weight: 600;
  color: #1c1c27;
  margin: 0;
`;

export const CommentBox = styled("div")`
  display: grid;
  gap: 10px;
  background-color: #fff;
  border-radius: 10px;
  padding: 15px 20px;
  margin-top: 15px;
  box-shadow: 5px 5px 20px 0 #0000001a;
  text-decoration: none;
`;

export const BoxContent = styled("div")`
  display: flex;
  gap: 10px;
`;

export const CommentButton = styled("button")`
  color: #adadad;
  align-items: center;
  justify-content: center;
  text-decoration: none;
  margin-top: 15px;
  padding: 15px 20px;
  ${button}
`;

export const NewsBox = styled("div")`
  display: grid;
  gap: 20px;
  max-width: 70%;
  margin-inline: auto;
  margin-block: 40px;

  @media screen and (max-width: 990px) {
    max-width: none;
    margin-inline: 50px;
  }

  @media screen and (max-width: 576px) {
    margin-inline: 20px;
  }
`;

export const LinkText = styled(Link)`
  color: #adadad;
`;

export const ItemBox = styled("div")`
  display: flex;
  gap: 8px;
  align-items: center;
  border-radius: 10px;
  box-shadow: 5px 5px 20px 0 #0000001a;
  padding: 10px;
  max-width: max-content;
  cursor: pointer;
  border: transparent solid 1px;

  &:hover {
    background-color: #f0f0f0;
    border: #e7e7e7 solid 1px;
  }
`;

export const TextContent = styled("div")`
  display: grid;
  gap: 8px;
  justify-content: space-between;
`;

export const SecondSec = styled("div")`
  display: flex;
  gap: 20px;
  justify-content: space-between;
  padding: 15px 0;
  border-block: #f0f0f0 solid 1px;
`;

export const LoaderContainer = styled("div")`
  width: 100%;
  height: 100%;
  position: fixed;
  top: 0;
  left: 0;
  display: grid;
  place-items: center;
`;

export const LoaderStyle = styled("div")`
  border: 10px solid #e3e3e3;
  border-top: 10px solid #adadad;
  border-radius: 50%;
  width: 60px;
  height: 60px;
  animation: spin 1s linear infinite;
}

@keyframes spin {
  0% {
    transform: rotate(0deg);
  }

  100% {
    transform: rotate(360deg);
  }
`;

export const LinkButton = styled(Link)`
  align-items: center;
  max-width: max-content;
  color: #1c1c27;
  margin-top: 15px;
  padding: 15px 20px;
  ${button}
`;

export const PageNumberBox = styled("div")`
  display: flex;
  gap: 20px;
  justify-content: center;
  margin-inline: auto;
  margin-block: 40px;

  @media screen and (max-width: 990px) {
    max-width: none;
    margin-inline: 50px;
  }

  @media screen and (max-width: 576px) {
    margin-inline: 20px;
  }
`;