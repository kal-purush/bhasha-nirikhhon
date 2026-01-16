import styled from "styled-components";

export const Container = styled.div`
  width: 310px;
  height: 320px;
  background-color: #f4f3f2;
  border-radius: 18;
  box-shadow: rgba(99, 99, 99, 0.2) 0px 2px 8px 0px;
  cursor: pointer;
  margin-top: 2rem;
  margin-right: 0.5rem;
  text-decoration: none;
  color: #000;

  transition: all 200ms ease-in-out;

  &:hover {
    background: #fdfdfd;
  }
`;

export const Image = styled.img`
  width: 310px;
  /* height: 260px; */
`;

export const ContainerSubtitle = styled.div`
  display: flex;
  flex-direction: row;
  justify-content: space-between;
  align-items: center;
  padding-top: 6px;
  padding: 10px;
`;

export const Subtitle = styled.p`
  font-family: "Roboto", sans-serif;
`;

export const Description = styled.p`
  font-family: "Roboto", sans-serif;
  color: #939393;
  padding: 10px;
  padding-top: 0;
`;