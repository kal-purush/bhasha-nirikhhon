import styled from "styled-components";

export const ButtonStyled = styled.button`
  width: 200px;
  height: 46.88px;
  left: 88px;
  top: 1112.02px;
  color: ${({ theme }) => theme.colors.textColor};
  background: ${({ theme }) => theme.colors.mainColor};
  border-radius: 5px;
  border: none;
  width: 200px;
  height: 46.88px;
  font-family: "Sansation";
  font-weight: 300;
  cursor: pointer;

  &:active {
    background-color: ${({ theme }) => theme.colors.appBackground};
    color: ${({ theme }) => theme.colors.mainColor};
    border: ${({ theme }) => theme.colors.mainColor};
  }
`;