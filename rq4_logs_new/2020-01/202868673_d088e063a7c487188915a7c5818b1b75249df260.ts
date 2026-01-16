import styled from "styled-components";

export const MenuContainer = styled.div`
  min-width: 10%;
  height: 540px;
  flex: 2 2 auto;
  display: flex;
  align-items: center;
  justify-content: center;
  border: 1px solid black;
  margin: 0 7.5px 15px;
  overflow: hidden;

  &:hover {
    cursor: pointer;

    & .background-image {
      transform: scale(1.1);
      transition: transform 6s cubic-bezier(0.35, 0.45, 0.45, 0.95);
    }

    & .content {
      opacity: 0.9;
    }
  }

  .background-image {
    width: 100%;
    height: 100%;
    background-position: center;
    background-repeat: no-repeat;
    background-size: cover;
  }
`;

export const ItemsContent = styled.div`
  height: 70px;
  padding: 0 25px;
  display: flex;
  flex-direction: column;
  align-items: center;
  justify-content: center;
  border: 1px solid black;
  background-color: white;
  opacity: 0.7;
  position: absolute;
`;

export const TitleH1 = styled.h1`
  font-weight: bold;
  margin-bottom: 6px;
  font-size: 22px;
  color: #4a4a4a;
`;

export const SubtitleSpan = styled.span`
  font-weight: lighter;
  font-size: 16px;
`;