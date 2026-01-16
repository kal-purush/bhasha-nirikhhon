import styled from "styled-components";

export const HeroContainer = styled.div`
  margin: 40px auto;
  display: flex;
  flex-wrap: wrap;
  align-content: flex-start;
  justify-content: center;
  flex-direction: column;
  align-items: center;
  width: 100%;
`;

export const HeroHeader = styled.div`
  display: flex;
  justify-content: space-between;
  width: 100%;
`;

export const HeroInfoContainer = styled.div`
  margin-top: 64px;
  display: flex;
  flex-direction: column;
  gap: 4rem;
  width: 100%;
`;

export const HeroInfo = styled.div`
  display: flex;
  flex-direction: row;
  flex-wrap: nowrap;
  gap: 4rem;
  justify-content: space-between;

  @media (max-width: 560px) {
    flex-direction: column-reverse;
    align-items: center;
    gap: 16px;
  }
`;

export const HeroDetails = styled.div`
  max-width: 50%;

  h1 {
    font-size: 2rem;
    color: ${(props) => props.theme["gray-500"]};
    font-weight: bolder;
    text-transform: uppercase;
  }

  p {
    color: ${(props) => props.theme["gray-300"]};
    font-size: 0.875rem;
    line-height: 2;
    font-weight: 500;
  }

  @media (max-width: 560px) {
    max-width: 100%;
  }
`;

export const HeroDetailHeader = styled.div`
  display: flex;
  margin-bottom: 2rem;
  gap: 8px;
  align-items: baseline;
`;

export const HeroData = styled.div`
  display: flex;
  justify-content: space-between;
  gap: 2rem;
  margin-top: 32px;
`;
export const DataDiv = styled.div`
  display: flex;
  flex-direction: column;
  width: max-content;
  gap: 8px;

  h3 {
    font-size: 0.75rem;
  }
`;
export const DataDetails = styled.div`
  display: flex;
  align-items: center;
  gap: 16px;
  height: 40px;
  span {
    font-weight: bold;
    font-size: 0.875rem;
  }

  img {
    width: 28px;
  }
`;

export const HeroLastComic = styled.div`
  margin-top: 32px;
  font-weight: bold;
  font-size: 0.75rem;

  span {
    display: inline-flex;
    margin-left: 8px;
    font-weight: 500;
  }
`;

export const HeroThumbnail = styled.div`
  object-fit: cover;
  width: 50%;

  @media (max-width: 560px) {
    width: 100%;
  }
  img {
    border-bottom: 16px solid ${(props) => props.theme["red-500"]};
    aspect-ratio: 1/1;
    width: 100%;
  }
`;
export const HeroLastComics = styled.div`
  padding-bottom: 150px;
  width: 100%;

  h2 {
    margin-bottom: 64px;
  }
`;

export const ComicsGrid = styled.div`
  display: grid;
  grid-template-columns: repeat(auto-fit, minmax(133px, 1fr));
  gap: 4rem;
`;

export const ComicsGridItem = styled.div`
  img {
    width: 100%;
  }

  span {
    font-size: 0.75rem;
    font-weight: bold;
  }
`;