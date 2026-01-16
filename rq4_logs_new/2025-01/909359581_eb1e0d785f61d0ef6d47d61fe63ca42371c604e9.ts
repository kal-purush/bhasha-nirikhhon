import styled from "styled-components";

import favoriteIconFull from "../../assets/favorito_01.svg";
import favoriteIcon from "../../assets/favorito_02.svg";
import favoriteIconHover from "../../assets/favorito_03.svg";

export const HeroContainer = styled.div`
  display: flex;
  flex-direction: column;
  gap: 4px;
  max-width: 15rem;
`;

export const HeroThumbnail = styled.div`
  object-fit: cover;
  img {
    border-bottom: 4px solid ${(props) => props.theme["red-500"]};
    aspect-ratio: 1/1;
    width: 100%;
  }
`;

export const HeroInfo = styled.div`
  display: flex;
  justify-content: space-between;

  a {
    font-weight: bold;
    text-decoration: none;
    color: ${(props) => props.theme["gray-500"]};

    &:hover {
      color: ${(props) => props.theme["gray-300"]};
    }
  }

  img {
    cursor: pointer;
  }
`;

export const FavoriteButtonContainer = styled.button`
  border: none;
  width: 20px;
  height: 20px;
  background-repeat: no-repeat;
  background-size: cover;
  background-color: transparent;
  cursor: pointer;
  background-image: url("${favoriteIcon}");
  &:hover {
    background-image: url("${favoriteIconHover}");
  }
`;

export const FavoritedButtonContainer = styled(FavoriteButtonContainer)`
  background-image: url("${favoriteIconFull}");
`;