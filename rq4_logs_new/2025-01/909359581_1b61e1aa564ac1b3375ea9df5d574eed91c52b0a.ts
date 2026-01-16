import styled from "styled-components";

import favoriteIconFull from "../../assets/favorito_01.svg";
import favoriteIcon from "../../assets/favorito_02.svg";
import favoriteIconHover from "../../assets/favorito_03.svg";

export const FavoriteContainer = styled.div`
  display: flex;
  align-items: center;
  gap: 8px;
`;

export const FavoriteButtonContainer = styled.div`
  border: none;
  width: 20px;
  height: 20px;
  background-repeat: no-repeat;
  background-size: cover;
  background-color: transparent;
  &:hover {
    background-image: url("${favoriteIconHover}");
  }
  cursor: pointer;
`;

export const FavoriteButtonContainerOn = styled(FavoriteButtonContainer)`
  background-image: url("${favoriteIconFull}");
`;

export const FavoriteButtonContainerOff = styled(FavoriteButtonContainer)`
  background-image: url("${favoriteIcon}");
`;

export const FavoriteLabel = styled.span`
  color: ${(props) => props.theme["red-300"]};
  font-size: 0.875rem;
  font-weight: 600;
`;