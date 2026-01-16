import { styled, theme } from '../../../theme';

export const MovieCardRelease = styled.div`
  padding: 3px 10px;
  display: inline-block;
  border: 1px solid ${theme.colors.grayBg};
  border-radius: 4px;
`;

export const MovieCardTitle = styled.h3``;

export const MovieCardGenres = styled.p`
  font-size: 14px;
`;

export const MovieCardImg = styled.img`
  max-width: 100%;

  &:hover {
    cursor: pointer;
  }
`;

export const MovieRating = styled.div``;

export const MovieTagline = styled.div``;

export const MovieData = styled.div``;

export const MovieRuntime = styled.div``;

export const MovieDescr = styled.div``;