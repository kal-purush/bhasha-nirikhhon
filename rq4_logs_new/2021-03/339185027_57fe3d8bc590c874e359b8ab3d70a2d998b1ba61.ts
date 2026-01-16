import { styled, theme } from '../../../../theme';

export const MovieCardInfoText = styled.div`
  display: flex;
  justify-content: space-between;
  align-items: baseline;
  color: ${theme.colors.white + theme.transparency['50']};
  font-weight: 500;
`;

export const MovieCardDescr = styled.div``;

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