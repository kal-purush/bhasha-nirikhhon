import styled from 'styled-components/native';

export const ContainerCategory = styled.View``;

export const ContainerItem = styled.View`
  max-width: 200px;
  margin-right: 20px;
`;

export const FlatList = styled.FlatList`
  margin-bottom: 100px;
  margin-left: 10px;
`;

export const FlatListAlbuns = styled.FlatList``;

export const ImageAlbum = styled.Image`
  width: 200px;
  height: 200px;
`;

export const NameCategory = styled.Text`
  color: ${({ theme }) => theme.Colors.WHITE};
  font-size: ${({ theme }) => theme.FontSizes.TEXT_NAME_CATEGORY}px;
  font-family: 'Montserrat_600SemiBold';
  font-weight: bold;

  margin-top: 30px;
  margin-bottom: 20px;
`;

export const NameAlbum = styled.Text`
  color: ${({ theme }) => theme.Colors.LIGHT_GRAY};
  font-size: ${({ theme }) => theme.FontSizes.TEXT_NAME_ALBUM}px;
`;