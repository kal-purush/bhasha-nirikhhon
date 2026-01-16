import styled from 'styled-components/native';

export const ContainerAlbum = styled.TouchableOpacity`
  flex-direction: row;
  margin-top: 20px;
  align-items: center;
`;

export const ContainerAlbumInfo = styled.View`
  margin-left: 15px;
  justify-content: center;
  flex: 0.95;
`;

export const ImageAlbum = styled.Image`
  width: 70px;
  height: 70px;
`;

export const NameAlbum = styled.Text`
  color: ${({ theme }) => theme.Colors.WHITE};
  font-size: 20px;
`;

export const ArtistName = styled.Text`
  color: ${({ theme }) => theme.Colors.LIGHT_GRAY_3};
  font-size: 18px;
`;

export const Button = styled.TouchableOpacity``;

export const FlatList = styled.FlatList`
  margin: 0px 0px 85px 20px;
`;