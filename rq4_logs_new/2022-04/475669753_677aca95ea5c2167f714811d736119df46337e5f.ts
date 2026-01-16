import styled from 'styled-components/native';

export const Container = styled.ScrollView`
  flex: 1;
  background-color: ${({ theme }) => theme.Colors.BLACK};
`;

export const ContainerHeader = styled.View`
  align-items: center;
  padding-top: 30px;
`;

export const ContainerAlbumInfo = styled.View`
  width: 100%;
  margin-top: 30px;
  margin-left: 30px;
`;

export const ContainerInfoTimer = styled.View`
  margin-top: 5px;
  flex-direction: row;
`;

export const ContainerButtons = styled.View`
  width: 100%;
  margin-left: 30px;
  padding: 15px 0px;
  flex-direction: row;
`;

export const ImageAlbum = styled.Image`
  width: 250px;
  height: 250px;
`;

export const ButtonBack = styled.TouchableOpacity`
  position: absolute;
  left: 10px;
  top: 30px;
`;

export const NameAlbum = styled.Text`
  color: ${({ theme }) => theme.Colors.WHITE};
  font-size: ${({ theme }) => theme.FontSizes.TEXT_PERIODE_SIZE}px;
`;

export const AlbumOwner = styled.Text`
  color: ${({ theme }) => theme.Colors.WHITE};
  font-size: 20px;
  margin: 5px 0px;
`;

export const LikesAlbum = styled.Text`
  color: ${({ theme }) => theme.Colors.LIGHT_GRAY_2};
  font-size: 18px;
  margin-left: 10px;
`;

export const Button = styled.TouchableOpacity``;

export const ButtonDownload = styled.TouchableOpacity`
  margin: 0 30px;
`;

export const ButtonPlay = styled.TouchableOpacity`
  position: absolute;
  right: 30px;
  bottom: 0px;
`;