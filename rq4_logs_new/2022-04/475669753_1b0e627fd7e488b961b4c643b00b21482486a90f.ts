import styled from 'styled-components/native';

export const ContainerPlaylist = styled.TouchableOpacity`
  flex-direction: row;
  margin-top: 20px;
`;

export const ContainerInfoPlaylist = styled.View`
  margin-left: 10px;
`;

export const ImagePlaylist = styled.Image`
  width: 70px;
  height: 70px;
`;

export const IconView = styled.View`
  transform: translateY(5px) rotate(45deg);
`;

export const NamePlaylist = styled.Text`
  color: ${({ isFixed, theme }) => (isFixed ? '#30ba61' : theme.Colors.WHITE)};
  font-size: ${({ theme }) => theme.FontSizes.TEXT_SEARCH_INFO}px;
`;

export const InfoPlaylist = styled.Text`
  color: ${({ theme }) => theme.Colors.LIGHT_GRAY_2};
  font-size: ${({ theme }) => theme.FontSizes.TEXT_SEARCH_INFO_2}px;
`;

export const FlatList = styled.FlatList`
  margin: 10px 10px;
`;