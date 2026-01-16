import styled from 'styled-components/native';

export const Container = styled.View`
  flex-direction: row;
  justify-content: space-between;
  align-items: center;
  padding: 50px 10px 0px;
`;

export const ContainerUser = styled.View`
  flex-direction: row;
  align-items: center;
`;

export const ContainerButtons = styled.View`
  flex-direction: row;
`;

export const ImageUser = styled.Image`
  width: 60px;
  height: 60px;
  border-radius: 100px;
`;

export const TextScreen = styled.Text`
  color: ${({ theme }) => theme.Colors.WHITE};
  font-size: ${({ theme }) => theme.FontSizes.TEXT_PERIODE_SIZE}px;
  font-family: 'Roboto_700Bold';
  margin-left: 10px;
`;

export const ButtonSearch = styled.TouchableOpacity``;

export const ButtonAdd = styled.TouchableOpacity`
  margin-left: 10px;
`;