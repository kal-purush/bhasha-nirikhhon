import styled from 'styled-components/native';

type ButtonFooterProps = {
  isCircle?: boolean;
};

export const ContainerFooter = styled.View``;

export const ContainerButtonFooter = styled.View<ButtonFooterProps>`
  background-color: ${({ theme }) => theme.Colors.DARK_GRAY_3};
  width: 70px;
  height: 70px;
  justify-content: center;
  align-items: center;
  border-radius: ${({ isCircle }) => (isCircle ? 100 : 20)}px;
  margin-right: 10px;
`;

export const NamePlaylist = styled.Text`
  color: ${({ theme }) => theme.Colors.WHITE};
  font-size: ${({ theme }) => theme.FontSizes.TEXT_SEARCH_INFO}px;
`;

export const ButtonFooter = styled.TouchableOpacity`
  margin-top: 20px;
  flex-direction: row;
  align-items: center;
`;