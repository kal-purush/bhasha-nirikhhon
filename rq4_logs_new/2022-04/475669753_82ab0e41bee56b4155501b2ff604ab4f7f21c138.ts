import styled from 'styled-components/native';

export const Container = styled.View`
  flex-direction: row;
  justify-content: space-between;
  align-content: center;

  padding: 30px 10px 0px;
`;

export const ContainerButtons = styled.View`
  flex-direction: row;
`;

export const ContainerPlaylists = styled.View``;

export const NotificationCircle = styled.View`
  position: absolute;
  width: 10px;
  height: 10px;

  background-color: red;
  border-radius: 10px;
  top: 0px;
  left: 4px;
`;

export const TextPeriode = styled.Text`
  font-size: ${({ theme }) => theme.FontSizes.TEXT_PERIODE_SIZE}px;
  color: ${({ theme }) => theme.Colors.WHITE};
  font-family: 'Roboto_700Bold';
`;

export const Button = styled.TouchableOpacity``;

export const ButtonCenter = styled.TouchableOpacity`
  margin: 0px 30px;
`;