import styled from 'styled-components/native';

export const Container = styled.View``;

export const ContainerButton = styled.TouchableOpacity`
  background: ${({ theme }) => theme.Colors.DARK_GRAY_2};
  flex-direction: row;
  padding: 10px 10px;
  margin: 10px 10px;
  border-radius: 5px;
  justify-content: space-between;
  align-items: center;
`;

export const ContainerInput = styled.View`
  height: 100%;
  background: ${({ theme }) => theme.Colors.DARK_GRAY_2};
  flex-direction: row;
  padding: 10px 10px;
  border-radius: 5px;
  justify-content: space-between;
  align-items: center;
`;

export const TextInput = styled.Text`
  font-size: ${({ theme }) => theme.FontSizes.TEXT_BUTTON_SEARCH}px;
  flex: 1;
  text-align: center;
  font-weight: bold;
`;

export const Input = styled.TextInput`
  flex: 1;
  font-size: 18px;
  color: ${({ theme }) => theme.Colors.WHITE};
  height: 100%;
`;

export const Button = styled.TouchableOpacity``;