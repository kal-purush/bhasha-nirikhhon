import styled from 'styled-components/native';

export const Container = styled.View`
  flex: 1;
  background-color: ${({ theme }) => theme.Colors.BLACK_2};
`;

export const ContainerInput = styled.View`
  flex: 0.1;
`;

export const ContainerResults = styled.View`
  flex: 1;
  background-color: ${({ theme }) => theme.Colors.BLACK};
  justify-content: center;
  align-items: center;
`;

export const Text1 = styled.Text`
  font-size: ${({ theme }) => theme.FontSizes.TEXT_SEARCH_INFO}px;
  color: ${({ theme }) => theme.Colors.WHITE};
`;

export const Text2 = styled.Text`
  font-size: ${({ theme }) => theme.FontSizes.TEXT_SEARCH_INFO_2}px;
  color: ${({ theme }) => theme.Colors.GRAY};
  margin-top: 10px;
`;