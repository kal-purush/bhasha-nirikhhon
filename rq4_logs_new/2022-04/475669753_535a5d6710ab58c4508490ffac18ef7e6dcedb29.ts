import styled from 'styled-components/native';

type ProgressProps = {
  barPercentage: number;
};

export const Container = styled.View``;

export const ContainerBar = styled.View`
  position: absolute;
  bottom: 0px;
  width: 100%;
  height: 3px;
  background: ${({ theme }) => theme.Colors.GRAY};
`;

export const ContainerProgress = styled.View<ProgressProps>`
  position: absolute;
  bottom: 0px;
  width: ${({ barPercentage }) => barPercentage}%;
  height: 3px;
  background: ${({ theme }) => theme.Colors.WHITE};
`;