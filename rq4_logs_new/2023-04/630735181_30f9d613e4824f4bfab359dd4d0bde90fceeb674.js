import React from 'react';
import styled from 'styled-components/native';

const ButtonFunctionArea = styled.View`
  flex: 1;
  justify-content: center;
  align-items: center;
`;

const ButtonFunction = styled.TouchableOpacity`
  justify-content: center;
  align-items: center;
  background-color: ${props => props.background || 'transparent'};
  border-radius: 10px;
  width: 65px;
  height: 65px;
`;

const ButtonFunctionText = styled.Text`
  font-size: 35px;
  font-weight: bold;
  color: ${props => props.color || props.theme.colors.colorNumber};
`;

export default ({value, color, background, onPress}) => {
  return (
    <ButtonFunctionArea>
      <ButtonFunction background={background} onPress={onPress}>
        <ButtonFunctionText color={color}>{value}</ButtonFunctionText>
      </ButtonFunction>
    </ButtonFunctionArea>
  );
};