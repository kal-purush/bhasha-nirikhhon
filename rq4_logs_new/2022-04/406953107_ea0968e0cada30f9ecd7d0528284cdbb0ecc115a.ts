/* eslint-disable import/prefer-default-export */
import styled from 'styled-components/native';

export const ContentContainer = styled.View`
  height: 100%;
  display: flex;
  justify-content: center;
  width: 100%;
  margin-top: -10%;
`;

export const TitleContainer = styled.View`
  margin-bottom: 30px;
  text-align: center;
  background-color: #f2f2f2;
  height: 20px;
  z-index: 1;
`;

// export const ButtonView = styled.View`
//   width: 70%;
//   position: absolute;
//   bottom: 10%;
// `;

export const ButtonHeader = styled.TouchableOpacity`
  position: absolute;
  display: flex;
  flex-direction: row;
  align-items: center;
  width: 100%;
  height: 50px;
  justify-content: flex-start;
  margin-left: -10px;
  z-index: 1;
  top: 10%;
  left: 0px;
`;

export const ButtonContainer = styled.View`
  position: absolute;
  display: flex;
  flex-direction: row;
  justify-content: space-between;
  width: 100%;
  padding-horizontal: 12%;
  bottom: 10%;
`;

export const ButtonView = styled.View`
  width: 45%;
`;