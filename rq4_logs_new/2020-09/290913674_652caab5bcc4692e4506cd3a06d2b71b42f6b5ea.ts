import styled from 'styled-components/native';

import colors from '../../styles/color';

export const List = styled.View`
  padding: 8px;
`;

export const ChannelContainer = styled.TouchableOpacity`
  flex-direction: row;
  align-items: center;
  justify-content: space-between;
  padding-right: 14px;
  margin-bottom: 25px;
`;

export const LeftSide = styled.View`
  flex-direction: row;
  align-items: center;
`;

export const RigthSide = styled.View`

`;

export const Avatar = styled.View`
  width: 48px;
  height: 48px;
  border-radius: 25px;
  background-color: ${colors.tag};
`;

export const Column = styled.View`
  padding-left: 10px;
`;

export const Username = styled.Text`
  font-size: 16px;
  color: ${colors.black};
  font-weight: bold;
`;

export const Info = styled.Text`
  font-size: 13px;
  color: ${colors.gray};
  margin-top: 1px;
`;

export const WhiteCircle = styled.View`
 width: 9px;
  height: 9px;
  border-radius: 5px;
  background-color: ${colors.black}; 
  opacity: .85;
`;