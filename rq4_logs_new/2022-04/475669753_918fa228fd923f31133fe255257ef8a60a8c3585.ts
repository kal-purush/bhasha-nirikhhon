import styled from 'styled-components/native';

export const Container = styled.View`
  flex: 1;
  background-color: #283ea3;
  align-items: center;
`;

export const ContainerImage = styled.View`
  margin-top: 80px;
`;

export const ContainerInfos = styled.View`
  margin-top: 20px;
  align-items: center;
  width: 250px;
`;

export const Image = styled.Image`
  width: 250px;
  height: 250px;
`;

export const Title = styled.Text`
  color: #1fce65;
  font-size: 21px;
  font-weight: bold;
  text-align: center;
`;

export const Information = styled.Text`
  color: #20a773;
  font-size: 18px;
  text-align: center;

  margin-top: 20px;
`;

export const ButtonUpdate = styled.TouchableOpacity`
  background-color: #1ed760;
  padding: 10px 20px;
  border-radius: 20px;

  margin-top: 20px;
  align-items: center;
  width: 200px;
`;

export const TextUpdate = styled.Text`
  color: #283ea3;
  text-transform: uppercase;
  font-weight: bold;
`;

export const ButtonIgnore = styled.TouchableOpacity`
  margin-top: 20px;
  align-items: center;
`;

export const TextIgnore = styled.Text`
  color: #1ed760;
  text-transform: uppercase;
  font-weight: bold;
`;