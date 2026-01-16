import styled from 'styled-components/native'

export const Container = styled.View`
  flex: 1;
  background-color: #8267e5;
  justify-content: center;
  padding: 40px;
`
export const BackgroundImage = styled.ImageBackground`
  flex: 1;
  justify-content: center;
`
export const Title = styled.Text`
  font-family: 'Archivo_700Bold';
  color: #fff;
  font-size: 32px;
  line-height: 37px;
  max-width: 180px;
`
export const Description = styled.Text`
  margin-top: 37px;
  color: #d4c2ff;
  line-height: 26px;
  font-size: 16px;
  font-family: 'Poppins_400Regular';
  max-width: 240px;
`
export const OkButton = styled.TouchableOpacity`
  margin: 40px 0;
  background-color: #04d371;
  height: 58px;
  align-items: center;
  justify-content: center;
  border-radius: 8px;
`

export const OkButtonText = styled.Text`
  color: #fff;
  font-family: 'Archivo_700Bold';
  font-size: 16px;
`