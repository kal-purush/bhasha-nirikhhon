import styled from 'styled-components/native'

export const Container = styled.View`
  background-color: #fff;
  border: 1px solid #e6e6f0;
  border-radius: 8px;
  margin-bottom: 16px;
  overflow: hidden;
`

export const Profile = styled.View`
  flex-direction: row;
  align-items: center;
  padding: 24px;
`

export const Avatar = styled.Image`
  width: 64px;
  height: 64px;
  border-radius: 32px;
  background-color: #eee;
`

export const ProfileInfo = styled.View`
  margin-left: 16px;
`

export const Name = styled.Text`
  font-family: 'Archivo_700Bold';
  color: #32264d;
  font-size: 20px;
`

export const Subject = styled.Text`
  font-family: 'Poppins_400Regular';
  font-size: 12px;
  margin-top: 4px;
  color: #6a6180;
`

export const Bio = styled.Text`
  margin: 10px 24px;
  font-family: 'Poppins_400Regular';
  line-height: 24px;
  font-size: 14px;
  color: #6a6180;
`
export const Footer = styled.View`
  margin-top: 24px;
  background-color: #fafafc;
  padding: 24px;
  align-items: center;
`

export const Price = styled.Text`
  font-family: 'Poppins_400Regular';
  color: #6a6180;
  font-size: 14px;
`

export const PriceValue = styled.Text`
  font-family: 'Archivo_700Bold';
  color: #8257e5;
  font-size: 16px;
`

export const ButtonsContainer = styled.View`
  flex-direction: row;
  margin-top: 16px;
`

export const Favorite = styled.TouchableOpacity`
  background-color: #8257e5;
  width: 56px;
  height: 56px;
  border-radius: 8px;
  align-items: center;
  justify-content: center;
  margin-right: 8px;
`

export const Whatsapp = styled.TouchableOpacity`
  background-color: #04d361;
  flex: 1;
  flex-direction: row;
  border-radius: 8px;
  align-items: center;
  justify-content: center;
  margin-right: 8px;
`

export const ContactText = styled.Text`
  font-family: 'Archivo_700Bold';
  font-size: 14px;
  color: #fff;
  margin-left: 12px;
`