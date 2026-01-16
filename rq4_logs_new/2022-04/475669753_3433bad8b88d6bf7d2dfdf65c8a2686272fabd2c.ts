import * as Animatable from 'react-native-animatable';
import styled from 'styled-components/native';

type ContainerProps = {
  backgroundColor: string;
};

export const Container = styled.TouchableOpacity<ContainerProps>`
  flex-direction: row;
  align-items: center;
  padding: 5px 10px;
  background-color: ${({ backgroundColor }) => backgroundColor};

  width: 100%;
  height: ${({ theme }) => theme.Sizes.HEIGHT_MUSIC_PLAYING_SIZE}px;
  border-radius: 10px;
  z-index: 1;

  position: absolute;
  bottom: ${({ theme }) => theme.Sizes.BOTTOM_TAB_MENU_SIZE - 10}px;
`;

export const ContainerInfoMusic = styled.View`
  max-width: 300px;
  margin-left: 20px;
  margin-right: 15px;
  overflow: hidden;
`;

export const ContainerButtons = styled.View`
  flex: 1;
  flex-direction: row;
  justify-content: space-between;
`;

export const ImageMusic = styled.Image`
  width: 45px;
  height: 45px;
  border-radius: 35px;
`;

export const NameMusic = styled(Animatable.Text)`
  color: ${({ theme }) => theme.Colors.WHITE};
  max-width: 200px;
  padding-right: 10px;
`;

export const NameArtist = styled.Text`
  color: ${({ theme }) => theme.Colors.LIGHT_GRAY_2};
  font-weight: bold;
`;

export const Button = styled.TouchableOpacity``;