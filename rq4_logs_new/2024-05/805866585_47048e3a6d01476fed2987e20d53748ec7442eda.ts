import { StyleProp, TextStyle, ViewStyle } from 'react-native';

type CustomButtonProps = {
  title: string;
  onPress: () => void;
  style?: StyleProp<ViewStyle>;
  textStyle?: StyleProp<TextStyle>;
};

export default CustomButtonProps;