import { StyleSheet } from "react-native";
import { inputIconSize, inputBoxSize } from '@src/theme/Dimensions';
import { Colors } from "@src/theme/Colors";

export const InputBoxStyle = StyleSheet.create({
  container: {
    marginTop: inputBoxSize.marginTop,
    flexDirection: 'row',
    alignItems: 'center',
    borderWidth: inputBoxSize.borderWidth,
    borderColor: Colors.light.inputBorder,
    backgroundColor: Colors.light.inputBackground,
    borderRadius: inputBoxSize.borderRadius,
    paddingHorizontal: inputBoxSize.paddingHorizontal,
    paddingVertical: inputBoxSize.paddingVertical,
  },
  containerFocused: {
    borderColor: Colors.light.inputFocus,
  },
  errorOcurred: {
    borderColor: Colors.light.errorText,
  },
  input: {
    flex: 1,
    height: inputIconSize[40],
    color: 'black',
    fontSize: inputBoxSize.fontSize,
  },
  leftIcon: {
    marginRight: inputBoxSize.leftIcon.marginRight,
  },
  leftIconFocused: {
    color: 'blue',
  },
  leftIconBlurred: {
    color: 'gray',
  },
  rightIcon: {
    width: inputBoxSize.rightIcon.width,
    height: inputBoxSize.rightIcon.height,
    marginLeft: inputBoxSize.rightIcon.marginLeft,
  },
  inputFocused: {
    borderColor: 'blue',
  },
  inputError: {
    borderColor: Colors.light.errorText,
    color: Colors.light.errorText,
  },
  errorContainer: {
    marginTop: inputBoxSize.error.marginTop,
    flexDirection: 'row',
    alignItems: 'center',
  },
});