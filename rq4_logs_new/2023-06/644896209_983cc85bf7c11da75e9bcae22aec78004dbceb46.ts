import { Colors } from "@src/theme/Colors";
import { StyleSheet } from "react-native";
import {Dimensions} from 'react-native';
const {width} = Dimensions.get('window');

export const SettingStyle = StyleSheet.create({
  container: {
    padding: 10,
    justifyContent:'center',
    alignSelf:'center',
    backgroundColor: 'rgb(231, 232, 235)',
    width: width * 0.95,
  },
  titleContainer: {
    alignItems: 'center',
    paddingBottom: 20,
  },
  select: {
    fontSize: 20,
  },
  row: {
    flexDirection: 'row',
    justifyContent: 'space-between',
    paddingHorizontal: 10,
    paddingVertical: 5,
  },
  selectedRow: {
    backgroundColor: 'rgb(45, 45, 45)',
  },
  selectedText: {
    color: 'rgb(231, 232, 235)',
  },
  text: {
    color: 'rgb(45, 45, 45)',
  },
});