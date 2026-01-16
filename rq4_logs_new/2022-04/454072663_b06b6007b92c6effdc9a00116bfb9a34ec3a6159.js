import { StyleSheet, Text, View } from "react-native";
import React from "react";
import RNPickerSelect from "react-native-picker-select";
import { Chevron } from "react-native-shapes";

import { WHITE, DARK } from "../../../theme/color";

const index = () => {
  return (
    <RNPickerSelect
      placeholder={{
        label: "Category",
        value: null,
      }}
      onValueChange={(value) => console.log(value)}
      style={{
        ...pickerSelectStyles,
        iconContainer: {
          top: 20,
          right: 15,
        },
      }}
      items={[
        { label: "Terror", value: "terror" },
        { label: "Drama", value: "drama" },
        { label: "Acción", value: "accion" },
      ]}
      Icon={() => <Chevron size={1.2} color={WHITE} />}
    />
  );
};

export default index;

const pickerSelectStyles = StyleSheet.create({
  inputIOS: {
    fontSize: 15,
    paddingVertical: 12,
    paddingHorizontal: 10,
    borderWidth: 1,
    borderRadius: 30,
    color: WHITE,
    backgroundColor: DARK,
    paddingRight: 30, // to ensure the text is never behind the icon
  },
  inputAndroid: {
    fontSize: 15,
    paddingHorizontal: 10,
    paddingVertical: 8,
    borderWidth: 0.5,
    borderRadius: 30,
    color: WHITE,
    backgroundColor: DARK,
    paddingRight: 30, // to ensure the text is never behind the icon
  },
});