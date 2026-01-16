import React from "react"
import { Pressable, StyleSheet, Text } from "react-native";
import { TouchableOpacity } from "react-native-gesture-handler";

function Button({ title, onPress }) {
  return (
    <Pressable 
      style={styles.button}
      onPress={onPress}
      color='#fff'>
      <Text style={styles.text}>{title}</Text>
    </Pressable>
  );
}

export default Button

const styles = StyleSheet.create({
  button: {
    backgroundColor: '#AAC9CF',
    borderRadius: 4
  },
  text: {
    textTransform: 'uppercase',
    color: '#fff',
    letterSpacing: 1,
    padding: 10
  },
});