import React from 'react';
import {
  TouchableOpacity, Text, Image, StyleSheet, View,
} from 'react-native';
import PropTypes from 'prop-types';
import { fonts, colors } from '../styles';

const Button = ({ onPress, image, text }) => (
  <TouchableOpacity
    style={styles.touchable}
    onPress={onPress}
  >
    <View style={styles.container}>
      <Image
        source={image}
        style={styles.icon}
        resizeMode="contain"
      />
      <Text style={styles.text}>{text}</Text>
    </View>
  </TouchableOpacity>
);

const styles = StyleSheet.create({
  container: {
    height: 36,
    flexDirection: 'row',
    justifyContent: 'center',
    alignItems: 'center',
  },
  icon: {
    height: 28,
    width: 28,
    marginRight: 8,
  },
  text: {
    color: colors.blue,
    fontSize: fonts.big,
  },
});

Button.propTypes = {
  image: PropTypes.number.isRequired,
  text: PropTypes.string.isRequired,
  onPress: PropTypes.func.isRequired,
};

export { Button };