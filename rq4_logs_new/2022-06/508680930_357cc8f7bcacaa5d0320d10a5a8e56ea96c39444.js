import React from 'react';
import {StyleSheet, View} from 'react-native';

import {colors} from '../styles/base.js';

const ScreenTemplate = ({children}) => {
  return <View style={styles.container}>{children}</View>;
};

export {ScreenTemplate};

const styles = StyleSheet.create({
  container: {
    flex: 1,
    padding: 15,
    backgroundColor: colors.secondary,
    // justifyContent: 'center',
    // alignItems: 'center',
  },
});