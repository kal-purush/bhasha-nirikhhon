import React from 'react';
import {Text, View, StyleSheet} from 'react-native';

export default function App() {
  return (
    <View style={styles.container}>
      <View style={styles.roundedWithVerticalBorders}>
        <Text>Lor1 Ipsum1</Text>
      </View>
    </View>
  );
}

const styles = StyleSheet.create({``
  container: {
    flex: 1,
    justifyContent: 'center',
    backgroundColor: 'white',
    padding: 8,
  },
  roundedWithVerticalBorders: {
    backgroundColor: '#90ee90',
    padding: 20,
    borderRadius: 20,

    borderBottomColor: 'red',
    borderBottomWidth: 5,

    borderTopColor: 'red',
    borderTopWidth: 5,
  },
});