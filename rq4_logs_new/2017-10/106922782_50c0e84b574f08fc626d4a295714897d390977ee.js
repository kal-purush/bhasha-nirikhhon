import React, { Component } from 'react';
import {
  StyleSheet,
  View
} from 'react-native';

import {
  SweetText,
  SweetTitle,
  LightPink,
} from 'sweet-company-styles';

export default class App extends Component<{}> {
  render() {
    return (
      <View style={styles.container}>
        <SweetTitle text="Sweetly Delicious Title" />
        <SweetText style={styles.welcome}>
          Tasty tasty text
        </SweetText>
      </View>
    );
  }
}

const styles = StyleSheet.create({
  container: {
    flex: 1,
    justifyContent: 'center',
    alignItems: 'center',
    backgroundColor: LightPink,
  },
  welcome: {
    textAlign: 'center',
    margin: 10,
  },
});