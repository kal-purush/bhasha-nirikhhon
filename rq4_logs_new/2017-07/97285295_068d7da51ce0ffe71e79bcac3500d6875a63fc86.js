import React from 'react';
import { StyleSheet, Text, TouchableOpacity, View } from 'react-native';
import { addNavigationHelpers, StackNavigator, NavigationActions } from 'react-navigation'
import { observable, action } from 'mobx'
import { observer } from 'mobx-react'

import AppNavigator from './navigators/AppNavigator';
import NavigationStore from './stores/NavigationStore';

@observer
class App extends React.Component {
  constructor(props, context) {
    super(props, context)
    this.store = new NavigationStore();
  }

  render() {
    return (
      <AppNavigator navigation={addNavigationHelpers({
        dispatch: this.store.dispatch,
        state: this.store.navigationState,
      })}/>
    )
  }
};

export default App