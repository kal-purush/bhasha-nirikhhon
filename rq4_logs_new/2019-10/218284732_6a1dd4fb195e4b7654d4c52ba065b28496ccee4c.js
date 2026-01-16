import React, { Component } from 'react';
import AppNavigation from './navigation/AppNavigation'
import { View, StatusBar, Platform } from 'react-native'
import { Provider } from 'react-redux'
import store from './store/config'

const App = () => {

  const STATUSBAR_HEIGHT = Platform.OS === 'ios' ? 20 : StatusBar.currentHeight;

  return (
    <Provider store={store}>
      <View style={{ flex: 1 }}>
        <StatusBar backgroundColor='black' barStyle='dark-content' />
        <View style={{ height: STATUSBAR_HEIGHT }}></View>
        <AppNavigation />
      </View>
    </Provider>
  )
}

export default App



