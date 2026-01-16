import { StatusBar } from 'expo-status-bar';
import React from 'react';
import { StyleSheet, Text, View } from 'react-native';
import  LoginScreen from './screens/LoginScreen'
import  HomeScreen from './screens/HomeScreen'
import  InventoryScreen from './screens/InventoryScreen'
import  SearchScreen from './screens/SearchScreen'
import  LogoutScreen from './screens/LogoutScreen'
import MyHeader from './components/MyHeader';
import {createAppContainer, createSwitchNavigator} from 'react-navigation';
import {createTabNavigator} from 'react-navigation-tabs'

export default function App() {
  return (
    <AppContainer/>
    
  );
}

const TabNavigator= createTabNavigator({
  HomeScreen: {screen: HomeScreen},
  InventoryScreen:{screen: InventoryScreen},
  SearchScreen:{screen: SearchScreen},
  LogoutScreen:{screen: LogoutScreen},

})

const switchNavigator= createSwitchNavigator({
  LoginScreen: {screen: LoginScreen},
  TabNavigator: {screen: TabNavigator}
})

const AppContainer= createAppContainer(switchNavigator)

const styles = StyleSheet.create({
  container: {
    flex: 1,
    backgroundColor: '#fff',
    alignItems: 'center',
    justifyContent: 'center',
  },
});