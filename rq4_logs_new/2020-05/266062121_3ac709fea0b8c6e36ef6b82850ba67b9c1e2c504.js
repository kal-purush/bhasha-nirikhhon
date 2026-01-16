import React, { useState, useContext } from  'react'
import { NavigationContainer } from '@react-navigation/native'
import { createStackNavigator } from '@react-navigation/stack'
import { createBottomTabNavigator } from '@react-navigation/bottom-tabs'

import {ContextProvider} from './src/context/MainContext'
import {Context} from './src/context/MainContext'
import AccountScreen from './src/screens/AccountScreen'
import SigninScreen from './src/screens/SigninScreen'
import SignupScreen from './src/screens/SignupScreen'
import TrackCreateScreen from './src/screens/TrackCreateScreen'
import TrackDetailScreen from './src/screens/TrackDetailScreen'
import TrackListScreen from './src/screens/TrackListScreen'

const Stack = createStackNavigator()
const Tab = createBottomTabNavigator()

function Tracks() {
  return (
    <Stack.Navigator initialRouteName="TrackList">
      <Stack.Screen name="TrackDetail" component={TrackDetailScreen} />
      <Stack.Screen name="TrackList" component={TrackListScreen} />
    </Stack.Navigator>
  )
}

function App() {
  const {isSignedIn} = useContext(Context)

  return isSignedIn ? (
      <NavigationContainer>
        <Tab.Navigator>
          <Tab.Screen name="TrackCreate" component={TrackCreateScreen} options={{title: "Create"}}/>
          <Tab.Screen name="Tracks" component={Tracks}/>
          <Tab.Screen name="Account" component={AccountScreen} />
        </Tab.Navigator>
      </NavigationContainer>
  ) : (
      <NavigationContainer>
        <Stack.Navigator initialRouteName="Signin"  screenOptions={{headerShown: false}} >
          <Stack.Screen name="Signin" component={SigninScreen} />
          <Stack.Screen name="Signup" component={SignupScreen} />
        </Stack.Navigator>
      </NavigationContainer>
  )
}

export default () => {
  return (
    <ContextProvider>
      <App />
    </ContextProvider>
  )
}