import React, { useEffect } from 'react';
import { NavigationContainer } from '@react-navigation/native';
import { createNativeStackNavigator } from '@react-navigation/native-stack';
import { StatusBar } from 'expo-status-bar';
import { LogBox } from 'react-native';

// Import screens
import Auth from './screens/Auth';
import NewAccount from './screens/NewAccount';
import Home from './screens/Home';
import Chat from './screens/Chat';
import Setting from './screens/Home/Setting';

// Import Firebase config
import firebase from './Config';

// Ignore specific Firebase warnings
LogBox.ignoreLogs([
  'Setting a timer',
  'AsyncStorage has been extracted',
  'Non-serializable values were found in the navigation state',
]);

const Stack = createNativeStackNavigator();

export default function App() {
  // Set up database presences to track online status
  useEffect(() => {
    // Reference to the Firebase database
    const database = firebase.database();
    const connectedRef = database.ref('.info/connected');

    const handleConnectionChange = (snapshot) => {
      // When we connect or disconnect, update our connection status
      if (snapshot.val() === true) {
        console.log('Connected to Firebase');
      } else {
        console.log('Disconnected from Firebase');
      }
    };

    connectedRef.on('value', handleConnectionChange);

    return () => {
      connectedRef.off('value', handleConnectionChange);
    };
  }, []);

  return (
    <NavigationContainer>
      <Stack.Navigator screenOptions={{ headerShown: false }}>
        <Stack.Screen name="Auth" component={Auth} />
        <Stack.Screen 
          name="NewAccount" 
          component={NewAccount} 
          options={{
            headerShown: true,
            headerTitle: "Back to Auth"
          }}
        />
        <Stack.Screen name="Home" component={Home} />
        <Stack.Screen 
          name="Chat" 
          component={Chat}
          options={{
            headerShown: false // We manage this in the component itself
          }}
        />
        <Stack.Screen name="Setting" component={Setting} />
      </Stack.Navigator>
      <StatusBar style="auto" />
    </NavigationContainer>
  );
}