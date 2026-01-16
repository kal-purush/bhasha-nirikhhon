import React from 'react';
import { NavigationContainer } from '@react-navigation/native';
import { createStackNavigator } from '@react-navigation/stack';
import LoginScreen from './screens/LoginScreen'; 
import RegisterScreen from './screens/RegisterScreem'; 
import TabNavigator from './screens/TabNavigator'; 
import Settings from './screens/Settings';
import { UserProvider } from './components/userId';

const Stack = createStackNavigator();

const App = () => {
  return (
    <UserProvider> {}
      <NavigationContainer>
        <Stack.Navigator initialRouteName="LoginScreen">
          <Stack.Screen
            name="LoginScreen"
            component={LoginScreen}
            options={{ title: 'Inicio de Sesión', headerShown: false }}
          />
          <Stack.Screen
            name="RegisterScreen"
            component={RegisterScreen}
            options={{ title: 'Registro', headerShown: true }}
          />
          <Stack.Screen
            name="TabNavigator"
            component={TabNavigator}
            options={{ headerShown: false }} 
          />
          <Stack.Screen
            name="Settings"
            component={Settings}
            options={{ title: 'Configuración', headerShown: true }}
          />
        </Stack.Navigator>
      </NavigationContainer>
    </UserProvider>
  );
};

export default App;