import React from 'react';
import { SafeAreaView, ScrollView, StyleSheet, Text, View, Image, TouchableOpacity, TextInput, Button, Alert } from 'react-native';
import { LinearGradient } from 'expo-linear-gradient';
import { DataTable } from 'react-native-paper';
import { NavigationContainer } from '@react-navigation/native';
import { createStackNavigator } from '@react-navigation/stack';
import { createBottomTabNavigator } from '@react-navigation/bottom-tabs';

import firebase from 'firebase';

import LoginScreen from './screens/LoginScreen.js'
import TelainicialScreen from './screens/TelainicialScreen.js'
import VendaScreen from './screens/VendaScreen.js'
import HistoricoScreen from './screens/HistoricoScreen.js'
import ConsultaScreen from './screens/ConsultaScreen.js'

const Tab = createBottomTabNavigator();

const firebaseConfig = {
  apiKey: "AIzaSyDnUkX7_rEy0PsNl9wbXFcV-DSSQyLnH9o",
  authDomain: "somos-64ffb.firebaseapp.com",
  projectId: "somos-64ffb",
  storageBucket: "somos-64ffb.appspot.com",
  messagingSenderId: "144268457481",
  appId: "1:144268457481:web:8437473e1dbd2e16442ad8"
};

if (firebase.apps.length == 0) {
  firebase.initializeApp(firebaseConfig);
}

const Stack = createStackNavigator();

export default function App() {
  return (
    <LinearGradient colors={['#bec4fd', '#f8fafc']}>
    <SafeAreaView style={styles.container}>
    <ScrollView style={styles.scrollView}>
    <View style={styles.fundo}>
        <NavigationContainer>
              <Tab.Navigator>
              <Tab.Screen name="INICIO" component={TelainicialScreen}/>
              <Tab.Screen name="VENDA" component={VendaScreen}/>
              <Tab.Screen name="HISTORICO" component={HistoricoScreen}/>
              <Tab.Screen name="SAIR" component={LoginScreen}/>
            </Tab.Navigator>
        </NavigationContainer>
    </View>
    </ScrollView>
    </SafeAreaView>
    </LinearGradient>
  );
}

const styles = StyleSheet.create({
    fundo:{
      height: 720,
  },
});