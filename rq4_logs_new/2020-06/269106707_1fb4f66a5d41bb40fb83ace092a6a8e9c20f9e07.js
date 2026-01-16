import React, { useReducer } from 'react';
import { StyleSheet, Text, View } from 'react-native';
import { enableScreens } from 'react-native-screens';
import { combineReducers, createStore, applyMiddleware } from 'redux';
import { Provider } from 'react-redux'
import { ActionSheetProvider } from '@expo/react-native-action-sheet';
import ReduxThunk from 'redux-thunk';
import * as firebase from 'firebase';
import { Appearance, AppearanceProvider, useColorScheme } from 'react-native-appearance';

import AuthNavigator from './navigator/ScreenNavigator';
import MessageReducer from './store/reducers/UpdateMessage';
import UserReducer from './store/reducers/UpdateUsers';
import ContactReducer from './store/reducers/UpdateContacts';
import LastSeenReducer from './store/reducers/UpdateLastSeen';

enableScreens();

const firebaseConfig = {
  apiKey: "AIzaSyAMuo4T63HGds3FlhknLxYfBDtcOOZ_Zmk",
  authDomain: "chat-application-master-rn.firebaseapp.com",
  databaseURL: "https://chat-application-master-rn.firebaseio.com",
  projectId: "chat-application-master-rn",
  storageBucket: "chat-application-master-rn.appspot.com",
  messagingSenderId: "184579749928",
  appId: "1:184579749928:web:24bb954b82b43e48c71d1d",
  measurementId: "G-VHMQTB75PE"
}

if (!firebase.apps.length) {
  firebase.initializeApp(firebaseConfig);
}

const rootReducer = combineReducers({
  messageReducer: MessageReducer,
  userReducer: UserReducer,
  contactReducer: ContactReducer,
  lastSeenReducer: LastSeenReducer
})

const store = createStore(rootReducer, applyMiddleware(ReduxThunk))

export default function App() {
  return (
    <AppearanceProvider>
      <ActionSheetProvider>
        <Provider store = {store} >
          <AuthNavigator />
        </Provider>
      </ActionSheetProvider>
    </AppearanceProvider>
  );
}

const styles = StyleSheet.create({
  container: {
    flex: 1,
    backgroundColor: '#fff',
    alignItems: 'center',
    justifyContent: 'center',
  },
});