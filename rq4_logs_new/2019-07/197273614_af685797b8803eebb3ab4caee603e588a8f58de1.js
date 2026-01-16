import main from './components/main';
import chat from './components/chat';

import { createAppContainer, createStackNavigator } from 'react-navigation'

const AppNavigator = createStackNavigator({
  main: { screen: main },
  chat: { screen: chat },
});

const AppContainer = createAppContainer(AppNavigator);

export default AppContainer;