import React, {useState, useEffect} from 'react';
import {Platform, StatusBar} from 'react-native';
import {Provider} from 'react-redux';
import {
  NavigationContainer,
  useNavigationContainerRef,
} from '@react-navigation/native';
import {createStackNavigator} from '@react-navigation/stack';
import {createBottomTabNavigator} from '@react-navigation/bottom-tabs';
import Ionicons from 'react-native-vector-icons/Ionicons';
import Orientation from 'react-native-orientation-locker';
import About from './Screens/About';
import Home from './Screens/Home';
import Register from './Screens/Register';
import Login from './Screens/Login';
import Splash from './Screens/Splash';
import StoryDetail from './Screens/StoryDetail';
import store from './redux/store';

Ionicons.loadFont();

const Stack = createStackNavigator();
const Tab = createBottomTabNavigator();

const DashboardTab = () => {
  return (
    <Tab.Navigator
      screenOptions={({route}) => ({
        tabBarShowLabel: true,
        gestureEnabled: false,
        tabBarStyle: {backgroundColor: '#E6F5F9'},
        tabBarActiveTintColor: '#0096C1',
        tabBarIcon: ({focused}) => {
          let iconName;
          if (route.name === 'Home') {
            iconName = focused ? 'home' : 'home-outline';
          } else if (route.name === 'About') {
            iconName = focused
              ? 'information-circle'
              : 'information-circle-outline';
          }

          return (
            <Ionicons
              name={iconName}
              size={22}
              color={focused ? '#0096C1' : '#5F5F5F'}
            />
          );
        },
      })}>
      <Tab.Screen
        name="Home"
        component={Home}
        options={{
          gestureEnabled: false,
          headerTitle: 'Top Stories from Hacker News',
        }}
      />
      <Tab.Screen
        name="About"
        component={About}
        options={{
          gestureEnabled: false,
          headerTitle: 'My Profile',
        }}
      />
    </Tab.Navigator>
  );
};

const App = () => {
  const [logoAnimationComplete, setLogoAnimationComplete] = useState(false);
  const navigationRef = useNavigationContainerRef();

  useEffect(() => {
    Orientation.lockToPortrait();

    return () => {
      Orientation.unlockAllOrientations();
    };
  }, []);

  const onLogoAnimationEnd = () => {
    setLogoAnimationComplete(true);
  };

  if (Platform.OS === 'android') {
    StatusBar.setBackgroundColor('white');
  }

  return (
    <Provider store={store}>
      <NavigationContainer ref={navigationRef}>
        <Stack.Navigator
          initialRouteName="Splash"
          screenOptions={{
            headerTitleAlign: 'center',
            headerBackTitleVisible: false,
            gestureEnabled: false,
          }}>
          <Stack.Screen
            name="Splash"
            options={{
              animationTypeForReplace: 'pop',
              headerShown: false,
            }}>
            {() => <Splash onLogoAnimationEnd={onLogoAnimationEnd} />}
          </Stack.Screen>

          <Stack.Screen
            name="CreateAccount"
            component={Register}
            options={{title: 'Create account'}}
          />

          <Stack.Screen
            name="Login"
            component={Login}
            options={{title: 'Welcome back!'}}
          />

          <Stack.Screen
            name="Dashboard"
            component={DashboardTab}
            options={{headerShown: false}}
          />

          <Stack.Screen
            name="StoryDetail"
            component={StoryDetail}
            options={{
              title: 'Hacker News',
            }}
          />
        </Stack.Navigator>
      </NavigationContainer>
    </Provider>
  );
};

export default App;