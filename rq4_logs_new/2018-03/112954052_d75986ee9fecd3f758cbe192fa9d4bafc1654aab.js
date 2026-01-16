import React from 'react';
import Expo from 'expo'
import { StyleSheet, View, Text, Image } from 'react-native';
import { TabNavigator, StackNavigator } from 'react-navigation';
import * as firebase from 'firebase';
import { Provider } from 'react-redux';
import ReduxThunk from 'redux-thunk';
import Icon from 'native-base'

import store from './store';

import WelcomeScreen from './screens/WelcomeScreen';
import AuthScreen from './screens/AuthScreen';
import HomeScreen from './screens/HomeScreen';
import SportsScreen from './screens/SportsScreen';
import TeamsScreen from './screens/TeamsScreen';
import NewsScreen from './screens/NewsScreen';
import AccountScreen from './screens/AccountScreen';

export default class App extends React.Component {
    // async componentWillMount() {
    //     await Expo.Font.loadAsync({
    //         'Roboto': require('native-base/Fonts/Roboto.ttf'),
    //         'Roboto_medium': require('native-base/Fonts/Roboto_medium.ttf'),
    //     });
    // }
render() {
    const RootNavigator = TabNavigator({
        welcome: { screen: WelcomeScreen },
        auth: { screen: AuthScreen },
        main: {
            screen: TabNavigator({
                home: {
                    screen: HomeScreen,
                    navigationOptions: () => ({
                        tabBarIcon: ({ tintColor }) => (
                            <Image
                                source={require('./assets/icons/home.png')}
                                style={[styles.icon, { tintColor }]}
                            />
                        )
                    })
                },
                sports: {
                    screen: SportsScreen,
                    navigationOptions: () => ({
                        tabBarIcon: ({ tintColor }) => (
                            <Image
                                source={require('./assets/icons/team.png')}
                                style={[styles.icon, { tintColor }]}
                            />
                        )
                    })
                },
                teams: {
                    screen: TeamsScreen,
                    navigationOptions: () => ({
                        tabBarIcon: ({ tintColor }) => (
                            <Image
                                source={require('./assets/icons/tick-inside-circle.png')}
                                style={[styles.icon, { tintColor }]}
                            />
                        )
                    })
                 },
                news: {
                    screen: NewsScreen,
                    navigationOptions: () => ({
                        tabBarIcon: ({ tintColor }) => (
                            <Image
                                source={require('./assets/icons/newspaper.png')}
                                style={[styles.icon, { tintColor }]}
                            />
                        ),
                        title: 'News'
                    })
                 },
                account: {
                    screen: AccountScreen,
                    navigationOptions: () => ({
                        tabBarIcon: ({ tintColor }) => (
                            <Image
                                source={require('./assets/icons/avatar.png')}
                                style={[styles.icon, { tintColor }]}
                            />
                        )
                    })
                 },
            }, {
                tabBarPosition: 'bottom',
                swipeEnabled: true,
                animationEnabled: true,
                lazy: true,
                tabBarOptions: {
                    backBehavior: 'none',
                    activeTintColor: '#4CE0D2',
                    showLabel: true,
                }
            })
        }
     }, {
        navigationOptions: {
            tabBarVisible: false
        },
        lazy: true
    }
);

    return (
        <Provider store={store}>
            <View style={styles.container}>
                <RootNavigator />
            </View>
        </Provider>
        );
    }
}

const styles = StyleSheet.create({
  container: {
    flex: 1,
    justifyContent: 'center'
    },
    icon: {
        width: 30,
        height: 30
    }
});