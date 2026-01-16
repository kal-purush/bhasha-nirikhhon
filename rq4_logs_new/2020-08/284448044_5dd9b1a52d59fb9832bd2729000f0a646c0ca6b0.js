import React from 'react';
import { createStackNavigator } from 'react-navigation-stack';

import About from '../screens/about';
import Header from '../shared/header';

const screens = {
	About: {
		screen: About,
		navigationOptions: ({ navigation }) => {
			return {
				headerTitle: () => <Header navigation={navigation} title=" about GameZone" />
			};
		}
	}
};

const AboutStack = createStackNavigator(screens, {
	defaultNavigationOptions: {
		headerTinColor: '#444',
		headerStyle: { backgroundColor: 'grey', height: 60 }
	}
});

export default AboutStack;