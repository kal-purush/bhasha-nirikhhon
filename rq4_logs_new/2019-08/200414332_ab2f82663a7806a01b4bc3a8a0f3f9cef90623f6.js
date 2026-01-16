import React from 'react';
import { StyleSheet, Text, View, Button } from 'react-native';
import { Icon } from 'react-native-elements';
import { createSwitchNavigator,
		 createAppContainer,
		 createDrawerNavigator,
		 createBottomTabNavigator,
		 createStackNavigator } from 'react-navigation';
import EventListItem from './components/EventListItem'

class WelcomeScreen extends React.Component{
	render() {
		return (
			<View style={{flex:1, alignItems: 'center', justifyContent:'center'}}>
				<Button title="Login" onPress={() => this.props.navigation.navigate('Dashboard')}/>
				<Button title="Sign Up" onPress={() => alert('button pressed')}/>
			</View>
		);
	}
};


class Events extends React.Component{
	render() {
		return (
			<View style={{flex:1, backgroundColor: '#f0f0f0'}}>
				<EventListItem
					eventTitle="Drinks at Nick's Pub for Women's World Cup"
					eventLocation="Dogtown, MO (4.6 mi)"
					eventImage="https://s3-media4.fl.yelpcdn.com/bphoto/twaytTxs40wJkhatmDozRg/ls.jpg"
					eventTime="Sat - June 29 - 11 AM"
					eventType="social"
					maxAttendees={4}
					currentAttendees={3}
				/>
				<EventListItem
					eventTitle="Hiking the Katy Trail"
					eventLocation="St. Charles, MO (12.3 mi)"
					eventImage="https://www.zocalopublicsquare.org/wp-content/uploads/2015/04/park.jpg"
					eventTime="Sat - June 29 - 11 AM"
					eventType="active"
					currentAttendees={4}
				/>
				<EventListItem
					eventTitle="Rock Climbing at Upper Limits"
					eventLocation="St. Louis, MO (2.3 mi)"
					eventImage="https://i2.wp.com/upperlimits.com/chesterfield/wp-content/uploads/sites/7/2018/12/5-1.jpg?resize=600%2C400&ssl=1"
					eventTime="Sat - June 29 - 11 AM"
					eventType="active"
					currentAttendees={4}
					maxAttendees={5}
				/>
				<EventListItem
					eventTitle="Catfish and the Bottlemen Concert"
					eventLocation="St. Louis, MO (2.3 mi)"
					eventTime="Sat - June 29 - 7 PM"
					eventType="music"
					currentAttendees={8}
				/>
			</View>
		);
	}
};

class Profile extends React.Component{
	render() {
		return (
			<View style={{flex:1, alignItems: 'center', justifyContent:'center'}}>
				<Text>Profile</Text>
			</View>
		);
	}
};

class Settings extends React.Component{
	render() {
		return (
			<View style={{flex:1, alignItems: 'center', justifyContent:'center', backgroundColor:'#f0f0f0'}}>
				<Text>Settings</Text>
			</View>
		);
	}
};

const AppTabNavigator = createBottomTabNavigator({
	Events,
	Profile,
	Settings
},
{
	navigationOptions: ({navigation}) => {
		const {routeName} = navigation.state.routes[navigation.state.index];
		if(routeName === 'Events'){
			return{
				headerTitle: (
					<View style={{paddingLeft:15}}>
						<Text style={{fontSize:25, fontWeight:'bold'}}>{routeName}</Text>
						<Text style={{color:'gray'}}>Within 50 miles</Text>
					</View>	
				),
				headerStyle: {
					elevation: 2,
					height: 70
					},
				};
		}else{
			return { header: null }
		}
	},
	defaultNavigationOptions:({navigation}) => ({
		tabBarIcon: ({ focused }) => {
		  const { routeName } = navigation.state;
		  let iconName;
		  switch (routeName) {
			case 'Events':
			  iconName = 'users';
			  break;
			case 'Profile':
			  iconName = 'id-card';
			  break;
			case 'Settings':
			  iconName = 'cogs';
			  break;
			default:
			  iconName = 'home';
			  break;
		  }
		  return (
			<Icon
			  size={30}
			  name={iconName}
			  type='font-awesome'
			  color={focused ? 'black' : '#f0f0f0'}
			/>
		  );
		},
	}),
	tabBarOptions: {
		style: {
			height: 50,
			borderTopColor: 'transparent',
			elevation: 10,
			
		},
		showIcon: true,
		showLabel: false,
		activeTintColor: 'black',
	},

});


const AppStackNavigator = createStackNavigator({
	AppTabNavigator: AppTabNavigator
});

const AppDrawerNavigator = createDrawerNavigator({
	Dashboard: {
		screen: AppStackNavigator
	}
});



const AppSwitchNavigator = createSwitchNavigator({
	Welcome: {screen: WelcomeScreen},
	Dashboard: {screen: AppStackNavigator}
	
},{
	initialRouteName: 'Welcome'
});



export default function App() {
  return (
	<AppContainer/>
  );
}

const AppContainer = createAppContainer(AppSwitchNavigator);