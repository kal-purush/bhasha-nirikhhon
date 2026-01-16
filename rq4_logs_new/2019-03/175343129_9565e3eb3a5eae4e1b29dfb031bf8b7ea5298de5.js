import React from 'react';
import { Keyboard, TouchableWithoutFeedback, KeyboardAvoidingView, TextInput, Slider, TouchableHighlight, Alert, View, Text, Button, StyleSheet, Dimensions, Image } from 'react-native';
import { createAppContainer, createStackNavigator, StackActions, NavigationActions, createBottomTabNavigator } from 'react-navigation'; // Version can be specified in package.json
import { ImagePicker } from 'expo';
import { ACTION_REQUEST_IGNORE_BATTERY_OPTIMIZATIONS } from 'expo/build/IntentLauncherAndroid/IntentLauncherAndroid';
import AreaScreen from './components/screens/AreaScreen';
import ReportScreen from './components/screens/ReportScreen';
import HomeScreen from './components/screens/HomeScreen';

//intialize state to empty area aray

const styles = StyleSheet.create({
  red: {
    color: 'red',
  },
  buttonContainer: {
    flexDirection: 'row',
    justifyContent: "center",
    alignItems: "center",
    borderStyle: 'solid',
    height: 80,
    borderRadius: 10,
    borderWidth: 1,
    margin: 5,
  },
  buttonText: {
    fontSize: 20
  }
});

async function alertIfRemoteNotificationsDisabledAsync() {
  const { Permissions } = Expo;
  const { status, permissions } = await Permissions.askAsync(Permissions.CAMERA_ROLL);
  if (status !== 'granted') {
    alert('Hey! You might want to enable notifications for my app, they are good.');
  } 
}

/*
class HomeScreen extends React.Component {
  
  static navigationOptions = {
    title: 'Home',
  };


 

  render() {

    //let response = getFromApiAsync();
    //console.log(response);

    return (
      <View style={{ flex: 1 }}>
        <View style={{ flex: 1, justifyContent: 'center', backgroundColor: "#00bfff", alignItems: "center",}}>
          <Text style={{fontSize: 26, fontWeight: "bold", }}>Ice Reports</Text>
        </View>
        
        <View style={{ flex: 4, justifyContent: 'flex-start' }}>
          <TouchableHighlight style={styles.buttonContainer} onPress={() => this.props.navigation.navigate('Frankenstein')} underlayColor="blue">
            <Text style={styles.buttonText}>Frankenstein</Text>
          </TouchableHighlight>
          <TouchableHighlight style={styles.buttonContainer} onPress={() => this.props.navigation.navigate('LakeWilloughby')} underlayColor="blue">
            <Text style={styles.buttonText}>Lake Willoughby</Text>
          </TouchableHighlight>
          <TouchableHighlight style={styles.buttonContainer} onPress={() => this.props.navigation.navigate('SmugglersNotch')} underlayColor="blue">
            <Text style={styles.buttonText}>Smuggler's Notch</Text>
          </TouchableHighlight>
          <TouchableHighlight style={styles.buttonContainer} onPress={() => this.props.navigation.navigate('SmugglersNotch')} underlayColor="blue">
            <Text style={styles.buttonText}>Chapel Pond</Text>
          </TouchableHighlight>
        </View>
      </View>
    );
  }  
}
*/


class Info extends React.Component {
  
  static navigationOptions = {
    title: 'SmugglersNotch',
  };

  render() {
    return (
      <View style={{ flex: 1, alignItems: 'stretch'}}>
        <View style={{ flex: 1, justifyContent: 'center', backgroundColor: "#00bfff", alignItems: "center",}}>
          <Text style={{fontSize: 26, fontWeight: "bold", }}>Info page</Text>
        </View>
        <View style={{ flex: 4, justifyContent: 'flex-start' }}>
          <Text>Ice reports and shops and insta page and donate</Text>
        </View>
      </View>
    );
  }  
}

class SmugglersNotch extends React.Component {
  
  static navigationOptions = {
    title: 'SmugglersNotch',
  };

  render() {
    return (
      <View style={{ flex: 1, alignItems: 'stretch'}}>
        <View style={{ flex: 1, justifyContent: 'center', backgroundColor: "#00bfff", alignItems: "center",}}>
          <Text style={{fontSize: 26, fontWeight: "bold", }}>Smuggler's Notch</Text>
        </View>
        <View style={{ flex: 4, justifyContent: 'flex-start' }}>
          <TouchableHighlight style={styles.buttonContainer} onPress={() => this.props.navigation.navigate('IceReport')} underlayColor="blue">
            <Text style={styles.buttonText}>ENT Gully 2/9</Text>
          </TouchableHighlight>
          <TouchableHighlight style={styles.buttonContainer} onPress={() => this.props.navigation.navigate('IceReport')} underlayColor="blue">
            <Text style={styles.buttonText}>Ragnarock 2/5</Text>
          </TouchableHighlight>
          <TouchableHighlight style={styles.buttonContainer} onPress={() => this.props.navigation.navigate('IceReport')} underlayColor="blue">
            <Text style={styles.buttonText}>Jefferson Slide 2/1</Text>
          </TouchableHighlight>
        </View>
      </View>
    );
  }  
}

class NewReport extends React.Component {
  
  state = {
    image: null,
    descText: null,
  };

  _onPressButton () {
    Alert.alert("pressed!")
  }

  _pickImage = async () => {
    let result = await ImagePicker.launchImageLibraryAsync({
      allowsEditing: true,
      aspect: [4, 3],
    });

    console.log(result);

    if (!result.cancelled) {
      this.setState({ image: result.uri });
    }
  };

  render() {
    let { image } = this.state;
    return (
        <KeyboardAvoidingView style={{ flex: 1, alignItems: 'stretch', justifyContent: "flex-start"}} behavior="position">
          <TouchableWithoutFeedback onPress={Keyboard.dismiss} accessible={false} style={{flex: 1, backgroundColor: 'red'}}>
            <View>
              <View style={{ height: 100, justifyContent: 'center', backgroundColor: "#00bfff", alignItems: "center",}}>
                <Text style={{fontSize: 26, fontWeight: "bold", }}>New report</Text>
              </View>
              <View style={{height:200, alignItems: 'center', marginTop: 20 }}>
                <View style={{ flex: 1, alignItems: 'center', justifyContent: 'center' }}>
                  <Button
                    title="Pick an image from camera roll"
                    onPress={this._pickImage}
                  />
                  {image &&
                    <Image source={{ uri: image }} style={{ width: 200, height: 200 }} />}
                </View>
              </View>
              <View style={{ height: 80, justifyContent: 'flex-end'  }}>
                <Text style={{fontSize: 20}}>Rating:</Text>
              </View>          
              <Slider
                value={this.state.value}
                onValueChange={value => this.setState({ value })}
              />
              <View style={{ height: 80, justifyContent: 'flex-end'  }}>
                <Text style={{fontSize: 20}}>Description:</Text>
              </View>
            </View>
          </TouchableWithoutFeedback>
          <TextInput multiline={true} placeholder='description' onChangeText={(text) => this.setState({text})} style={{borderColor: 'gray', borderWidth: 1, height:100, justifyContent: 'flex-start'}} />
          <TouchableHighlight style={styles.buttonContainer} onPress={() => console.log("submitted")} underlayColor="blue">
            <Text style={styles.buttonText}>Submit</Text>
          </TouchableHighlight>
        </KeyboardAvoidingView>
    );
  }  
}

export const HomeScreenStack = createStackNavigator({
  Home: {
    screen: HomeScreen,
  },
  Frankenstein: {
    screen: AreaScreen,
  },
  SmugglersNotch: {
    screen: SmugglersNotch,
  },
  IceReport: {
    screen: ReportScreen,
  },
  AreaScreen: {
    screen: AreaScreen,
  }
}, {
    initialRouteName: 'Home',
});

export const TabNavigator = createBottomTabNavigator({
  Home: {
    screen: HomeScreenStack,
    navigationOptions: {
      tabBarLabel: "Browse Reports",
    }
  },
  Info: {
    screen: Info,
    navigationOptions: {
      tabBarLabel: "Info",
    }
  },
  NewReport: {
    screen: NewReport,
    navigationOptions: {
      tabBarLabel: "New Report",
    }
  },
});




export default createAppContainer(TabNavigator);