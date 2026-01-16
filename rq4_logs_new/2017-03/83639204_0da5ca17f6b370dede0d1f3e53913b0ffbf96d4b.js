import React, {Component}from 'react';
  import{
    Text,
    TouchableOpacity,
    View
} from 'react-native';

export default class SimpleButton extends Component{
  render(){
    return(
      <TouchableOpacity onPress={ () => console.log('Pressed!')}>
      <View>
      <Text>SimpleButton</Text>
      </View>
      </TouchableOpacity>
    );
  }
}