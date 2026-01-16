import React, { Component } from 'react';
import { Actions } from 'react-native-router-flux';
import { View, Text, TouchableOpacity } from 'react-native';
import FontAwesome from 'react-native-vector-icons/FontAwesome';
import EvilIcons from 'react-native-vector-icons/EvilIcons';
var color = 'rgb(142, 142, 147)';

export default class ActiveItem extends Component {

    StopPress() {
      Actions.stop_();
    }

    resetColor() {
      color = 'rgb(142, 142, 147)';
    }

    isColor() {
      if(this.props.route == 'Greenlink') {
          color = 'rgb(124, 31, 206)';
       } else if(this.props.route == 'B') {
          color = 'rgb(32, 116, 214)';
       } else if(this.props.route == 'C') {
         color = 'rgb(244, 74, 74)';
       } else if(this.props.route == 'EE') {
         color = 'rgb(62, 210, 177)';
       } else if(this.props.route == 'F') {
         color = 'rgb(243, 37, 168)';
       } else if(this.props.route == 'H') {
         color = 'rgb(119, 204, 27)';
       } else if(this.props.route == 'LX') {
         color = 'rgb(255, 204, 0)';
       } else if(this.props.route == 'All Campuses') {
         color = 'rgb(0, 156, 80)';
       } else if(this.props.route == 'New Brunsquick 1 Shuttle') {
         color = 'rgb(236, 76, 127)';
       } else if(this.props.route == 'New Brunsquick 2 Shuttle') {
         color = 'rgb(90, 200, 250)';
       } else if(this.props.route == 'RBHS/Hospital') {
         color = 'rgb(88, 86, 214)';
       } else if(this.props.route == 'REX B') {
         color = 'rgb(255, 149, 0)';
       } else if(this.props.route == 'REX L') {
         color = 'rgb(187, 120, 246)';
       } else if(this.props.route == 'Weekend 1') {
         color = 'rgb(244, 74, 74)';
       } else if(this.props.route == 'Weekend 2') {
         color = 'rgb(32, 116, 214)';
       }
    }


    render() {
      this.resetColor()
      this.isColor()
      console.log(color);
      return (
        <View style={{ marginHorizontal: 16 }}>
        <TouchableOpacity onPress={this.StopPress.bind(this)}>
        <View
        style={{
          borderBottomColor: 'rgb(200, 199, 204)',
          borderBottomWidth: 1,
        }}
        />
        <View style={{ flexDirection: 'row', marginVertical: 15, alignItems: 'center', justifyContent: 'space-between' }}>
        <View style={{ flexDirection: 'row' }}>
          <View style={{
            height: 14,
            width: 14,
            borderRadius: 7,
            backgroundColor: color,
            alignItems: 'center',
            justifyContent: 'center',
            shadowOffset:{  width: 0,  height: 2,  },
            shadowColor: 'rgb(143, 143, 143)',
            shadowOpacity: 0.5,
          }}>
            <FontAwesome name="circle" size={7.5} color='white' />
          </View>
          <Text style={styles.textStyle}>{this.props.route}</Text>
          </View>
          <EvilIcons name="chevron-right" size={25} color="rgb(138,138,143)" />
          </View>
          </TouchableOpacity>
        </View>
      );
    }

}

const styles = {
  textStyle: {
    fontFamily: 'system font',
    paddingLeft: 14
  }
};